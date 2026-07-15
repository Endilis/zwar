from __future__ import annotations

import asyncio
import logging
import time

import aiohttp

from . import config

logger = logging.getLogger("bm_client")

BASE_URL = "https://api.battlemetrics.com"
# Потолок на одно ожидание внутри _wait_turn (кроме уже осмысленно
# ограниченного BM_MAX_BACKOFF_SEC для настоящих 429) — BM отдаёт лимиты
# на минутное окно, 65с с запасом. См. докстринг _wait_turn.
_MAX_SINGLE_WAIT_SEC = 65.0


class BattleMetricsError(Exception):
    pass


class BattleMetricsRateLimiter:
    """
    Единая точка прохода всех запросов к BattleMetrics.

    Три независимых тормоза, любой может сработать:
      1. Жёсткий минимальный интервал между запросами (BM_MIN_REQUEST_INTERVAL_SEC).
      2. Адаптивный — по заголовкам X-Rate-Limit-Remaining/-Reset из последнего ответа.
      3. Backoff по 429 с уважением Retry-After (или экспоненциально, если его нет).

    Всё сериализуется через один asyncio.Lock — это специально: раньше бот
    слал запросы параллельно/без пауз с нескольких токенов и ловил бан по IP.
    Один процесс = один заказчик перед BM, независимо от того, сколько
    отслеживаемых игроков или параллельных поисков запрошено внутри сервиса.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._last_request_ts = 0.0
        self._rate_remaining: int | None = None
        self._rate_limit: int | None = None
        self._rate_reset_ts: float | None = None
        self._rate_reset_raw_header: str | None = None
        self._backoff_until = 0.0
        self._backoff_sec = config.BM_DEFAULT_BACKOFF_SEC
        self._waiters = 0

    def debug_state(self) -> dict:
        """Снимок состояния лимитера через API (логов Render под рукой нет) —
        см. инцидент 2026-07-15, поиск/статус зависали на много минут."""
        now = time.monotonic()
        return {
            "waiters_in_queue": self._waiters,
            "lock_locked": self._lock.locked(),
            "rate_remaining": self._rate_remaining,
            "rate_limit": self._rate_limit,
            "rate_reset_raw_header": self._rate_reset_raw_header,
            "rate_reset_in_sec": (self._rate_reset_ts - now) if self._rate_reset_ts else None,
            "backoff_in_sec": max(0.0, self._backoff_until - now),
            "seconds_since_last_request": (now - self._last_request_ts) if self._last_request_ts else None,
        }

    async def _wait_turn(self) -> None:
        # ИНЦИДЕНТ 2026-07-15: эта функция вызывается ВНУТРИ self._lock — если
        # тут случайно посчитается неадекватно большое ожидание (например
        # рассинхрон единиц в X-Rate-Limit-Reset — сек vs мс, или сам
        # заголовок странный), лок держится всё это время, и ВСЕ запросы
        # процесса (поиск/статус/прокси/фоновый опрос panel-api) синхронно
        # виснут на многие минуты — ровно так уронило поиск в панели. Явный
        # потолок на разовое ожидание — последний рубеж, независимо от того,
        # что именно пошло не так при расчёте wait.
        now = time.monotonic()
        if self._backoff_until > now:
            wait = min(self._backoff_until - now, config.BM_MAX_BACKOFF_SEC)
            await asyncio.sleep(wait)
            now = time.monotonic()

        if self._rate_remaining is not None and self._rate_remaining <= config.BM_RATE_LIMIT_SAFETY_MARGIN:
            if self._rate_reset_ts and self._rate_reset_ts > now:
                raw_wait = self._rate_reset_ts - now
                wait = min(raw_wait, _MAX_SINGLE_WAIT_SEC)
                if raw_wait > _MAX_SINGLE_WAIT_SEC:
                    logger.warning(
                        "BM rate wait %.1fs выглядит неадекватно (заголовок X-Rate-Limit-Reset?) — обрезаю до %.0fs",
                        raw_wait, _MAX_SINGLE_WAIT_SEC,
                    )
                else:
                    logger.info("BM rate budget низкий (remaining=%s), ждём %.1fs до сброса окна", self._rate_remaining, wait)
                await asyncio.sleep(wait)
                now = time.monotonic()
                self._rate_remaining = None  # окно сброшено, разблокируем до следующего заголовка

        min_gap = config.BM_MIN_REQUEST_INTERVAL_SEC
        elapsed = now - self._last_request_ts
        if elapsed < min_gap:
            await asyncio.sleep(min_gap - elapsed)

    def _record_headers(self, headers: "aiohttp.typedefs.LooseHeaders") -> None:
        remaining = headers.get("X-Rate-Limit-Remaining")
        limit = headers.get("X-Rate-Limit-Limit")
        reset = headers.get("X-Rate-Limit-Reset")
        self._rate_reset_raw_header = reset
        if remaining is not None and remaining.isdigit():
            self._rate_remaining = int(remaining)
        if limit is not None and limit.isdigit():
            self._rate_limit = int(limit)
        if reset is not None:
            try:
                # BM отдаёт unix-timestamp секунд до сброса окна.
                self._rate_reset_ts = time.monotonic() + max(0.0, float(reset) - time.time())
            except ValueError:
                pass
        if limit and self._rate_remaining is not None:
            logger.debug("BM rate: %s/%s remaining", self._rate_remaining, limit)

    async def request(
        self,
        session: aiohttp.ClientSession,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        token: str | None,
        max_retries: int = 3,
    ) -> dict:
        url = f"{BASE_URL}{path}"
        headers = {"Accept": "application/vnd.api+json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"

        self._waiters += 1
        try:
            for attempt in range(1, max_retries + 1):
                async with self._lock:
                    await self._wait_turn()
                    self._last_request_ts = time.monotonic()
                    # НАСТОЯЩАЯ ПРИЧИНА инцидента 2026-07-15 (не расчёт wait,
                    # как думал сначала): здесь не было своего таймаута.
                    # get_session() создаёт голый aiohttp.ClientSession без
                    # timeout=, а этот вызов держит self._lock — если BM/сеть
                    # зависает без ответа и без TCP RST, aiohttp ждёт дефолтные
                    # 300с (или дольше), и ВСЕ остальные запросы процесса
                    # (поиск/статус/прокси/поллер panel-api) блокируются на
                    # это время. Явный таймаут — обязателен именно тут, а не
                    # только на вызывающей стороне (клиентский timeout бота
                    # не спасает: раз лок держится в relay, ответа не будет,
                    # пока не истечёт именно ЭТОТ таймаут).
                    request_timeout = aiohttp.ClientTimeout(total=20.0)
                    try:
                        async with session.request(
                            method, url, headers=headers, params=params, timeout=request_timeout,
                        ) as resp:
                            self._record_headers(resp.headers)

                            if resp.status == 429:
                                retry_after_raw = resp.headers.get("Retry-After")
                                if retry_after_raw and retry_after_raw.isdigit():
                                    wait = float(retry_after_raw)
                                else:
                                    wait = min(self._backoff_sec, config.BM_MAX_BACKOFF_SEC)
                                    self._backoff_sec = min(self._backoff_sec * 2, config.BM_MAX_BACKOFF_SEC)
                                self._backoff_until = time.monotonic() + wait
                                logger.warning(
                                    "BM 429 на %s %s (попытка %s/%s), ждём %.1fs",
                                    method, path, attempt, max_retries, wait,
                                )
                                continue

                            # Успешный ответ (не обязательно 429) — сбрасываем экспоненту backoff.
                            self._backoff_sec = config.BM_DEFAULT_BACKOFF_SEC

                            if resp.status >= 400:
                                text = await resp.text()
                                raise BattleMetricsError(f"{method} {path} -> {resp.status}: {text[:300]}")

                            return await resp.json()
                    except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                        logger.warning("BM запрос %s %s не ответил за %.0fs: %r", method, path, request_timeout.total, exc)
                        raise BattleMetricsError(f"{method} {path}: no response within {request_timeout.total:.0f}s") from exc

            raise BattleMetricsError(f"{method} {path}: превышено число попыток после повторных 429")
        finally:
            self._waiters -= 1


_limiter = BattleMetricsRateLimiter()
_session: aiohttp.ClientSession | None = None


async def get_session() -> aiohttp.ClientSession:
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession()
    return _session


async def close_session() -> None:
    global _session
    if _session is not None:
        await _session.close()
        _session = None


_working_token_index = 0


async def _request_any_token(session: aiohttp.ClientSession, method: str, path: str, *, params: dict | None = None) -> dict:
    """Пробует токены по очереди, начиная с последнего рабочего. Переключение
    ТОЛЬКО на 401 (сам токен невалиден/отозван) — на 429/403 (rate limit,
    ip-бан) переключать токен бессмысленно и вредно: это лимит на уровне
    IP/аккаунта, а не токена, так что просто множит запросы без толку.

    Если ВСЕ токены отклонены — последняя попытка вообще без токена.
    BattleMetrics разрешает неаутентифицированные запросы (свой, более
    скромный лимит: 15/сек, 60/мин против 45/сек, 300/мин с токеном) —
    некоторые эндпоинты (например прямой GET /players/{id}) могут работать
    и без токена, даже если filter[search] его требует."""
    global _working_token_index
    tokens = config.BATTLEMETRICS_TOKENS
    last_exc: BattleMetricsError | None = None

    for offset in range(len(tokens)):
        idx = (_working_token_index + offset) % len(tokens)
        try:
            data = await _limiter.request(session, method, path, params=params, token=tokens[idx])
            _working_token_index = idx
            return data
        except BattleMetricsError as exc:
            if " -> 401:" not in str(exc):
                raise
            logger.warning("BM токен #%s отклонён (401), пробуем следующий", idx)
            last_exc = exc

    logger.warning("Все %s токенов отклонены (401), пробуем без токена", len(tokens))
    try:
        return await _limiter.request(session, method, path, params=params, token=None)
    except BattleMetricsError as exc:
        raise last_exc or exc


async def raw_get(path: str, params: dict | None = None) -> dict:
    """Публичная обёртка над _request_any_token для сложных агрегаций (см.
    main.py::bm_proxy), где парсинг ответа остаётся на стороне вызывающего
    сервиса — тот же безопасный лимитер/ротация токенов, что и у остальных
    функций этого модуля."""
    session = await get_session()
    return await _request_any_token(session, "GET", path, params=params)


async def search_players(nickname: str, *, page_size: int = 20) -> list[dict]:
    """Поиск кандидатов по нику — 1 запрос к BM, без фильтра на точное совпадение
    (в отличие от legacy): панель показывает варианты, пользователь выбирает сам."""
    session = await get_session()
    data = await _request_any_token(
        session, "GET", "/players",
        params={
            "filter[search]": f'"{nickname}"',
            "page[size]": page_size,
            "include": "server",
            "sort": "-lastSeen",
        },
    )
    included_servers = {
        item["id"]: item.get("attributes", {})
        for item in data.get("included", [])
        if item.get("type") == "server"
    }

    candidates = []
    for player in data.get("data", []):
        attrs = player.get("attributes", {})
        server_rel = (player.get("relationships") or {}).get("servers", {}).get("data") or []
        server_name = None
        last_seen = None
        is_online = False
        if server_rel:
            latest = max(server_rel, key=lambda s: (s.get("meta") or {}).get("lastSeen", ""))
            meta = latest.get("meta") or {}
            last_seen = meta.get("lastSeen")
            is_online = bool(meta.get("online"))
            server_name = included_servers.get(latest.get("id"), {}).get("name")

        candidates.append({
            "bm_player_id": player.get("id"),
            "name": attrs.get("name"),
            "server": server_name,
            "last_seen": last_seen,
            "is_online": is_online,
        })
    return candidates


async def fetch_player_status(bm_player_id: str) -> dict:
    """Снимок статуса одного игрока — 1 запрос к BM.

    ВАЖНО (проверено вживую 2026-07-15): прямой GET /players/{id}?include=server
    НЕ заполняет player.relationships.servers (в отличие от списочного
    /players?filter[search]=..., который заполняет) — BM отдаёт данные о
    серверах только в `included`, без привязки relationships на уровне
    одного игрока. Раньше код читал именно relationships.servers и поэтому
    ВСЕГДА получал is_online=False/server=None, вне зависимости от
    реального статуса. Правильный путь — тот же, что уже используется в
    сложной статистике (fetch_player_full_stats в боте): relationships/
    sessions, page[size]=1, самая свежая сессия; stop=null означает "сессия
    ещё не закрыта" = игрок сейчас на сервере. Имя игрока берём из самой
    сессии (attributes.name) — отдельный запрос профиля не нужен, держим
    1 запрос на проверку (важно для расчёта частоты опроса в panel-api)."""
    session = await get_session()
    sessions_data = await _request_any_token(
        session, "GET", f"/players/{bm_player_id}/relationships/sessions",
        params={
            "include": "server",
            "page[size]": 1,
            "fields[session]": "start,stop,firstTime,name",
            "fields[server]": "name",
        },
    )
    name = None
    server_name = None
    last_seen = None
    is_online = False
    sessions = sessions_data.get("data") or []
    if sessions:
        latest = sessions[0]
        s_attrs = latest.get("attributes") or {}
        name = s_attrs.get("name")
        last_seen = s_attrs.get("start")
        is_online = s_attrs.get("stop") is None
        server_id = ((latest.get("relationships") or {}).get("server") or {}).get("data", {}).get("id")
        if server_id:
            for item in sessions_data.get("included", []):
                if item.get("type") == "server" and item.get("id") == server_id:
                    server_name = (item.get("attributes") or {}).get("name")
                    break

    return {
        "bm_player_id": bm_player_id,
        "name": name,
        "server": server_name,
        "last_seen": last_seen,
        "is_online": is_online,
        "checked_at": time.time(),
    }
