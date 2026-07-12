from __future__ import annotations

import asyncio
import logging
import time

import aiohttp

from . import config

logger = logging.getLogger("bm_client")

BASE_URL = "https://api.battlemetrics.com"


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
        self._rate_reset_ts: float | None = None
        self._backoff_until = 0.0
        self._backoff_sec = config.BM_DEFAULT_BACKOFF_SEC

    async def _wait_turn(self) -> None:
        now = time.monotonic()
        if self._backoff_until > now:
            await asyncio.sleep(self._backoff_until - now)
            now = time.monotonic()

        if self._rate_remaining is not None and self._rate_remaining <= config.BM_RATE_LIMIT_SAFETY_MARGIN:
            if self._rate_reset_ts and self._rate_reset_ts > now:
                wait = self._rate_reset_ts - now
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
        if remaining is not None and remaining.isdigit():
            self._rate_remaining = int(remaining)
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
        token: str,
        max_retries: int = 3,
    ) -> dict:
        url = f"{BASE_URL}{path}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.api+json",
        }

        for attempt in range(1, max_retries + 1):
            async with self._lock:
                await self._wait_turn()
                self._last_request_ts = time.monotonic()
                async with session.request(method, url, headers=headers, params=params) as resp:
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

        raise BattleMetricsError(f"{method} {path}: превышено число попыток после повторных 429")


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
    IP/аккаунта, а не токена, так что просто множит запросы без толку."""
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

    raise last_exc or BattleMetricsError("Все токены BattleMetrics отклонены (401)")


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
    """Снимок статуса одного игрока — 1 запрос к BM (include=server даёт нужные
    данные без дополнительного запроса на сессии, этого достаточно для online/offline)."""
    session = await get_session()
    data = await _request_any_token(
        session, "GET", f"/players/{bm_player_id}",
        params={"include": "server"},
    )
    player = data.get("data") or {}
    attrs = player.get("attributes") or {}
    included_servers = {
        item["id"]: item.get("attributes", {})
        for item in data.get("included", [])
        if item.get("type") == "server"
    }

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

    return {
        "bm_player_id": bm_player_id,
        "name": attrs.get("name"),
        "server": server_name,
        "last_seen": last_seen,
        "is_online": is_online,
        "checked_at": time.time(),
    }
