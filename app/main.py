from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException

from . import bm_client, config
from .schemas import (
    BmProxyRequest,
    SearchRequest,
    SearchResponse,
    StatusResponse,
)

_BM_PROXY_ALLOWED_PREFIXES = ("/players/", "/servers/")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Полностью stateless: relay сам ничего не крутит и ничего не хранит
    # между запросами (нет своего поллера, нет своего Redis-кэша/списка
    # отслеживаемых — раньше был, убрано намеренно). Единственная задача —
    # принять ID, сходить в BM прямо сейчас через общий rate-limiter
    # (bm_client.BattleMetricsRateLimiter) и вернуть результат. Периодичность
    # опроса и список отслеживаемых полностью на стороне вызывающего
    # (panel-api) — см. её собственный round-robin цикл.
    logger.info("battlemetrics-relay запущен (stateless)")
    yield
    await bm_client.close_session()


app = FastAPI(title="battlemetrics-relay", lifespan=lifespan)


def require_panel_secret(x_relay_secret: str = Header(default="")) -> None:
    if x_relay_secret != config.RELAY_SHARED_SECRET:
        raise HTTPException(status_code=401, detail="bad relay secret")


@app.get("/health")
async def health() -> dict:
    return {"ok": True}


@app.get("/debug/limiter", dependencies=[Depends(require_panel_secret)])
async def debug_limiter() -> dict:
    """Снимок состояния общего BattleMetricsRateLimiter — нет прямого доступа
    к логам Render, это единственный способ увидеть, не завис ли лимитер
    (см. инцидент 2026-07-15: поиск/статус зависали на много минут)."""
    return bm_client._limiter.debug_state()


@app.post("/players/search", response_model=SearchResponse, dependencies=[Depends(require_panel_secret)])
async def search_players(body: SearchRequest) -> SearchResponse:
    try:
        candidates = await bm_client.search_players(body.nickname)
    except bm_client.BattleMetricsError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return SearchResponse(candidates=candidates)


@app.get(
    "/players/{bm_player_id}/status",
    response_model=StatusResponse,
    dependencies=[Depends(require_panel_secret)],
)
async def get_status(bm_player_id: str) -> StatusResponse:
    """Живой запрос к BM прямо сейчас — никакого кэша/поллера на стороне
    relay. Частоту вызовов (и, соответственно, безопасность по rate-limit
    BM) контролирует вызывающий — см. bm_client.BattleMetricsRateLimiter,
    единая точка прохода всех запросов этого процесса."""
    try:
        status = await bm_client.fetch_player_status(bm_player_id)
    except bm_client.BattleMetricsError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return StatusResponse(**status)


@app.post("/bm/proxy", dependencies=[Depends(require_panel_secret)])
async def bm_proxy(body: BmProxyRequest) -> dict:
    """Проход произвольного GET к BM через тот же безопасный
    BattleMetricsRateLimiter/_request_any_token, что search/status — для
    сложных агрегаций (например полная статистика игрока — несколько
    параллельных эндпоинтов), где парсинг ответа остаётся на стороне бота.
    Не открытый прокси: path ограничен allowlist'ом ниже."""
    if not body.path.startswith(_BM_PROXY_ALLOWED_PREFIXES):
        raise HTTPException(status_code=400, detail="path not allowed")
    try:
        return await bm_client.raw_get(body.path, body.params)
    except bm_client.BattleMetricsError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
