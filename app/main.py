from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException

from . import bm_client, config, poller, store
from .schemas import (
    SearchRequest,
    SearchResponse,
    StatusBatchRequest,
    StatusBatchResponse,
    StatusResponse,
    TrackRequest,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("main")

_poller_task: asyncio.Task | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _poller_task
    _poller_task = asyncio.create_task(poller.run_forever())
    logger.info("battlemetrics-relay запущен")
    yield
    poller.stop()
    if _poller_task:
        _poller_task.cancel()
    await bm_client.close_session()
    await store.close_redis()


app = FastAPI(title="battlemetrics-relay", lifespan=lifespan)


def require_panel_secret(x_relay_secret: str = Header(default="")) -> None:
    if x_relay_secret != config.RELAY_SHARED_SECRET:
        raise HTTPException(status_code=401, detail="bad relay secret")


@app.get("/health")
async def health() -> dict:
    return {"ok": True}


@app.post("/players/search", response_model=SearchResponse, dependencies=[Depends(require_panel_secret)])
async def search_players(body: SearchRequest) -> SearchResponse:
    try:
        candidates = await bm_client.search_players(body.nickname)
    except bm_client.BattleMetricsError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return SearchResponse(candidates=candidates)


@app.post("/players/track", dependencies=[Depends(require_panel_secret)])
async def track_player(body: TrackRequest) -> dict:
    await store.track_player(body.bm_player_id)
    return {"ok": True, "bm_player_id": body.bm_player_id}


@app.delete("/players/track/{bm_player_id}", dependencies=[Depends(require_panel_secret)])
async def untrack_player(bm_player_id: str) -> dict:
    await store.untrack_player(bm_player_id)
    return {"ok": True}


@app.get(
    "/players/{bm_player_id}/status",
    response_model=StatusResponse,
    dependencies=[Depends(require_panel_secret)],
)
async def get_status(bm_player_id: str) -> StatusResponse:
    """
    Панель дёргает этот эндпоинт хоть каждую секунду — он ТОЛЬКО читает Redis
    и никогда не обращается к BattleMetrics напрямую. Свежесть данных
    определяется фоновым поллером (см. app/poller.py), а не частотой этих
    запросов.
    """
    status = await store.get_status(bm_player_id)
    if status is None:
        return StatusResponse(bm_player_id=bm_player_id, pending=True)
    return StatusResponse(**status)


@app.post(
    "/players/status/batch",
    response_model=StatusBatchResponse,
    dependencies=[Depends(require_panel_secret)],
)
async def get_status_batch(body: StatusBatchRequest) -> StatusBatchResponse:
    """Пачка статусов за один Redis MGET — ноль обращений к BattleMetrics.
    Использовать вместо N вызовов /players/{id}/status, когда нужно сразу
    много игроков (например список слежки пользователя)."""
    raw = await store.get_status_batch(body.bm_player_ids)
    out: dict[str, StatusResponse] = {}
    for bm_player_id, status in raw.items():
        if status is None:
            out[bm_player_id] = StatusResponse(bm_player_id=bm_player_id, pending=True)
        else:
            out[bm_player_id] = StatusResponse(**status)
    return StatusBatchResponse(statuses=out)
