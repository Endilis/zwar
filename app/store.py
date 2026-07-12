from __future__ import annotations

import json
import time

from redis.asyncio import Redis

from . import config

TRACKED_SET_KEY = "bmrelay:tracked"
STATUS_KEY_PREFIX = "bmrelay:status:"
PRIORITY_QUEUE_KEY = "bmrelay:priority_queue"

_redis: Redis | None = None


async def get_redis() -> Redis:
    global _redis
    if _redis is None:
        _redis = Redis.from_url(config.REDIS_URL, decode_responses=True, health_check_interval=30)
        await _redis.ping()
    return _redis


async def close_redis() -> None:
    global _redis
    if _redis is not None:
        await _redis.aclose()
        _redis = None


async def track_player(bm_player_id: str) -> None:
    r = await get_redis()
    await r.sadd(TRACKED_SET_KEY, bm_player_id)
    # Приоритетный запрос на немедленное обновление после подтверждения игроком.
    await r.lpush(PRIORITY_QUEUE_KEY, bm_player_id)


async def untrack_player(bm_player_id: str) -> None:
    r = await get_redis()
    await r.srem(TRACKED_SET_KEY, bm_player_id)
    await r.delete(f"{STATUS_KEY_PREFIX}{bm_player_id}")


async def list_tracked() -> list[str]:
    r = await get_redis()
    return list(await r.smembers(TRACKED_SET_KEY))


async def pop_priority(timeout: float = 0.0) -> str | None:
    r = await get_redis()
    item = await r.lpop(PRIORITY_QUEUE_KEY)
    return item


async def save_status(bm_player_id: str, status: dict) -> None:
    r = await get_redis()
    await r.set(f"{STATUS_KEY_PREFIX}{bm_player_id}", json.dumps(status))


async def get_status(bm_player_id: str) -> dict | None:
    r = await get_redis()
    raw = await r.get(f"{STATUS_KEY_PREFIX}{bm_player_id}")
    if not raw:
        return None
    status = json.loads(raw)
    checked_at = status.get("checked_at") or 0
    status["is_stale"] = (time.time() - checked_at) > config.STATUS_STALE_AFTER_SEC
    return status


async def get_status_batch(bm_player_ids: list[str]) -> dict[str, dict | None]:
    """MGET по всем сразу — один round-trip к Redis, ноль запросов к BM."""
    if not bm_player_ids:
        return {}
    r = await get_redis()
    keys = [f"{STATUS_KEY_PREFIX}{pid}" for pid in bm_player_ids]
    raw_values = await r.mget(keys)
    now = time.time()
    out: dict[str, dict | None] = {}
    for pid, raw in zip(bm_player_ids, raw_values):
        if not raw:
            out[pid] = None
            continue
        status = json.loads(raw)
        checked_at = status.get("checked_at") or 0
        status["is_stale"] = (now - checked_at) > config.STATUS_STALE_AFTER_SEC
        out[pid] = status
    return out
