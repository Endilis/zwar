from __future__ import annotations

import asyncio
import logging
import time

from . import bm_client, config, store

logger = logging.getLogger("poller")

_stop = asyncio.Event()
_cursor = 0


async def _refresh_one(bm_player_id: str) -> None:
    try:
        status = await bm_client.fetch_player_status(bm_player_id)
        await store.save_status(bm_player_id, status)
    except Exception:
        logger.exception("Не удалось обновить статус игрока %s", bm_player_id)


async def run_forever() -> None:
    """
    Фоновый цикл — единственный источник трафика к BM API для отслеживаемых
    игроков. Панель никогда не триггерит запрос к BM напрямую своим поллингом
    (GET /players/{id}/status читает только Redis) — это и есть развязка,
    которая не даёт частоте опроса панели влиять на нагрузку на BM API.

    Приоритетная очередь (сразу после /track) обслуживается первой, затем —
    плавный round-robin по всем отслеживаемым с интервалом, который растягивается
    по мере роста списка (WATCH_REFRESH_WINDOW_SEC / N), а не наоборот.
    """
    global _cursor
    logger.info("Поллер запущен: окно обновления=%.0fs, мин. интервал=%.1fs",
                config.WATCH_REFRESH_WINDOW_SEC, config.BM_MIN_REQUEST_INTERVAL_SEC)

    while not _stop.is_set():
        priority_id = await store.pop_priority()
        if priority_id:
            await _refresh_one(priority_id)
            continue

        tracked = sorted(await store.list_tracked())
        if not tracked:
            await asyncio.sleep(2.0)
            continue

        _cursor %= len(tracked)
        bm_player_id = tracked[_cursor]
        _cursor += 1

        interval = max(config.BM_MIN_REQUEST_INTERVAL_SEC, config.WATCH_REFRESH_WINDOW_SEC / len(tracked))
        started = time.monotonic()
        await _refresh_one(bm_player_id)
        elapsed = time.monotonic() - started
        remaining = interval - elapsed
        if remaining > 0:
            await asyncio.sleep(remaining)


def stop() -> None:
    _stop.set()
