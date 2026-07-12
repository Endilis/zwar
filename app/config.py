from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


def _env_float(name: str, default: float) -> float:
    raw = _env(name)
    try:
        return float(raw) if raw else default
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = _env(name)
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


# BattleMetrics: основной токен + опциональные резервные (только на случай 401/403
# основного, НЕ для наращивания объёма запросов — ротация "чтобы слать больше"
# была одной из причин бана в legacy-боте).
BATTLEMETRICS_TOKENS = [t.strip() for t in _env("BATTLEMETRICS_TOKENS").split(",") if t.strip()]
if not BATTLEMETRICS_TOKENS:
    raise RuntimeError("BATTLEMETRICS_TOKENS не задан в .env")

REDIS_URL = _env("REDIS_URL", "redis://127.0.0.1:6379/0")

# Секрет, которым панель подтверждает себя перед relay (см. паттерн
# TICKET_BOT_INTERNAL_SECRET в основном боте).
RELAY_SHARED_SECRET = _env("RELAY_SHARED_SECRET")
if not RELAY_SHARED_SECRET:
    raise RuntimeError("RELAY_SHARED_SECRET не задан в .env")

# --- Rate limiting ---
# Жёсткий пол: минимальный интервал между ЛЮБЫМИ двумя запросами к BM API,
# независимо от заголовков лимита. Это последний рубеж защиты от бана.
BM_MIN_REQUEST_INTERVAL_SEC = _env_float("BM_MIN_REQUEST_INTERVAL_SEC", 1.0)
# Запас прочности: если по заголовкам X-Rate-Limit-Remaining осталось меньше
# этого числа запросов — ждём до сброса окна вместо того, чтобы бить в лимит.
BM_RATE_LIMIT_SAFETY_MARGIN = _env_int("BM_RATE_LIMIT_SAFETY_MARGIN", 5)
# Backoff при 429, если сервер не прислал Retry-After.
BM_DEFAULT_BACKOFF_SEC = _env_float("BM_DEFAULT_BACKOFF_SEC", 5.0)
BM_MAX_BACKOFF_SEC = _env_float("BM_MAX_BACKOFF_SEC", 300.0)

# --- Поллер отслеживаемых игроков ---
# Желаемое окно, за которое должен обновиться КАЖДЫЙ отслеживаемый игрок.
# Реальный интервал на игрока = max(BM_MIN_REQUEST_INTERVAL_SEC, WATCH_REFRESH_WINDOW_SEC / N),
# т.е. при росте списка обновления идут реже, а не быстрее лимита.
WATCH_REFRESH_WINDOW_SEC = _env_float("WATCH_REFRESH_WINDOW_SEC", 300.0)
# Статус считается протухшим и не отдаётся панели, если не обновлялся дольше.
STATUS_STALE_AFTER_SEC = _env_float("STATUS_STALE_AFTER_SEC", 900.0)

HOST = _env("HOST", "0.0.0.0")
PORT = _env_int("PORT", 8000)
