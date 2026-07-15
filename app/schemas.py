from __future__ import annotations

from pydantic import BaseModel, Field


class SearchRequest(BaseModel):
    nickname: str = Field(min_length=1, max_length=64)


class PlayerCandidate(BaseModel):
    bm_player_id: str
    name: str | None
    server: str | None
    last_seen: str | None
    is_online: bool


class SearchResponse(BaseModel):
    candidates: list[PlayerCandidate]


class BmProxyRequest(BaseModel):
    """Единственная точка прохода произвольных GET к BM для сложных агрегаций
    (полная статистика игрока — несколько параллельных эндпоинтов), где
    парсинг ответа остаётся на стороне бота, а не дублируется в relay.
    path ограничен allowlist'ом в main.py (/players/, /servers/) — это не
    открытый прокси."""
    path: str = Field(min_length=1, max_length=200)
    params: dict | None = None


class StatusResponse(BaseModel):
    """Живой снимок статуса на момент запроса — relay ничего не кэширует
    между вызовами, свежесть = времени последнего вызова этого эндпоинта."""
    bm_player_id: str
    name: str | None = None
    server: str | None = None
    last_seen: str | None = None
    is_online: bool = False
    checked_at: float | None = None
