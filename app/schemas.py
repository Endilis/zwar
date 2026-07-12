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


class TrackRequest(BaseModel):
    bm_player_id: str = Field(min_length=1, max_length=32)


class StatusResponse(BaseModel):
    bm_player_id: str
    name: str | None = None
    server: str | None = None
    last_seen: str | None = None
    is_online: bool = False
    checked_at: float | None = None
    is_stale: bool | None = None
    pending: bool = False
