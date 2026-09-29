"""
Live match endpoints, backed by Redis (current state per match_id, key
`match:{match_id}:state`, written by the streaming consumer).
"""

import json
from typing import List, Optional

import redis
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from db import get_redis_client

router = APIRouter()


class LiveMatchesResponse(BaseModel):
    match_ids: List[str]


class LiveMatchState(BaseModel):
    match_id: str
    timestamp: str
    innings_number: int
    over_number: int
    ball_number: int
    batting_team: Optional[str] = None
    bowling_team: Optional[str] = None
    current_score: Optional[int] = None
    wickets_lost: Optional[int] = None
    target: Optional[int] = None
    balls_remaining: Optional[int] = None
    current_run_rate: Optional[float] = None
    required_run_rate: Optional[float] = None
    win_probability: Optional[float] = None


@router.get("/matches", response_model=LiveMatchesResponse)
def list_live_matches(r: redis.Redis = Depends(get_redis_client)):
    match_ids = [key.split(":", 2)[1] for key in r.scan_iter(match="match:*:state")]
    return LiveMatchesResponse(match_ids=sorted(match_ids))


@router.get("/match/{match_id}", response_model=LiveMatchState)
def get_live_match(match_id: str, r: redis.Redis = Depends(get_redis_client)):
    raw = r.get(f"match:{match_id}:state")
    if raw is None:
        raise HTTPException(status_code=404, detail=f"No live state found for match_id '{match_id}'")
    return json.loads(raw)
