"""
Historical stats endpoints, backed by the dbt marts in Postgres:
player_career_stats, team_venue_performance, phase_wise_trends.
"""

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from psycopg2.extensions import connection as PGConnection
from pydantic import BaseModel

from db import get_db

router = APIRouter()


def _like_pattern(value: str) -> str:
    """Builds a case-insensitive "contains" pattern for ILIKE, escaping any
    literal %/_ the user typed so they can't accidentally (or deliberately)
    inject their own wildcards."""
    escaped = value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


class PlayerCareerStats(BaseModel):
    player: str
    runs_scored: Optional[int] = None
    balls_faced: Optional[int] = None
    batting_strike_rate: Optional[float] = None
    dismissals: Optional[int] = None
    boundaries_hit: Optional[int] = None
    balls_bowled: Optional[int] = None
    runs_conceded: Optional[int] = None
    wickets_taken: Optional[int] = None
    economy_rate: Optional[float] = None


class TeamVenuePerformance(BaseModel):
    team: str
    venue: str
    matches_played: int
    matches_won: int
    win_percentage: float


class PhaseTrend(BaseModel):
    team: str
    season: str
    phase: str
    total_runs: int
    legal_balls: int
    average_run_rate: Optional[float] = None


@router.get("/player/{player_name}", response_model=List[PlayerCareerStats])
def get_player_stats(player_name: str, db: PGConnection = Depends(get_db)):
    """Case-insensitive partial match on player name (Cricsheet stores names
    as initials + surname, e.g. 'MS Dhoni', so an exact match on 'Dhoni' alone
    would never hit). Returns every match -- no cap -- so the caller always
    gets the true set to disambiguate from. player_career_stats has on the
    order of a thousand rows at most, so an uncapped ILIKE scan here is cheap;
    this is a search endpoint, not a paginated listing."""
    with db.cursor() as cur:
        cur.execute(
            "SELECT * FROM player_career_stats WHERE player ILIKE %s ORDER BY player",
            (_like_pattern(player_name),),
        )
        rows = cur.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail=f"No stats found for player matching '{player_name}'")
    return rows


@router.get("/team/{team_name}/venues", response_model=List[TeamVenuePerformance])
def get_team_venue_performance(team_name: str, db: PGConnection = Depends(get_db)):
    with db.cursor() as cur:
        cur.execute(
            "SELECT * FROM team_venue_performance WHERE team ILIKE %s ORDER BY venue",
            (_like_pattern(team_name),),
        )
        rows = cur.fetchall()
    return rows


@router.get("/phase-trends", response_model=List[PhaseTrend])
def get_phase_trends(
    team: Optional[str] = None,
    season: Optional[str] = None,
    db: PGConnection = Depends(get_db),
):
    query = "SELECT * FROM phase_wise_trends WHERE 1=1"
    params: list = []
    if team:
        query += " AND team ILIKE %s"
        params.append(_like_pattern(team))
    if season:
        query += " AND season = %s"
        params.append(season)
    query += " ORDER BY team, season, phase"

    with db.cursor() as cur:
        cur.execute(query, params)
        rows = cur.fetchall()
    return rows
