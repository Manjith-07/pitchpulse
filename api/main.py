"""
PitchPulse API: exposes historical stats (from Postgres dbt marts) and live
match state including win probability (from Redis, written by the streaming
consumer). See /docs for the interactive API reference.
"""

from fastapi import FastAPI
from pydantic import BaseModel

from routers import historical, live

app = FastAPI(
    title="PitchPulse API",
    description="Historical cricket stats and live win-probability-enabled match state.",
    version="0.1.0",
)

app.include_router(historical.router, prefix="/historical", tags=["historical"])
app.include_router(live.router, prefix="/live", tags=["live"])


class HealthResponse(BaseModel):
    status: str


@app.get("/health", response_model=HealthResponse)
def health():
    return HealthResponse(status="ok")
