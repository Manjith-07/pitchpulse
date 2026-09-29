"""
Connection helpers for Postgres (historical stats, from the dbt marts) and
Redis (live match state, written by the streaming consumer).
"""

import os
from pathlib import Path

import psycopg2
import psycopg2.extras
import redis

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except ImportError:
    pass


def get_pg_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        user=os.environ.get("POSTGRES_USER", "pitchpulse"),
        password=os.environ.get("POSTGRES_PASSWORD", "pitchpulse"),
        dbname=os.environ.get("POSTGRES_DB", "pitchpulse"),
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def get_db():
    """FastAPI dependency: yields a Postgres connection for one request, then closes it."""
    conn = get_pg_connection()
    try:
        yield conn
    finally:
        conn.close()


_redis_client = None


def get_redis_client() -> redis.Redis:
    """Returns a shared Redis client. redis-py manages its own connection pool
    internally, so a single long-lived client instance is safe to reuse
    across requests rather than opening a new connection each time."""
    global _redis_client
    if _redis_client is None:
        _redis_client = redis.Redis(
            host=os.environ.get("REDIS_HOST", "localhost"),
            port=int(os.environ.get("REDIS_PORT", 6379)),
            decode_responses=True,
        )
    return _redis_client
