"""
Publishes a fabricated but cricket-plausible second-innings run chase directly
to the `live-match-events` Kafka topic, matching the exact schema producer.py
would send. Useful for testing/demoing the consumer, Redis state, API, and
dashboard without spending any of CricAPI's 100-requests/day free quota, and
without needing to wait for an actual live match to be happening.

Every simulated match_id is forced into a "sim-" namespace, and by default
this script cleans up after itself once the simulated innings finishes
(deleting the Redis state key and any live_events_log rows it caused) -- so
running this repeatedly for testing doesn't leave stale entries cluttering
the dashboard's live-match dropdown. Pass --no-cleanup to keep the data
around (e.g. to leave a demo visible in the dashboard for a while).

Usage:
    python simulate_match.py [match_id] [--interval SECONDS] [--no-cleanup]
"""

import argparse
import json
import logging
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import redis
from kafka import KafkaProducer

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("simulate_match")

SIM_PREFIX = "sim-"
# Gives the consumer time to process the last published Kafka event before
# cleanup deletes it -- otherwise a slow consumer could re-create the Redis
# key immediately after this script deletes it.
CLEANUP_DELAY_SECONDS = 5


def get_kafka_producer() -> KafkaProducer:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def generate_chase(target: int, batting_team: str, bowling_team: str):
    """Yields one event per over (a coarser cadence than ball-by-ball, which
    is fine -- the real producer only gets one snapshot per poll too), for a
    20-over chase that plausibly ends either just short or just over target."""
    score = 0
    wickets = 0
    for over_number in range(20):
        # a run rate that trends toward "just enough", with some randomness
        runs_needed_pace = target / 20
        over_runs = max(0, round(random.gauss(runs_needed_pace, 3)))
        score += over_runs
        if random.random() < 0.12 and wickets < 9:
            wickets += 1

        legal_balls_bowled = (over_number + 1) * 6
        yield {
            "match_id": None,  # filled in by caller
            "timestamp": None,  # filled in by caller
            "innings_number": 2,
            "over_number": over_number,
            "ball_number": 0,
            "batting_team": batting_team,
            "bowling_team": bowling_team,
            "current_score": score,
            "wickets_lost": wickets,
            "target": target,
            "balls_remaining": max(120 - legal_balls_bowled, 0),
        }

        if score >= target or wickets >= 10:
            break


def cleanup(match_id: str):
    """Deletes the Redis state key and any Postgres log rows this simulated
    match created. Best-effort: a cleanup failure shouldn't be treated as the
    simulation itself having failed."""
    try:
        r = redis.Redis(
            host=os.environ.get("REDIS_HOST", "localhost"),
            port=int(os.environ.get("REDIS_PORT", 6379)),
        )
        deleted = r.delete(f"match:{match_id}:state")
        logger.info("Cleanup: removed Redis key match:%s:state (existed: %s)", match_id, bool(deleted))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Cleanup: couldn't remove Redis key for %s: %s", match_id, exc)

    try:
        conn = psycopg2.connect(
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=os.environ.get("POSTGRES_PORT", "5432"),
            user=os.environ.get("POSTGRES_USER", "pitchpulse"),
            password=os.environ.get("POSTGRES_PASSWORD", "pitchpulse"),
            dbname=os.environ.get("POSTGRES_DB", "pitchpulse"),
        )
        with conn, conn.cursor() as cur:
            cur.execute("DELETE FROM live_events_log WHERE match_id = %s", (match_id,))
            logger.info("Cleanup: removed %d live_events_log row(s) for %s", cur.rowcount, match_id)
        conn.close()
    except Exception as exc:  # noqa: BLE001 - table may not exist yet if the consumer never ran
        logger.warning("Cleanup: couldn't remove live_events_log rows for %s: %s", match_id, exc)


def main():
    parser = argparse.ArgumentParser(description="Simulate a live match chase onto Kafka")
    parser.add_argument("match_id", nargs="?", default="001")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between simulated overs")
    parser.add_argument("--target", type=int, default=175)
    parser.add_argument("--batting-team", default="Simulated Strikers")
    parser.add_argument("--bowling-team", default="Simulated Challengers")
    parser.add_argument(
        "--no-cleanup", action="store_true",
        help="Leave the simulated match's Redis/Postgres data in place instead of deleting it afterward",
    )
    args = parser.parse_args()

    match_id = args.match_id if args.match_id.startswith(SIM_PREFIX) else f"{SIM_PREFIX}{args.match_id}"

    producer = get_kafka_producer()
    topic = os.environ.get("KAFKA_TOPIC", "live-match-events")

    logger.info("Simulating match %s chasing %d, publishing to %s every %.1fs",
                match_id, args.target, topic, args.interval)

    for event in generate_chase(args.target, args.batting_team, args.bowling_team):
        event["match_id"] = match_id
        event["timestamp"] = datetime.now(timezone.utc).isoformat()
        producer.send(topic, value=event)
        producer.flush()
        logger.info("Published: over %d, score %d/%d", event["over_number"] + 1, event["current_score"], event["wickets_lost"])
        time.sleep(args.interval)

    logger.info("Simulated match %s finished", match_id)
    producer.close()

    if args.no_cleanup:
        logger.info("--no-cleanup set: leaving %s's data in Redis/Postgres", match_id)
    else:
        logger.info("Waiting %ds for the consumer to catch up before cleaning up...", CLEANUP_DELAY_SECONDS)
        time.sleep(CLEANUP_DELAY_SECONDS)
        cleanup(match_id)


if __name__ == "__main__":
    main()
