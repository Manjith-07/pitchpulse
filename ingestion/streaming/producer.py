"""
Polls a live cricket match from a CricAPI-style API and publishes normalized
events to the `live-match-events` Kafka topic.

Usage:
    python producer.py [match_id]

If match_id isn't given as an argument, falls back to the LIVE_CRICKET_MATCH_ID
environment variable.

Polling interval: CricAPI's free tier only allows 100 requests/day. A real
live poller would check every 10-15 seconds, but at that rate a single T20
would burn through the entire daily quota in under 10 minutes. Polling every
2-3 minutes instead keeps a full ~3.5 hour match comfortably under budget
(roughly 70-105 requests) while still feeling "live" on the dashboard.

For testing or demoing without spending any API quota at all, use
simulate_match.py instead, which publishes fake events straight to Kafka.
"""

import argparse
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from kafka import KafkaProducer

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("producer")

POLL_INTERVAL_MIN_SECONDS = 120
POLL_INTERVAL_MAX_SECONDS = 180
MIN_BACKOFF_SECONDS = 30
MAX_BACKOFF_SECONDS = 30 * 60

# Balls per innings by format, used to compute balls_remaining. Formats not
# listed here (e.g. test cricket, which has no fixed overs limit) simply get
# a null balls_remaining.
FORMAT_OVERS = {"t20": 20, "t20i": 20, "odi": 50, "list a": 50}


def get_kafka_producer() -> KafkaProducer:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    return KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def fetch_match_info(match_id: str) -> dict:
    base_url = os.environ["LIVE_CRICKET_API_BASE_URL"].rstrip("/")
    api_key = os.environ["LIVE_CRICKET_API_KEY"]
    resp = requests.get(
        f"{base_url}/match_info",
        params={"apikey": api_key, "id": match_id},
        timeout=15,
    )
    resp.raise_for_status()
    payload = resp.json()
    if payload.get("status") != "success":
        raise RuntimeError(f"CricAPI error: {payload.get('reason', payload)}")
    return payload["data"]


def parse_legal_balls(overs_value) -> int:
    """Converts CricAPI's '12.4' over notation (12 completed overs, 4 legal
    balls into the next one) into a plain count of legal balls bowled."""
    if overs_value is None:
        return 0
    overs_str = str(overs_value)
    if "." in overs_str:
        completed, balls = overs_str.split(".")
    else:
        completed, balls = overs_str, "0"
    return int(completed) * 6 + int(balls or 0)


def build_event(match_id: str, data: dict):
    """Transforms a CricAPI match_info response into our Kafka message schema.
    Returns None if the match hasn't started yet (no score data to report)."""
    score = data.get("score") or []
    if not score:
        return None

    current = score[-1]
    innings_number = len(score)
    legal_balls_bowled = parse_legal_balls(current.get("o"))
    over_number = legal_balls_bowled // 6
    ball_number = legal_balls_bowled % 6

    # CricAPI labels each innings like "Some Team Name Inning 1"
    inning_label = current.get("inning", "") or ""
    batting_team = inning_label.rsplit(" Inning", 1)[0].strip() or None
    teams = data.get("teams") or []
    bowling_team = next((t for t in teams if t != batting_team), None)

    target = None
    if innings_number >= 2:
        target = score[0].get("r", 0) + 1

    match_type = (data.get("matchType") or "").lower()
    overs_limit = FORMAT_OVERS.get(match_type)
    balls_remaining = None
    if overs_limit is not None:
        balls_remaining = max(overs_limit * 6 - legal_balls_bowled, 0)

    return {
        "match_id": match_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "innings_number": innings_number,
        "over_number": over_number,
        "ball_number": ball_number,
        "batting_team": batting_team,
        "bowling_team": bowling_team,
        "current_score": current.get("r"),
        "wickets_lost": current.get("w"),
        "target": target,
        "balls_remaining": balls_remaining,
    }


def poll_loop(match_id: str):
    producer = get_kafka_producer()
    topic = os.environ.get("KAFKA_TOPIC", "live-match-events")
    backoff = 0

    logger.info("Polling match %s every %d-%ds, publishing to topic %s",
                match_id, POLL_INTERVAL_MIN_SECONDS, POLL_INTERVAL_MAX_SECONDS, topic)

    while True:
        try:
            data = fetch_match_info(match_id)
            backoff = 0

            event = build_event(match_id, data)
            if event is not None:
                producer.send(topic, value=event)
                producer.flush()
                logger.info("Published event: %s", event)
            else:
                logger.info("Match %s hasn't started yet, nothing to publish", match_id)

            if data.get("matchEnded"):
                logger.info("Match %s has ended, stopping producer", match_id)
                break

        except Exception as exc:  # noqa: BLE001 - a single bad poll must not crash the process
            backoff = min(max(backoff * 2, MIN_BACKOFF_SECONDS), MAX_BACKOFF_SECONDS)
            logger.warning("Poll failed (%s); backing off %ds", exc, backoff)
            time.sleep(backoff)
            continue

        sleep_seconds = random.uniform(POLL_INTERVAL_MIN_SECONDS, POLL_INTERVAL_MAX_SECONDS)
        logger.info("Sleeping %.0fs until next poll", sleep_seconds)
        time.sleep(sleep_seconds)

    producer.close()


def main():
    parser = argparse.ArgumentParser(description="Poll a live cricket match and publish to Kafka")
    parser.add_argument(
        "match_id",
        nargs="?",
        default=os.environ.get("LIVE_CRICKET_MATCH_ID") or None,
        help="CricAPI match id (defaults to LIVE_CRICKET_MATCH_ID env var)",
    )
    args = parser.parse_args()

    if not args.match_id:
        logger.error("No match_id given -- pass one as an argument or set LIVE_CRICKET_MATCH_ID")
        sys.exit(1)

    poll_loop(args.match_id)


if __name__ == "__main__":
    main()
