"""
Subscribes to the `live-match-events` Kafka topic. For every event, computes
current/required run rate, applies the trained win probability model, and
writes the latest state per match to Redis at `match:{match_id}:state`
(with a TTL so stale matches expire automatically). Also appends every event
to the `live_events_log` Postgres table so live matches can be replayed or
analyzed later.

If ml/model/win_probability_model.pkl doesn't exist yet (i.e. phase 3 hasn't
been run), this still runs fine -- it just writes a null win_probability and
logs a warning once at startup. Restart the consumer after training a model
to pick it up (the model directory is bind-mounted, so no rebuild needed).
"""

import json
import logging
import os
from pathlib import Path

import joblib
import numpy as np
import psycopg2
import redis
from kafka import KafkaConsumer

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("consumer")

REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_PATH = REPO_ROOT / "ml" / "model" / "win_probability_model.pkl"
FEATURE_LIST_PATH = REPO_ROOT / "ml" / "model" / "feature_list.json"

REDIS_STATE_TTL_SECONDS = 6 * 60 * 60  # a few hours, per spec

CREATE_LIVE_EVENTS_LOG = """
CREATE TABLE IF NOT EXISTS live_events_log (
    id SERIAL PRIMARY KEY,
    match_id TEXT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    innings_number INTEGER,
    over_number INTEGER,
    ball_number INTEGER,
    batting_team TEXT,
    bowling_team TEXT,
    current_score INTEGER,
    wickets_lost INTEGER,
    target INTEGER,
    balls_remaining INTEGER,
    current_run_rate DOUBLE PRECISION,
    required_run_rate DOUBLE PRECISION,
    win_probability DOUBLE PRECISION
);
"""

INSERT_LIVE_EVENT = """
INSERT INTO live_events_log (
    match_id, event_timestamp, innings_number, over_number, ball_number,
    batting_team, bowling_team, current_score, wickets_lost, target,
    balls_remaining, current_run_rate, required_run_rate, win_probability
) VALUES (
    %(match_id)s, %(timestamp)s, %(innings_number)s, %(over_number)s, %(ball_number)s,
    %(batting_team)s, %(bowling_team)s, %(current_score)s, %(wickets_lost)s, %(target)s,
    %(balls_remaining)s, %(current_run_rate)s, %(required_run_rate)s, %(win_probability)s
);
"""


def load_model():
    if not MODEL_PATH.exists() or not FEATURE_LIST_PATH.exists():
        logger.warning(
            "No trained model found at %s -- win_probability will be null until "
            "ml/train_win_probability.py (phase 3) has been run.",
            MODEL_PATH,
        )
        return None, None
    model = joblib.load(MODEL_PATH)
    feature_list = json.loads(FEATURE_LIST_PATH.read_text())
    logger.info("Loaded win probability model, features: %s", feature_list)
    return model, feature_list


def get_redis_client() -> redis.Redis:
    return redis.Redis(
        host=os.environ.get("REDIS_HOST", "localhost"),
        port=int(os.environ.get("REDIS_PORT", 6379)),
        decode_responses=True,
    )


def get_pg_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        user=os.environ.get("POSTGRES_USER", "pitchpulse"),
        password=os.environ.get("POSTGRES_PASSWORD", "pitchpulse"),
        dbname=os.environ.get("POSTGRES_DB", "pitchpulse"),
    )


def compute_run_rates(event: dict):
    legal_balls_bowled = event["over_number"] * 6 + event["ball_number"]

    current_run_rate = None
    if legal_balls_bowled > 0 and event.get("current_score") is not None:
        current_run_rate = round(event["current_score"] / (legal_balls_bowled / 6), 2)

    required_run_rate = None
    target = event.get("target")
    balls_remaining = event.get("balls_remaining")
    if target is not None and balls_remaining:
        runs_needed = max(target - event["current_score"], 0)
        required_run_rate = round(runs_needed / (balls_remaining / 6), 2)

    return current_run_rate, required_run_rate


def compute_win_probability(event: dict, current_run_rate, required_run_rate, model, feature_list):
    if model is None or event.get("target") is None or event.get("balls_remaining") is None:
        return None

    runs_needed = max(event["target"] - event["current_score"], 0)
    balls_remaining = event["balls_remaining"]
    wickets_in_hand = max(10 - (event.get("wickets_lost") or 0), 0)

    # The innings is over (no balls or no wickets left) and the model was
    # never trained on this state -- training features are always "about to
    # face this ball", so the minimum balls_remaining/wickets_in_hand seen in
    # training is 1, not 0 (once either hits 0, the innings ends and no
    # further deliveries exist to generate a training row from). The outcome
    # here is already known for certain, so skip the model rather than feed
    # it an out-of-distribution required_run_rate.
    if balls_remaining <= 0 or wickets_in_hand <= 0:
        return 1.0 if runs_needed <= 0 else 0.0

    features_by_name = {
        "runs_needed": runs_needed,
        "balls_remaining": balls_remaining,
        "wickets_in_hand": wickets_in_hand,
        "current_run_rate": current_run_rate or 0,
        "required_run_rate": required_run_rate or 0,
    }
    ordered = np.array([[features_by_name[name] for name in feature_list]])
    return round(float(model.predict_proba(ordered)[0][1]), 4)


def log_to_postgres(conn, event: dict, current_run_rate, required_run_rate, win_probability):
    with conn.cursor() as cur:
        cur.execute(
            INSERT_LIVE_EVENT,
            {
                **event,
                "current_run_rate": current_run_rate,
                "required_run_rate": required_run_rate,
                "win_probability": win_probability,
            },
        )
    conn.commit()


def main():
    model, feature_list = load_model()
    redis_client = get_redis_client()

    pg_conn = None
    try:
        pg_conn = get_pg_connection()
        with pg_conn.cursor() as cur:
            cur.execute(CREATE_LIVE_EVENTS_LOG)
        pg_conn.commit()
    except Exception as exc:  # noqa: BLE001 - live_events_log is a nice-to-have, not critical
        logger.warning("Could not set up Postgres live_events_log (%s); continuing without it", exc)
        pg_conn = None

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC", "live-match-events")
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap.split(","),
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="pitchpulse-consumer",
    )

    logger.info("Listening on topic %s", topic)
    for message in consumer:
        event = message.value
        current_run_rate, required_run_rate = compute_run_rates(event)
        win_probability = compute_win_probability(event, current_run_rate, required_run_rate, model, feature_list)

        state = {
            **event,
            "current_run_rate": current_run_rate,
            "required_run_rate": required_run_rate,
            "win_probability": win_probability,
        }
        redis_key = f"match:{event['match_id']}:state"
        redis_client.set(redis_key, json.dumps(state), ex=REDIS_STATE_TTL_SECONDS)
        logger.info(
            "Updated %s: score=%s/%s crr=%s rrr=%s win_probability=%s",
            redis_key, event.get("current_score"), event.get("wickets_lost"),
            current_run_rate, required_run_rate, win_probability,
        )

        if pg_conn is not None:
            try:
                log_to_postgres(pg_conn, event, current_run_rate, required_run_rate, win_probability)
            except Exception as exc:  # noqa: BLE001 - don't let a logging failure kill the consumer
                logger.warning("Failed to log event to Postgres: %s", exc)
                pg_conn.rollback()


if __name__ == "__main__":
    main()
