"""
Parses Cricsheet JSON match files and loads them into the `pitchpulse` Postgres
warehouse (raw_matches, raw_deliveries).

Usage:
    python load_cricsheet.py --input-dir /path/to/ipl_raw

Safe to re-run: uses ON CONFLICT DO NOTHING, so already-loaded matches/deliveries
are skipped rather than duplicated or erroring out.

Connection settings are read from environment variables (POSTGRES_HOST,
POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB), which are already
set inside the Airflow container via docker-compose. For running this script by
hand from the host machine, it also tries to load a `.env` file from the repo
root if `python-dotenv` is installed.
"""

import argparse
import json
import logging
import os
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

try:
    from dotenv import load_dotenv

    # repo root is two levels up from ingestion/batch/
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("load_cricsheet")

CREATE_RAW_MATCHES = """
CREATE TABLE IF NOT EXISTS raw_matches (
    match_id TEXT PRIMARY KEY,
    match_type TEXT,
    season TEXT,
    match_date DATE,
    venue TEXT,
    city TEXT,
    team1 TEXT,
    team2 TEXT,
    toss_winner TEXT,
    toss_decision TEXT,
    winner TEXT,
    win_by_type TEXT,
    win_by_margin INTEGER,
    player_of_match TEXT
);
"""

CREATE_RAW_DELIVERIES = """
CREATE TABLE IF NOT EXISTS raw_deliveries (
    match_id TEXT REFERENCES raw_matches(match_id),
    innings_number INTEGER,
    batting_team TEXT,
    over_number INTEGER,
    ball_number INTEGER,
    batter TEXT,
    bowler TEXT,
    non_striker TEXT,
    runs_batter INTEGER,
    runs_extras INTEGER,
    runs_total INTEGER,
    extras_type TEXT,
    is_wicket BOOLEAN DEFAULT FALSE,
    wicket_kind TEXT,
    player_dismissed TEXT,
    PRIMARY KEY (match_id, innings_number, over_number, ball_number)
);
"""

INSERT_MATCH = """
INSERT INTO raw_matches (
    match_id, match_type, season, match_date, venue, city,
    team1, team2, toss_winner, toss_decision, winner,
    win_by_type, win_by_margin, player_of_match
) VALUES (
    %(match_id)s, %(match_type)s, %(season)s, %(match_date)s, %(venue)s, %(city)s,
    %(team1)s, %(team2)s, %(toss_winner)s, %(toss_decision)s, %(winner)s,
    %(win_by_type)s, %(win_by_margin)s, %(player_of_match)s
)
ON CONFLICT (match_id) DO NOTHING;
"""

INSERT_DELIVERIES = """
INSERT INTO raw_deliveries (
    match_id, innings_number, batting_team, over_number, ball_number,
    batter, bowler, non_striker, runs_batter, runs_extras, runs_total,
    extras_type, is_wicket, wicket_kind, player_dismissed
) VALUES %s
ON CONFLICT (match_id, innings_number, over_number, ball_number) DO NOTHING;
"""


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        user=os.environ.get("POSTGRES_USER", "pitchpulse"),
        password=os.environ.get("POSTGRES_PASSWORD", "pitchpulse"),
        dbname=os.environ.get("POSTGRES_DB", "pitchpulse"),
    )


def ensure_schema(conn):
    with conn.cursor() as cur:
        cur.execute(CREATE_RAW_MATCHES)
        cur.execute(CREATE_RAW_DELIVERIES)
    conn.commit()


def parse_match_row(match_id: str, info: dict) -> dict:
    teams = info["teams"]
    toss = info.get("toss", {})
    outcome = info.get("outcome", {})

    winner = outcome.get("winner")
    win_by_type = None
    win_by_margin = None
    by = outcome.get("by")
    if by:
        if "runs" in by:
            win_by_type, win_by_margin = "runs", by["runs"]
        elif "wickets" in by:
            win_by_type, win_by_margin = "wickets", by["wickets"]

    player_of_match_list = info.get("player_of_match")
    player_of_match = player_of_match_list[0] if player_of_match_list else None

    dates = info.get("dates")
    match_date = dates[0] if dates else None

    return {
        "match_id": match_id,
        "match_type": info.get("match_type"),
        "season": str(info.get("season")) if info.get("season") is not None else None,
        "match_date": match_date,
        "venue": info.get("venue"),
        "city": info.get("city"),
        "team1": teams[0],
        "team2": teams[1],
        "toss_winner": toss.get("winner"),
        "toss_decision": toss.get("decision"),
        "winner": winner,
        "win_by_type": win_by_type,
        "win_by_margin": win_by_margin,
        "player_of_match": player_of_match,
    }


def parse_delivery_rows(match_id: str, innings: list) -> list:
    rows = []
    for innings_number, inning in enumerate(innings, start=1):
        batting_team = inning.get("team")
        for over in inning.get("overs", []):
            over_number = over.get("over")
            # position within the over's delivery list, including illegal
            # deliveries (wides/no-balls) -- this just needs to be unique per
            # over, it is not the "legal ball count" used for over progression
            for ball_number, delivery in enumerate(over.get("deliveries", []), start=1):
                runs = delivery.get("runs", {})
                extras = delivery.get("extras")
                extras_type = ",".join(extras.keys()) if extras else None
                wickets = delivery.get("wickets")
                is_wicket = bool(wickets)
                wicket_kind = wickets[0].get("kind") if wickets else None
                player_dismissed = wickets[0].get("player_out") if wickets else None

                rows.append(
                    (
                        match_id,
                        innings_number,
                        batting_team,
                        over_number,
                        ball_number,
                        delivery.get("batter"),
                        delivery.get("bowler"),
                        delivery.get("non_striker"),
                        runs.get("batter", 0),
                        runs.get("extras", 0),
                        runs.get("total", 0),
                        extras_type,
                        is_wicket,
                        wicket_kind,
                        player_dismissed,
                    )
                )
    return rows


def load_file(conn, path: Path) -> int:
    """Loads a single match JSON file. Returns the number of delivery rows inserted."""
    with open(path, "r", encoding="utf-8") as f:
        match_json = json.load(f)

    match_id = path.stem
    match_row = parse_match_row(match_id, match_json["info"])
    delivery_rows = parse_delivery_rows(match_id, match_json.get("innings", []))

    with conn.cursor() as cur:
        cur.execute(INSERT_MATCH, match_row)
        if delivery_rows:
            execute_values(cur, INSERT_DELIVERIES, delivery_rows)
    conn.commit()
    return len(delivery_rows)


def main():
    parser = argparse.ArgumentParser(description="Load Cricsheet JSON files into Postgres")
    parser.add_argument(
        "--input-dir",
        default="data/ipl_raw",
        help="Directory containing Cricsheet match JSON files",
    )
    args = parser.parse_args()

    input_dir = Path(args.input_dir)
    json_files = sorted(input_dir.glob("*.json"))
    if not json_files:
        logger.error("No JSON files found in %s", input_dir)
        return

    conn = get_connection()
    ensure_schema(conn)

    matches_processed = 0
    deliveries_inserted = 0
    skipped = []

    for path in json_files:
        try:
            deliveries_inserted += load_file(conn, path)
            matches_processed += 1
        except Exception as exc:  # noqa: BLE001 - we want to log and continue on any parse error
            conn.rollback()
            skipped.append((path.name, str(exc)))
            logger.warning("Skipped %s: %s", path.name, exc)

    conn.close()

    logger.info("=== Load summary ===")
    logger.info("Matches processed: %d", matches_processed)
    logger.info("Deliveries inserted (or already present): %d", deliveries_inserted)
    logger.info("Matches skipped: %d", len(skipped))
    for filename, reason in skipped:
        logger.info("  - %s: %s", filename, reason)


if __name__ == "__main__":
    main()
