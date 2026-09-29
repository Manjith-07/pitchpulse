"""
Loads the match_state_features dbt mart from Postgres, cleans it, and splits
it into train/test sets for the win probability model.

Splitting is done by match_id (never mid-match) so that no rows from the same
match appear in both train and test -- otherwise the model could "learn" a
specific match's trajectory rather than generalizing, which would make the
held-out AUC look better than it really is.

Importable by train_win_probability.py, and also runnable standalone:
    python feature_engineering.py
"""

import json
import logging
import os
from pathlib import Path

import pandas as pd
import psycopg2

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except ImportError:
    pass

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("feature_engineering")

# Order matters: this exact order is what gets saved to feature_list.json and
# is the contract the streaming consumer relies on when building its feature
# vector for a live prediction.
FEATURE_COLUMNS = [
    "runs_needed",
    "balls_remaining",
    "wickets_in_hand",
    "current_run_rate",
    "required_run_rate",
]
LABEL_COLUMN = "chasing_team_won"

MODEL_DIR = Path(__file__).resolve().parent / "model"
FEATURE_LIST_PATH = MODEL_DIR / "feature_list.json"


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        user=os.environ.get("POSTGRES_USER", "pitchpulse"),
        password=os.environ.get("POSTGRES_PASSWORD", "pitchpulse"),
        dbname=os.environ.get("POSTGRES_DB", "pitchpulse"),
    )


def load_match_state_features(conn) -> pd.DataFrame:
    query = f"""
        SELECT match_id, {", ".join(FEATURE_COLUMNS)}, {LABEL_COLUMN}
        FROM match_state_features
    """
    return pd.read_sql_query(query, conn)


def clean_features(df: pd.DataFrame) -> pd.DataFrame:
    """current_run_rate is null only on the very first legal ball of an
    innings (no balls bowled yet to compute a rate from) -- 0 is the correct
    value there, not a missing-data problem. required_run_rate has no nulls
    in practice since a chase always ends before balls_remaining hits 0."""
    df = df.copy()
    df["current_run_rate"] = df["current_run_rate"].fillna(0)
    return df


def split_by_match(df: pd.DataFrame, test_size: float = 0.2, random_state: int = 42):
    match_ids = df["match_id"].unique()
    rng = pd.Series(match_ids).sample(frac=1.0, random_state=random_state)
    cutoff = int(len(rng) * (1 - test_size))
    train_ids = set(rng.iloc[:cutoff])
    test_ids = set(rng.iloc[cutoff:])

    train_df = df[df["match_id"].isin(train_ids)].reset_index(drop=True)
    test_df = df[df["match_id"].isin(test_ids)].reset_index(drop=True)
    return train_df, test_df


def save_feature_list(path: Path = FEATURE_LIST_PATH):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(FEATURE_COLUMNS, indent=2))
    logger.info("Saved feature list to %s: %s", path, FEATURE_COLUMNS)


def load_train_test_split():
    """Convenience entry point for train_win_probability.py."""
    conn = get_connection()
    try:
        df = load_match_state_features(conn)
    finally:
        conn.close()
    df = clean_features(df)
    return split_by_match(df)


def main():
    train_df, test_df = load_train_test_split()
    save_feature_list()

    logger.info("Total rows: %d", len(train_df) + len(test_df))
    logger.info("Train: %d rows across %d matches", len(train_df), train_df["match_id"].nunique())
    logger.info("Test:  %d rows across %d matches", len(test_df), test_df["match_id"].nunique())
    logger.info("Train label balance: %s", train_df[LABEL_COLUMN].value_counts(normalize=True).to_dict())
    logger.info("Test label balance:  %s", test_df[LABEL_COLUMN].value_counts(normalize=True).to_dict())


if __name__ == "__main__":
    main()
