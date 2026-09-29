"""
Loads the saved win probability model and prints predictions for a handful of
hand-picked match situations, so you can sanity check that it makes
cricketing sense before trusting it live -- e.g. needing a few runs off the
last ball should look hopeless, while needing a few off many balls with
wickets in hand should look close to a certainty.

Usage:
    python evaluate.py
"""

import json
import logging
from pathlib import Path

import joblib
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("evaluate")

MODEL_DIR = Path(__file__).resolve().parent / "model"
MODEL_PATH = MODEL_DIR / "win_probability_model.pkl"
FEATURE_LIST_PATH = MODEL_DIR / "feature_list.json"

SCENARIOS = [
    {
        "description": "Needs 5 off the last ball, 1 wicket in hand",
        "runs_needed": 5, "balls_remaining": 1, "wickets_in_hand": 1, "current_run_rate": 7.5,
    },
    {
        "description": "Needs 5 off 12 balls, 8 wickets in hand",
        "runs_needed": 5, "balls_remaining": 12, "wickets_in_hand": 8, "current_run_rate": 8.0,
    },
    {
        "description": "Needs 10 off 6 balls, 2 wickets in hand",
        "runs_needed": 10, "balls_remaining": 6, "wickets_in_hand": 2, "current_run_rate": 8.0,
    },
    {
        "description": "First ball of the chase, target 180",
        "runs_needed": 180, "balls_remaining": 120, "wickets_in_hand": 10, "current_run_rate": 0.0,
    },
    {
        "description": "Comfortable mid-innings: needs 40 off 60 balls, 7 wickets in hand",
        "runs_needed": 40, "balls_remaining": 60, "wickets_in_hand": 7, "current_run_rate": 9.0,
    },
    {
        "description": "Deep trouble mid-innings: needs 70 off 40 balls, 2 wickets in hand",
        "runs_needed": 70, "balls_remaining": 40, "wickets_in_hand": 2, "current_run_rate": 6.5,
    },
]


def required_run_rate(runs_needed, balls_remaining):
    if balls_remaining <= 0:
        return 0.0
    return round(runs_needed / (balls_remaining / 6), 2)


def main():
    if not MODEL_PATH.exists() or not FEATURE_LIST_PATH.exists():
        logger.error("No trained model found -- run train_win_probability.py first.")
        return

    model = joblib.load(MODEL_PATH)
    feature_list = json.loads(FEATURE_LIST_PATH.read_text())

    print(f"{'Scenario':<62} {'Win probability':>16}")
    print("-" * 80)
    for scenario in SCENARIOS:
        features = dict(scenario)
        features["required_run_rate"] = required_run_rate(
            scenario["runs_needed"], scenario["balls_remaining"]
        )
        ordered = np.array([[features[name] for name in feature_list]])
        probability = model.predict_proba(ordered)[0][1]
        print(f"{scenario['description']:<62} {probability:>15.1%}")


if __name__ == "__main__":
    main()
