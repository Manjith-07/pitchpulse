"""
Trains two win probability models on match_state_features (LogisticRegression
baseline, GradientBoostingClassifier as a stretch comparison), evaluates both
on a held-out test set (split by match_id, see feature_engineering.py), and
saves whichever one scores better as win_probability_model.pkl -- that's the
file the streaming consumer loads to score live matches.

Usage:
    python train_win_probability.py
"""

import logging
from pathlib import Path

import joblib
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from feature_engineering import FEATURE_COLUMNS, LABEL_COLUMN, MODEL_DIR, load_train_test_split, save_feature_list

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("train_win_probability")

MODEL_PATH = MODEL_DIR / "win_probability_model.pkl"
REPORT_PATH = MODEL_DIR / "evaluation_report.txt"


def train_logistic_regression(X_train, y_train):
    # Logistic regression needs its features on a comparable scale (runs_needed
    # ranges 0-250+, wickets_in_hand only 0-10), so it's wrapped in a Pipeline
    # with a scaler. GradientBoostingClassifier below is tree-based and
    # doesn't need this.
    model = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", LogisticRegression(max_iter=1000)),
    ])
    model.fit(X_train, y_train)
    return model


def train_gradient_boosting(X_train, y_train):
    model = GradientBoostingClassifier(random_state=42)
    model.fit(X_train, y_train)
    return model


def evaluate(model, X_test, y_test):
    probs = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, probs)
    loss = log_loss(y_test, probs)
    return auc, loss


def main():
    logger.info("Loading train/test split from match_state_features...")
    train_df, test_df = load_train_test_split()
    save_feature_list()

    # Train on plain numpy arrays (not a DataFrame) so the fitted model
    # doesn't retain column-name metadata -- otherwise scikit-learn warns on
    # every prediction made from a plain feature vector, which is exactly how
    # both the streaming consumer and evaluate.py call predict_proba().
    X_train = train_df[FEATURE_COLUMNS].to_numpy()
    y_train = train_df[LABEL_COLUMN].to_numpy()
    X_test = test_df[FEATURE_COLUMNS].to_numpy()
    y_test = test_df[LABEL_COLUMN].to_numpy()

    logger.info("Training LogisticRegression on %d rows...", len(X_train))
    logreg = train_logistic_regression(X_train, y_train)
    logreg_auc, logreg_logloss = evaluate(logreg, X_test, y_test)

    logger.info("Training GradientBoostingClassifier on %d rows...", len(X_train))
    gbc = train_gradient_boosting(X_train, y_train)
    gbc_auc, gbc_logloss = evaluate(gbc, X_test, y_test)

    candidates = [
        ("LogisticRegression", logreg, logreg_auc, logreg_logloss),
        ("GradientBoostingClassifier", gbc, gbc_auc, gbc_logloss),
    ]
    # Higher AUC wins; a lower log loss breaks a tie.
    best_name, best_model, best_auc, best_logloss = max(
        candidates, key=lambda c: (c[2], -c[3])
    )

    report_lines = [
        "PitchPulse win probability model -- evaluation report",
        "=" * 55,
        f"Training rows: {len(X_train)} ({train_df['match_id'].nunique()} matches)",
        f"Test rows:     {len(X_test)} ({test_df['match_id'].nunique()} matches)",
        f"Features:      {FEATURE_COLUMNS}",
        "",
        "Model                        AUC       Log loss",
        "-" * 55,
        f"LogisticRegression           {logreg_auc:.4f}    {logreg_logloss:.4f}",
        f"GradientBoostingClassifier   {gbc_auc:.4f}    {gbc_logloss:.4f}",
        "",
        f"Selected model: {best_name} (AUC={best_auc:.4f}, log loss={best_logloss:.4f})",
        f"Saved to: ml/model/{MODEL_PATH.name}",
    ]
    report = "\n".join(report_lines)

    print(report)
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(report + "\n")
    joblib.dump(best_model, MODEL_PATH)
    logger.info("Wrote %s and %s", MODEL_PATH, REPORT_PATH)


if __name__ == "__main__":
    main()
