"""
Unit tests for feature_engineering.py's pure transformation logic
(clean_features, split_by_match, save_feature_list). These don't touch
Postgres -- load_match_state_features/load_train_test_split (the two
functions that do) are thin wrappers around a DB read and are exercised
indirectly by CI's `dbt build` step producing the real mart instead.
"""

import json

import pandas as pd

from feature_engineering import FEATURE_COLUMNS, clean_features, save_feature_list, split_by_match


def test_clean_features_fills_null_current_run_rate_with_zero():
    df = pd.DataFrame({
        "match_id": ["m1", "m1", "m2"],
        "current_run_rate": [None, 8.5, None],
        "required_run_rate": [9.0, 9.0, 9.0],
    })

    cleaned = clean_features(df)

    assert cleaned["current_run_rate"].tolist() == [0, 8.5, 0]


def test_clean_features_leaves_other_columns_unchanged():
    df = pd.DataFrame({
        "match_id": ["m1"],
        "current_run_rate": [None],
        "required_run_rate": [7.25],
        "wickets_in_hand": [6],
    })

    cleaned = clean_features(df)

    assert cleaned["required_run_rate"].tolist() == [7.25]
    assert cleaned["wickets_in_hand"].tolist() == [6]


def test_clean_features_does_not_mutate_the_input_dataframe():
    df = pd.DataFrame({"match_id": ["m1"], "current_run_rate": [None]})
    original = df.copy()

    clean_features(df)

    pd.testing.assert_frame_equal(df, original)


def _rows_for_match(match_id: str, n_rows: int) -> list:
    return [{"match_id": match_id, "ball": i, "chasing_team_won": i % 2} for i in range(n_rows)]


def test_split_by_match_never_splits_a_single_match_across_train_and_test():
    # 10 matches, 5 rows each -- enough matches that an 80/20 split is exact
    rows = []
    for i in range(10):
        rows.extend(_rows_for_match(f"match_{i}", 5))
    df = pd.DataFrame(rows)

    train_df, test_df = split_by_match(df, test_size=0.2, random_state=42)

    train_ids = set(train_df["match_id"].unique())
    test_ids = set(test_df["match_id"].unique())
    assert train_ids.isdisjoint(test_ids)
    assert train_ids | test_ids == set(df["match_id"].unique())


def test_split_by_match_keeps_all_rows_for_a_match_together():
    rows = []
    for i in range(10):
        rows.extend(_rows_for_match(f"match_{i}", 5))
    df = pd.DataFrame(rows)

    train_df, test_df = split_by_match(df, test_size=0.2, random_state=42)

    # every row of a given match_id ends up on the same side of the split
    for match_id in df["match_id"].unique():
        in_train = (train_df["match_id"] == match_id).sum()
        in_test = (test_df["match_id"] == match_id).sum()
        assert in_train == 0 or in_test == 0
        assert in_train + in_test == 5

    assert len(train_df) + len(test_df) == len(df)


def test_split_by_match_respects_approximate_test_size():
    rows = []
    for i in range(20):
        rows.extend(_rows_for_match(f"match_{i}", 3))
    df = pd.DataFrame(rows)

    train_df, test_df = split_by_match(df, test_size=0.25, random_state=42)

    n_train_matches = train_df["match_id"].nunique()
    n_test_matches = test_df["match_id"].nunique()
    assert n_train_matches == 15
    assert n_test_matches == 5


def test_split_by_match_is_deterministic_for_a_fixed_seed():
    rows = []
    for i in range(10):
        rows.extend(_rows_for_match(f"match_{i}", 4))
    df = pd.DataFrame(rows)

    train_1, test_1 = split_by_match(df, test_size=0.2, random_state=42)
    train_2, test_2 = split_by_match(df, test_size=0.2, random_state=42)

    assert set(train_1["match_id"]) == set(train_2["match_id"])
    assert set(test_1["match_id"]) == set(test_2["match_id"])


def test_save_feature_list_writes_expected_json_in_order(tmp_path):
    path = tmp_path / "feature_list.json"

    save_feature_list(path)

    saved = json.loads(path.read_text())
    # order matters -- it's the contract the streaming consumer relies on
    # when building a feature vector for a live prediction
    assert saved == FEATURE_COLUMNS
