"""
Unit tests for load_cricsheet.py's parsing logic (parse_match_row,
parse_delivery_rows). These are pure functions -- no Postgres connection is
opened -- so the tests run against a small, hand-built match dict shaped like
real Cricsheet JSON, rather than needing a downloaded fixture file (data/ is
gitignored and not present in a fresh clone or in CI).
"""

from load_cricsheet import parse_delivery_rows, parse_match_row

SAMPLE_INFO = {
    "match_type": "T20",
    "season": "2020",
    "dates": ["2020-04-01"],
    "venue": "Test Ground",
    "city": "Test City",
    "teams": ["Team A", "Team B"],
    "toss": {"winner": "Team A", "decision": "bat"},
    "outcome": {"winner": "Team A", "by": {"runs": 10}},
    "player_of_match": ["X Player"],
}

# One over for each innings. Innings 1's over deliberately includes a wide
# (illegal delivery), a boundary, and a wicket, to exercise every branch of
# parse_delivery_rows in a single small fixture.
SAMPLE_INNINGS = [
    {
        "team": "Team A",
        "overs": [
            {
                "over": 0,
                "deliveries": [
                    {
                        "batter": "P1", "bowler": "P2", "non_striker": "P3",
                        "runs": {"batter": 1, "extras": 0, "total": 1},
                    },
                    {
                        "batter": "P3", "bowler": "P2", "non_striker": "P1",
                        "runs": {"batter": 0, "extras": 1, "total": 1},
                        "extras": {"wides": 1},
                    },
                    {
                        "batter": "P3", "bowler": "P2", "non_striker": "P1",
                        "runs": {"batter": 4, "extras": 0, "total": 4},
                    },
                    {
                        "batter": "P3", "bowler": "P2", "non_striker": "P1",
                        "runs": {"batter": 0, "extras": 0, "total": 0},
                        "wickets": [{"player_out": "P3", "kind": "bowled", "fielders": []}],
                    },
                ],
            }
        ],
    },
    {
        "team": "Team B",
        "overs": [
            {
                "over": 0,
                "deliveries": [
                    {
                        "batter": "P4", "bowler": "P5", "non_striker": "P6",
                        "runs": {"batter": 6, "extras": 0, "total": 6},
                    },
                ],
            }
        ],
    },
]


def test_parse_match_row_extracts_expected_fields():
    row = parse_match_row("test_match_1", SAMPLE_INFO)

    assert row == {
        "match_id": "test_match_1",
        "match_type": "T20",
        "season": "2020",
        "match_date": "2020-04-01",
        "venue": "Test Ground",
        "city": "Test City",
        "team1": "Team A",
        "team2": "Team B",
        "toss_winner": "Team A",
        "toss_decision": "bat",
        "winner": "Team A",
        "win_by_type": "runs",
        "win_by_margin": 10,
        "player_of_match": "X Player",
    }


def test_parse_match_row_handles_missing_optional_fields():
    minimal_info = {
        "teams": ["Team A", "Team B"],
        # no toss, outcome, dates, season, venue, city, player_of_match
    }
    row = parse_match_row("test_match_2", minimal_info)

    assert row["team1"] == "Team A"
    assert row["team2"] == "Team B"
    assert row["winner"] is None
    assert row["win_by_type"] is None
    assert row["win_by_margin"] is None
    assert row["match_date"] is None
    assert row["season"] is None
    assert row["player_of_match"] is None


def test_parse_match_row_handles_win_by_wickets():
    info = {**SAMPLE_INFO, "outcome": {"winner": "Team B", "by": {"wickets": 4}}}
    row = parse_match_row("test_match_3", info)

    assert row["winner"] == "Team B"
    assert row["win_by_type"] == "wickets"
    assert row["win_by_margin"] == 4


def test_parse_delivery_rows_row_count_includes_illegal_deliveries():
    rows = parse_delivery_rows("test_match_1", SAMPLE_INNINGS)
    # innings 1 has 4 deliveries (including the wide), innings 2 has 1
    assert len(rows) == 5


def test_parse_delivery_rows_ball_number_counts_every_delivery_in_order():
    rows = parse_delivery_rows("test_match_1", SAMPLE_INNINGS)
    innings_1_rows = [r for r in rows if r[1] == 1]
    ball_numbers = [r[4] for r in innings_1_rows]
    # 1-indexed position in the over's delivery list, including the wide --
    # this is deliberately NOT the "legal ball count" used elsewhere
    assert ball_numbers == [1, 2, 3, 4]


def test_parse_delivery_rows_extracts_runs_correctly():
    rows = parse_delivery_rows("test_match_1", SAMPLE_INNINGS)
    # column order: match_id, innings_number, batting_team, over_number,
    # ball_number, batter, bowler, non_striker, runs_batter, runs_extras,
    # runs_total, extras_type, is_wicket, wicket_kind, player_dismissed
    first_ball = rows[0]
    assert first_ball[8:11] == (1, 0, 1)  # runs_batter, runs_extras, runs_total

    boundary_ball = rows[2]
    assert boundary_ball[8:11] == (4, 0, 4)


def test_parse_delivery_rows_flags_extras_type_for_wide():
    rows = parse_delivery_rows("test_match_1", SAMPLE_INNINGS)
    wide_row = rows[1]
    assert wide_row[11] == "wides"

    non_extra_row = rows[0]
    assert non_extra_row[11] is None


def test_parse_delivery_rows_flags_wicket_details():
    rows = parse_delivery_rows("test_match_1", SAMPLE_INNINGS)
    wicket_row = rows[3]
    is_wicket, wicket_kind, player_dismissed = wicket_row[12:15]

    assert is_wicket is True
    assert wicket_kind == "bowled"
    assert player_dismissed == "P3"

    non_wicket_row = rows[0]
    assert non_wicket_row[12] is False
    assert non_wicket_row[13] is None
    assert non_wicket_row[14] is None


def test_parse_delivery_rows_assigns_innings_number_and_batting_team():
    rows = parse_delivery_rows("test_match_1", SAMPLE_INNINGS)
    innings_1_rows = [r for r in rows if r[1] == 1]
    innings_2_rows = [r for r in rows if r[1] == 2]

    assert len(innings_1_rows) == 4
    assert all(r[2] == "Team A" for r in innings_1_rows)
    assert len(innings_2_rows) == 1
    assert innings_2_rows[0][2] == "Team B"
