"""
Regression test for a real off-by-one bug in simulate_match.py: over_number
was emitted as the 0-indexed loop variable (the over currently being bowled)
instead of the count of fully-completed overs, which is what consumer.py
assumes when it reconstructs balls bowled as `over_number * 6 + ball_number`.
That mismatch under-counted balls bowled by a full over throughout every
simulated match, most visibly making the last over look like the innings had
already finished (balls_remaining wrongly hit 0 a full over early), which
forced compute_win_probability's "innings is over" short-circuit to fire
too soon.

This test checks the invariant that would have caught it: for every event,
balls bowled (via over_number/ball_number) plus balls_remaining must equal
120 (a full T20 innings) -- no gaps, no double-counting.
"""

from simulate_match import generate_chase


def test_balls_bowled_and_balls_remaining_always_sum_to_a_full_innings():
    for event in generate_chase(target=175, batting_team="A", bowling_team="B"):
        balls_bowled = event["over_number"] * 6 + event["ball_number"]
        assert balls_bowled + event["balls_remaining"] == 120


def test_over_number_counts_completed_overs_not_the_zero_indexed_loop_variable():
    events = list(generate_chase(target=999999, batting_team="A", bowling_team="B"))
    # target is unreachable, so the full 20 overs are generated
    over_numbers = [e["over_number"] for e in events]
    assert over_numbers == list(range(1, 21))


def test_balls_remaining_counts_down_by_a_full_over_each_event():
    events = list(generate_chase(target=999999, batting_team="A", bowling_team="B"))
    balls_remaining = [e["balls_remaining"] for e in events]
    assert balls_remaining == list(range(114, -6, -6))


def test_last_over_is_not_mistaken_for_an_already_finished_innings():
    events = list(generate_chase(target=999999, batting_team="A", bowling_team="B"))
    second_to_last = events[-2]
    # about to bowl the last over: 6 legitimate balls left, not 0
    assert second_to_last["balls_remaining"] == 6
