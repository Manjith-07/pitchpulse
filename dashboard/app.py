"""
PitchPulse dashboard (Streamlit).

Two modes, picked from the sidebar:
  - Live match: current score/run-rates/win-probability for a tracked match,
    plus a live-updating line chart of win probability as the innings unfolds.
  - Historical explorer: player career stats, team win% by venue, and
    phase-wise (powerplay/middle/death) run-rate trends.

All data comes from the FastAPI service (api/main.py) -- this file never talks
to Postgres or Redis directly.
"""

import os
import time
from pathlib import Path

import pandas as pd
import requests
import streamlit as st

try:
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
except ImportError:
    pass

API_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8000")

st.set_page_config(page_title="PitchPulse", page_icon="🏏", layout="wide")

PHASE_ORDER = ["powerplay", "middle", "death"]


def api_get(path: str, params: dict | None = None):
    """GETs a path from the API. Returns (data, error_message) -- exactly one
    of the two is None, so callers can check `if error:` before using data."""
    try:
        resp = requests.get(f"{API_BASE_URL}{path}", params=params, timeout=10)
    except requests.exceptions.ConnectionError:
        return None, f"Can't reach the API at {API_BASE_URL}. Is it running?"

    if resp.status_code == 404:
        return None, resp.json().get("detail", "Not found")
    if not resp.ok:
        return None, f"API error ({resp.status_code}): {resp.text}"
    return resp.json(), None


def render_live_tab():
    matches, error = api_get("/live/matches")
    if error:
        st.error(error)
        return

    match_ids = matches["match_ids"]
    if not match_ids:
        st.info(
            "No live matches currently tracked. Start the producer against a real "
            "match, or try `docker compose --profile streaming run --rm producer "
            "python ingestion/streaming/simulate_match.py demo-001` to see this "
            "tab populated without spending any API quota."
        )
        return

    match_id = st.sidebar.selectbox("Live match", match_ids)

    state, error = api_get(f"/live/match/{match_id}")
    if error:
        st.error(error)
        return

    st.subheader(f"{state.get('batting_team') or '?'} vs {state.get('bowling_team') or '?'}")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric(
        "Score",
        f"{state.get('current_score', '?')}/{state.get('wickets_lost', '?')}",
        f"over {state['over_number']}.{state['ball_number']}",
    )
    current_rr = state.get("current_run_rate")
    required_rr = state.get("required_run_rate")
    # Note: these can legitimately be 0.0 (e.g. a maiden start, or a chase
    # that has already reached its target), so check for None explicitly
    # rather than using `value or "—"`, which would wrongly treat 0.0 as
    # missing data.
    col2.metric("Current run rate", current_rr if current_rr is not None else "—")
    col3.metric("Required run rate", required_rr if required_rr is not None else "—")
    win_prob = state.get("win_probability")
    col4.metric(
        f"{state.get('batting_team') or 'Batting side'} win probability",
        f"{win_prob:.0%}" if win_prob is not None else "—",
    )
    if state.get("target"):
        st.caption(f"Target: {state['target']}")

    # Accumulate a per-match history of win-probability snapshots in session
    # state, client-side, across reruns -- the API only ever returns the
    # latest state, so this is what turns single snapshots into a trend line.
    history_key = f"history_{match_id}"
    if history_key not in st.session_state:
        st.session_state[history_key] = []
    history = st.session_state[history_key]

    balls_into_innings = state["over_number"] * 6 + state["ball_number"]
    already_have_this_point = history and history[-1]["balls_into_innings"] == balls_into_innings
    if win_prob is not None and not already_have_this_point:
        history.append({"balls_into_innings": balls_into_innings, "win_probability": win_prob})

    if history:
        df = pd.DataFrame(history).set_index("balls_into_innings")
        st.line_chart(df["win_probability"], y_label="Win probability")
    else:
        st.caption("Win probability chart will appear once the model has scored at least one ball.")

    st.divider()
    auto_refresh = st.checkbox("Auto-refresh", value=True)
    refresh_seconds = st.number_input("Refresh interval (seconds)", min_value=2, max_value=60, value=5)
    if st.button("Refresh now") or auto_refresh:
        if auto_refresh:
            time.sleep(refresh_seconds)
        st.rerun()


def render_player_card(stats: dict):
    batting_col, bowling_col = st.columns(2)
    with batting_col:
        st.markdown("**Batting**")
        st.metric("Runs", stats["runs_scored"])
        st.metric("Balls faced", stats["balls_faced"])
        st.metric("Strike rate", stats["batting_strike_rate"])
        st.metric("Dismissals", stats["dismissals"])
        st.metric("Boundaries", stats["boundaries_hit"])
    with bowling_col:
        st.markdown("**Bowling**")
        st.metric("Balls bowled", stats["balls_bowled"])
        st.metric("Runs conceded", stats["runs_conceded"])
        st.metric("Wickets", stats["wickets_taken"])
        st.metric("Economy rate", stats["economy_rate"])


def render_historical_tab():
    st.subheader("Player career stats")
    player_name = st.text_input("Player name", placeholder="e.g. V Kohli")
    if player_name:
        matches, error = api_get(f"/historical/player/{player_name}")
        if error:
            st.warning(error)
        elif len(matches) == 1:
            render_player_card(matches[0])
        else:
            # Cricsheet stores names as initials + surname, so a partial
            # search like "Dhoni" or "kohli" often matches more than one
            # player -- let the user pick rather than guessing for them.
            st.info(f"{len(matches)} players matched '{player_name}':")
            names = [m["player"] for m in matches]
            chosen = st.selectbox("Pick a player", names, key="player_disambiguation")
            render_player_card(next(m for m in matches if m["player"] == chosen))

    st.divider()
    st.subheader("Team win percentage by venue")
    team_name = st.text_input("Team name", placeholder="e.g. Mumbai Indians", key="team_venues")
    if team_name:
        rows, error = api_get(f"/historical/team/{team_name}/venues")
        if error:
            st.warning(error)
        elif not rows:
            st.info(f"No venue data found for '{team_name}'.")
        else:
            df = pd.DataFrame(rows).sort_values("matches_played", ascending=False)
            st.dataframe(df, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Phase-wise run rate trends")
    trend_col1, trend_col2 = st.columns(2)
    trend_team = trend_col1.text_input("Team", placeholder="e.g. Mumbai Indians", key="team_trends")
    trend_season = trend_col2.text_input("Season (optional)", placeholder="e.g. 2019")
    if trend_team:
        params = {"team": trend_team}
        if trend_season:
            params["season"] = trend_season
        rows, error = api_get("/historical/phase-trends", params=params)
        if error:
            st.warning(error)
        elif not rows:
            st.info(f"No phase trend data found for '{trend_team}'.")
        else:
            df = pd.DataFrame(rows)
            # one bar series per season, phases in cricket order rather than alphabetical
            pivot = df.pivot_table(index="phase", columns="season", values="average_run_rate")
            pivot = pivot.reindex(PHASE_ORDER)
            st.bar_chart(pivot, y_label="Average run rate")


st.title("🏏 PitchPulse")

mode = st.sidebar.radio("Mode", ["Live match", "Historical explorer"])

if mode == "Live match":
    render_live_tab()
else:
    render_historical_tab()
