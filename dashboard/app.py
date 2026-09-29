"""
PitchPulse dashboard (Streamlit).

Two modes, picked from the sidebar:
  - Live match: current score/run-rates/win-probability for a tracked match,
    plus a live-updating line chart of win probability as the innings unfolds.
  - Historical explorer: player career stats, team win% by venue, and
    phase-wise (powerplay/middle/death) run-rate trends.

All data comes from the FastAPI service (api/main.py) -- this file never talks
to Postgres or Redis directly.

Visual design: a night-match, stadium-under-floodlights palette with a
monospace face for anything that's a live, constantly-updating number (score,
run rates, win probability) so digits don't jitter in width as they change,
and a grotesque sans for headings/labels. See inject_css() for the full
rationale on each choice.
"""

import os
import time
from pathlib import Path

import altair as alt
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

# -- Palette -------------------------------------------------------------
# Grounded in a floodlit night match rather than a generic dashboard: a
# near-black green-tinted background (turf in shadow), a slightly lifted
# panel tone for surfaces, pitch green as the one primary accent, and ball
# red held back exclusively for live/urgent signals rather than spent on
# decoration.
BG = "#0D1512"
PANEL = "#16211C"
ACCENT = "#3E8451"
ACCENT_LIGHT = "#6FA87A"  # lighter tint of the accent, for a second data series
ACCENT_DARK = "#274D33"  # darker tint of the accent, for a third data series
DANGER = "#B4402F"
TEXT = "#EDEAE1"
TEXT_MUTED = "#8A9690"
BORDER = "rgba(138, 150, 144, 0.16)"

FONT_SANS = "Space Grotesk"
FONT_MONO = "JetBrains Mono"


def inject_css():
    st.markdown(
        f"""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600;700&display=swap');

        html, body, .stApp {{
            background-color: {BG} !important;
            color: {TEXT} !important;
        }}
        /* Set the sans as the one inherited default rather than re-declaring
           it on every span/div -- Streamlit renders its own icons (sidebar
           collapse arrow, etc) as ligature text in an icon font via nested
           spans, and a blanket span/div rule beats that with higher CSS
           specificity, turning icons into literal words like
           "keyboard_double_arrow_left". Inheritance alone reaches all our
           own text without touching icon elements, which set their own
           font-family. */
        .stApp {{
            font-family: '{FONT_SANS}', sans-serif;
        }}
        .stApp h1, .stApp h2, .stApp h3, .stApp h4 {{
            font-family: '{FONT_SANS}', sans-serif;
            font-weight: 600;
            letter-spacing: -0.01em;
            color: {TEXT};
        }}
        [data-testid="stIconMaterial"] {{
            font-family: 'Material Symbols Rounded', 'Material Symbols Outlined', 'Material Icons' !important;
        }}
        [data-testid="stSidebar"] {{
            background-color: {PANEL};
            border-right: 1px solid {BORDER};
        }}
        [data-testid="stHeader"] {{
            background-color: {BG};
        }}
        hr {{ border-color: {BORDER}; }}

        /* Inputs: flat panels, no default Streamlit rounded-shadow look */
        .stTextInput input, .stNumberInput input,
        .stSelectbox div[data-baseweb="select"] > div {{
            background-color: {PANEL} !important;
            color: {TEXT} !important;
            border: 1px solid {BORDER} !important;
            border-radius: 4px !important;
            box-shadow: none !important;
            font-family: '{FONT_SANS}', sans-serif !important;
        }}
        .stTextInput input::placeholder {{ color: {TEXT_MUTED} !important; opacity: 1; }}

        /* Buttons: flat, accent border, invert on hover -- no drop shadow */
        .stButton button {{
            background-color: {PANEL};
            color: {TEXT};
            border: 1px solid {ACCENT};
            border-radius: 4px;
            font-family: '{FONT_SANS}', sans-serif;
            font-weight: 500;
            box-shadow: none;
        }}
        .stButton button:hover {{
            background-color: {ACCENT};
            color: {BG};
            border-color: {ACCENT};
        }}
        .stButton button:focus:not(:active) {{
            color: {TEXT};
            border-color: {ACCENT};
        }}

        input[type="checkbox"], input[type="radio"] {{ accent-color: {ACCENT}; }}

        /* Info/warning banners: flat panel instead of Streamlit's default
           rounded-with-shadow alert card */
        [data-testid="stAlert"] {{
            background-color: {PANEL};
            border: 1px solid {BORDER};
            border-radius: 4px;
            box-shadow: none;
            color: {TEXT};
        }}

        /* ---- Custom components (all hand-built via st.markdown) ---- */

        .pp-mono {{
            font-family: '{FONT_MONO}', monospace;
            font-variant-numeric: tabular-nums;
        }}

        @keyframes pp-pulse-in {{
            from {{ opacity: 0; transform: scale(0.97); }}
            to {{ opacity: 1; transform: scale(1); }}
        }}

        .pp-live-badge {{
            display: inline-flex;
            align-items: center;
            gap: 0.4rem;
            background-color: rgba(180, 64, 47, 0.15);
            color: {DANGER};
            font-family: '{FONT_SANS}', sans-serif;
            font-size: 0.75rem;
            font-weight: 600;
            padding: 0.25rem 0.6rem;
            border-radius: 3px;
            letter-spacing: 0.03em;
            vertical-align: middle;
        }}
        .pp-live-dot {{
            width: 6px; height: 6px; border-radius: 50%;
            background-color: {DANGER};
        }}

        .pp-hero {{
            text-align: center;
            padding: 1.25rem 0 0.25rem 0;
            animation: pp-pulse-in 0.5s ease-out;
        }}
        .pp-hero-label {{
            color: {TEXT_MUTED};
            font-size: 0.95rem;
            margin-bottom: 0.1rem;
        }}
        .pp-hero-value {{
            font-family: '{FONT_MONO}', monospace;
            font-variant-numeric: tabular-nums;
            font-size: 5.5rem;
            font-weight: 700;
            color: {ACCENT_LIGHT};
            line-height: 1;
        }}

        .pp-scoreboard {{
            display: flex;
            background-color: {PANEL};
            border-radius: 6px;
            padding: 1rem 0;
            margin: 1rem 0 1.5rem 0;
        }}
        .pp-scoreboard-segment {{
            flex: 1;
            text-align: center;
            padding: 0 0.5rem;
            border-left: 1px solid {BORDER};
        }}
        .pp-scoreboard-segment:first-child {{ border-left: none; }}
        .pp-scoreboard-label {{
            color: {TEXT_MUTED};
            font-size: 0.75rem;
            letter-spacing: 0.04em;
            margin-bottom: 0.3rem;
        }}
        .pp-scoreboard-value {{
            font-family: '{FONT_MONO}', monospace;
            font-variant-numeric: tabular-nums;
            font-size: 1.7rem;
            font-weight: 600;
            color: {TEXT};
        }}

        .pp-scorecard {{
            width: 100%;
            border-collapse: collapse;
        }}
        .pp-scorecard th {{
            text-align: right;
            color: {TEXT_MUTED};
            font-weight: 500;
            font-size: 0.75rem;
            letter-spacing: 0.03em;
            padding: 0.4rem 0.6rem;
            border-bottom: 1px solid {BORDER};
        }}
        .pp-scorecard th:first-child {{ text-align: left; }}
        .pp-scorecard td {{
            padding: 0.5rem 0.6rem;
            border-bottom: 1px solid {BORDER};
            color: {TEXT};
        }}
        .pp-scorecard td.pp-name {{ font-weight: 600; }}
        .pp-scorecard td.pp-num {{
            font-family: '{FONT_MONO}', monospace;
            font-variant-numeric: tabular-nums;
            text-align: right;
            color: {TEXT};
        }}
        .pp-scorecard td.pp-num.pp-muted {{ color: {TEXT_MUTED}; }}
        .pp-scorecard tr:hover td {{ background-color: rgba(62, 132, 81, 0.08); }}

        .pp-table {{ width: 100%; border-collapse: collapse; }}
        .pp-table th {{
            text-align: left;
            color: {TEXT_MUTED};
            font-weight: 500;
            font-size: 0.75rem;
            letter-spacing: 0.03em;
            padding: 0.5rem 0.75rem;
            border-bottom: 1px solid {BORDER};
        }}
        .pp-table td {{
            padding: 0.5rem 0.75rem;
            border-bottom: 1px solid {BORDER};
            color: {TEXT};
        }}
        .pp-table td.pp-num {{
            font-family: '{FONT_MONO}', monospace;
            font-variant-numeric: tabular-nums;
            text-align: right;
        }}
        .pp-table tr:hover td {{ background-color: rgba(62, 132, 81, 0.06); }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def interpolate_hex(start: str, end: str, t: float) -> str:
    """Linearly interpolates between two hex colors at t in [0, 1] -- used to
    build an N-step sequential ramp for an arbitrary number of seasons
    without ever repeating a color, unlike cycling through a short fixed list."""
    start, end = start.lstrip("#"), end.lstrip("#")
    r1, g1, b1 = int(start[0:2], 16), int(start[2:4], 16), int(start[4:6], 16)
    r2, g2, b2 = int(end[0:2], 16), int(end[2:4], 16), int(end[4:6], 16)
    r = round(r1 + (r2 - r1) * t)
    g = round(g1 + (g2 - g1) * t)
    b = round(b1 + (b2 - b1) * t)
    return f"#{r:02x}{g:02x}{b:02x}"


def fmt(value, decimals=None, suffix=""):
    """Formats a possibly-null numeric value, consistently, for display.
    Returns an em dash for None rather than the string "None" or a stray 0."""
    if value is None:
        return "—"
    if decimals is not None:
        return f"{value:.{decimals}f}{suffix}"
    return f"{value}{suffix}"


def altair_theme(chart):
    """Applies the dashboard's dark palette/fonts to an Altair chart -- the
    one piece of styling Streamlit's own chart theming can't reach, since
    st.line_chart/st.bar_chart don't expose axis/background colors directly."""
    return (
        chart.configure(background=PANEL, font=FONT_SANS)
        .configure_axis(
            labelColor=TEXT_MUTED,
            titleColor=TEXT_MUTED,
            gridColor="rgba(138, 150, 144, 0.12)",
            domainColor=BORDER,
            labelFont=FONT_MONO,
        )
        .configure_view(strokeWidth=0)
        .configure_legend(labelColor=TEXT_MUTED, titleColor=TEXT_MUTED, labelFont=FONT_SANS)
    )


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


def match_label(match_id: str, state: dict | None) -> str:
    """Builds a human-readable dropdown label ("India vs Australia, 142/3,
    sim") instead of showing the raw match_id, which is either a CricAPI
    UUID or a "sim-..." test id -- neither means anything to a viewer."""
    if not state or not state.get("batting_team"):
        return match_id
    label = f"{state['batting_team']} vs {state.get('bowling_team') or '?'}, {fmt(state.get('current_score'))}/{fmt(state.get('wickets_lost'))}"
    if match_id.startswith("sim-"):
        label += ", sim"
    return label


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

    # Fetch every tracked match's state up front -- needed to build a
    # readable dropdown label, and it means picking one doesn't need a
    # second round-trip to the API.
    states = {}
    for mid in match_ids:
        state, _ = api_get(f"/live/match/{mid}")
        if state:
            states[mid] = state

    match_id = st.sidebar.selectbox(
        "Live match", match_ids, format_func=lambda mid: match_label(mid, states.get(mid))
    )
    state = states.get(match_id)
    if state is None:
        st.error(f"No live state found for match_id '{match_id}'")
        return

    batting_team = state.get("batting_team") or "?"
    bowling_team = state.get("bowling_team") or "?"
    st.markdown(
        f'<div style="display:flex;align-items:center;gap:0.7rem;margin-bottom:0.25rem;">'
        f'<h2 style="margin:0;">{batting_team} vs {bowling_team}</h2>'
        f'<span class="pp-live-badge"><span class="pp-live-dot"></span>LIVE</span>'
        f"</div>",
        unsafe_allow_html=True,
    )

    # -- Hero: win probability is the reason this tab exists, so it gets the
    # most visual weight on the page rather than being one metric among four.
    win_prob = state.get("win_probability")
    with st.container(key="pp_hero"):
        st.markdown(
            f'<div class="pp-hero">'
            f'<div class="pp-hero-label">{batting_team} win probability</div>'
            f'<div class="pp-hero-value pp-mono">{f"{win_prob:.0%}" if win_prob is not None else "—"}</div>'
            f"</div>",
            unsafe_allow_html=True,
        )

    # -- Scoreboard strip: score/overs/run-rates read together as one
    # stadium-readout line rather than as separate bordered metric cards.
    segments = [
        ("SCORE", f"{fmt(state.get('current_score'))}/{fmt(state.get('wickets_lost'))}"),
        ("OVERS", f"{state['over_number']}.{state['ball_number']}"),
        ("CRR", fmt(state.get("current_run_rate"), 2)),
        ("RRR", fmt(state.get("required_run_rate"), 2)),
    ]
    if state.get("target"):
        segments.append(("TARGET", str(state["target"])))
    segments_html = "".join(
        f'<div class="pp-scoreboard-segment"><div class="pp-scoreboard-label">{label}</div>'
        f'<div class="pp-scoreboard-value pp-mono">{value}</div></div>'
        for label, value in segments
    )
    st.markdown(f'<div class="pp-scoreboard">{segments_html}</div>', unsafe_allow_html=True)

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

    with st.container(key="pp_chart"):
        if history:
            df = pd.DataFrame(history)
            chart = (
                alt.Chart(df)
                .mark_line(color=ACCENT_LIGHT, strokeWidth=2.5, point=alt.OverlayMarkDef(color=ACCENT_LIGHT, size=40))
                .encode(
                    x=alt.X("balls_into_innings:Q", title="Balls into the innings", axis=alt.Axis(format="d")),
                    y=alt.Y("win_probability:Q", title="Win probability", axis=alt.Axis(format="%"), scale=alt.Scale(domain=[0, 1])),
                    tooltip=[
                        alt.Tooltip("balls_into_innings:Q", title="Ball", format="d"),
                        alt.Tooltip("win_probability:Q", title="Win probability", format=".0%"),
                    ],
                )
                .properties(height=300)
            )
            st.altair_chart(altair_theme(chart), use_container_width=True)
        else:
            st.caption("Win probability chart will appear once the model has scored at least one ball.")

    st.divider()
    auto_refresh = st.checkbox("Auto-refresh", value=True)
    refresh_seconds = st.number_input("Refresh interval (seconds)", min_value=2, max_value=60, value=5)
    if st.button("Refresh now") or auto_refresh:
        if auto_refresh:
            time.sleep(refresh_seconds)
        st.rerun()


def render_scorecard(matches: list[dict]):
    """Renders every matched player as one compact scorecard-style row
    (name / runs / average / strike rate / wickets / economy) rather than a
    grid of bordered stat cards -- closer to how an actual scorecard reads,
    and it scales to any number of matches without needing a picker widget."""
    rows_html = []
    for stats in matches:
        runs = stats.get("runs_scored")
        dismissals = stats.get("dismissals")
        average = round(runs / dismissals, 2) if runs is not None and dismissals else None
        rows_html.append(
            "<tr>"
            f'<td class="pp-name">{stats["player"]}</td>'
            f'<td class="pp-num">{fmt(runs)}</td>'
            f'<td class="pp-num">{fmt(average, 2)}</td>'
            f'<td class="pp-num">{fmt(stats.get("batting_strike_rate"), 2)}</td>'
            f'<td class="pp-num pp-muted">{fmt(stats.get("wickets_taken"))}</td>'
            f'<td class="pp-num pp-muted">{fmt(stats.get("economy_rate"), 2)}</td>'
            "</tr>"
        )
    st.markdown(
        '<table class="pp-scorecard">'
        "<thead><tr><th>Player</th><th>Runs</th><th>Avg</th><th>SR</th><th>Wkts</th><th>Econ</th></tr></thead>"
        f"<tbody>{''.join(rows_html)}</tbody>"
        "</table>",
        unsafe_allow_html=True,
    )


def render_historical_tab():
    st.subheader("Player career stats")
    player_name = st.text_input("Player name", placeholder="e.g. V Kohli")
    if player_name:
        matches, error = api_get(f"/historical/player/{player_name}")
        if error:
            st.warning(error)
        else:
            if len(matches) > 1:
                st.caption(f"{len(matches)} players matched '{player_name}'")
            render_scorecard(matches)

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
            rows = sorted(rows, key=lambda r: r["matches_played"], reverse=True)
            body = "".join(
                "<tr>"
                f'<td>{r["venue"]}</td>'
                f'<td class="pp-num">{r["matches_played"]}</td>'
                f'<td class="pp-num">{r["matches_won"]}</td>'
                f'<td class="pp-num">{r["win_percentage"]:.2f}</td>'
                "</tr>"
                for r in rows
            )
            st.markdown(
                '<table class="pp-table">'
                "<thead><tr><th>Venue</th><th>Played</th><th>Won</th><th>Win %</th></tr></thead>"
                f"<tbody>{body}</tbody></table>",
                unsafe_allow_html=True,
            )

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
            seasons = sorted(df["season"].unique())
            # Seasons are ordered (years), so a sequential tint of the single
            # brand accent reads more honestly than inventing unrelated
            # categorical hues for what's really a magnitude-over-time axis.
            # Interpolated rather than cycled through a short list of stops --
            # with more than a handful of seasons a 3-color cycle would give
            # unrelated years the same color.
            colors = [interpolate_hex(ACCENT_DARK, ACCENT_LIGHT, i / max(len(seasons) - 1, 1)) for i in range(len(seasons))]
            chart = (
                alt.Chart(df)
                .mark_bar()
                .encode(
                    x=alt.X("phase:N", sort=PHASE_ORDER, title=None),
                    xOffset=alt.XOffset("season:N", sort=seasons),
                    y=alt.Y("average_run_rate:Q", title="Average run rate"),
                    color=alt.Color("season:N", sort=seasons, scale=alt.Scale(domain=seasons, range=colors), legend=alt.Legend(title=None) if len(seasons) > 1 else None),
                    tooltip=["team", "season", "phase", alt.Tooltip("average_run_rate:Q", format=".2f")],
                )
                .properties(height=300)
            )
            st.altair_chart(altair_theme(chart), use_container_width=True)


inject_css()

st.title("🏏 PitchPulse")

mode = st.sidebar.radio("Mode", ["Live match", "Historical explorer"])

if mode == "Live match":
    render_live_tab()
else:
    render_historical_tab()
