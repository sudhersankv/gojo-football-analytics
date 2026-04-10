"""
Gojo Live — Redis Match Viewer (Streamlit)

Reads live/completed fixture data from Upstash Redis and renders a
match dashboard similar to the main React UI.

Usage:
  1. Fill in UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN in temp/.env
  2. pip install -r temp/requirements.txt
  3. streamlit run temp/app.py
"""

import json
import os
from pathlib import Path

import plotly.graph_objects as go
import requests
import streamlit as st
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

REDIS_URL = os.getenv("UPSTASH_REDIS_REST_URL", "")
REDIS_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN", "")
KEY_PREFIX = "gojo:live:fixture"

# ── Page config ──────────────────────────────────────────────────────────────

st.set_page_config(page_title="Gojo Live", page_icon="⚽", layout="wide")

PURPLE = "#3d195b"
CYAN = "#00ff87"
MAGENTA = "#e90052"

# ── Redis helpers ────────────────────────────────────────────────────────────

def redis_cmd(*args):
    resp = requests.post(
        REDIS_URL,
        headers={"Authorization": f"Bearer {REDIS_TOKEN}", "Content-Type": "application/json"},
        json=list(args),
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json().get("result")


def redis_get(key: str):
    raw = redis_cmd("GET", key)
    if raw is None:
        return None
    return json.loads(raw) if isinstance(raw, str) else raw


def redis_zrange(key: str):
    raw = redis_cmd("ZRANGE", key, "0", "-1")
    return raw or []


def redis_scan_fixtures():
    """Return fixture IDs that have a :latest key in Redis."""
    cursor, ids = "0", set()
    while True:
        result = redis_cmd("SCAN", cursor, "MATCH", f"{KEY_PREFIX}:*:latest", "COUNT", "100")
        cursor = result[0]
        for k in result[1]:
            parts = k.split(":")
            if len(parts) >= 4:
                ids.add(parts[3])
        if cursor == "0":
            break
    return sorted(ids)

# ── Data loaders ─────────────────────────────────────────────────────────────

def load_latest(fid: str):
    return redis_get(f"{KEY_PREFIX}:{fid}:latest")


def load_minutes(fid: str):
    return redis_zrange(f"{KEY_PREFIX}:{fid}:minutes")


def load_detail(fid: str, minute: str):
    return redis_get(f"{KEY_PREFIX}:{fid}:detail:m:{minute}")


def load_players(fid: str, minute: str):
    return redis_get(f"{KEY_PREFIX}:{fid}:players:m:{minute}")


def load_light_bucket(fid: str, minute: str):
    return redis_get(f"{KEY_PREFIX}:{fid}:m:{minute}")

# ── UI components ────────────────────────────────────────────────────────────

STATUS_LABELS = {
    "FT": "Full Time", "AET": "After Extra Time", "PEN": "Penalties",
    "1H": "1st Half", "2H": "2nd Half", "HT": "Half Time", "NS": "Not Started",
    "LIVE": "Live",
}


def render_score_header(data):
    teams = data.get("teams", {})
    score = data.get("score", {})
    status = data.get("status_short", "")
    elapsed = data.get("elapsed")

    home_name = teams.get("home", {}).get("name", "Home")
    away_name = teams.get("away", {}).get("name", "Away")
    home_goals = score.get("home", "-")
    away_goals = score.get("away", "-")
    ht = score.get("ht", {})

    st.markdown(
        f"""
        <div style="background: {PURPLE}; border-radius: 16px; padding: 32px 16px; margin-bottom: 24px; text-align: center;">
            <div style="display: flex; align-items: center; justify-content: center; gap: 48px;">
                <div style="color: rgba(255,255,255,0.9); font-size: 18px; font-weight: 600;">{home_name}</div>
                <div>
                    <div style="font-size: 48px; font-weight: 900; color: #fff; letter-spacing: -2px;">
                        {home_goals if home_goals is not None else '-'} — {away_goals if away_goals is not None else '-'}
                    </div>
                    <div style="color: rgba(255,255,255,0.7); font-size: 12px; font-weight: 600; text-transform: uppercase; letter-spacing: 1px;">
                        {STATUS_LABELS.get(status, status)}{f' · {elapsed}\'' if elapsed else ''}
                    </div>
                    {f'<div style="color: rgba(255,255,255,0.45); font-size: 11px; margin-top: 4px;">HT: {ht.get("home", "-")} — {ht.get("away", "-")}</div>' if ht.get("home") is not None else ''}
                </div>
                <div style="color: rgba(255,255,255,0.9); font-size: 18px; font-weight: 600;">{away_name}</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_events(detail):
    events = detail.get("events", [])
    if not events:
        st.info("No events recorded.")
        return

    teams = detail.get("teams", {})
    home_id = teams.get("home", {}).get("id")

    ICONS = {"Goal": "⚽", "Card": "🟨", "subst": "🔄", "Var": "📺"}

    for ev in events:
        minute = ev.get("time", {}).get("elapsed", "?")
        extra = ev.get("time", {}).get("extra")
        minute_str = f"{minute}+{extra}'" if extra else f"{minute}'"
        etype = ev.get("type", "")
        detail_text = ev.get("detail", "")
        icon = ICONS.get(etype, "•")
        if etype == "Card" and detail_text and "red" in detail_text.lower():
            icon = "🟥"

        player = ev.get("player", {}).get("name", "")
        assist = ev.get("assist", {}).get("name", "")
        team_id = ev.get("team", {}).get("id")
        is_home = team_id == home_id
        align = "left" if is_home else "right"

        cols = st.columns([5, 1, 5])
        if is_home:
            with cols[0]:
                st.markdown(
                    f"**{minute_str}** {icon} **{player}**"
                    + (f"  \n<small style='color:#888;'>{detail_text}" + (f" — {assist}" if assist else "") + "</small>" if detail_text or assist else ""),
                    unsafe_allow_html=True,
                )
        else:
            with cols[2]:
                st.markdown(
                    f"**{player}** {icon} **{minute_str}**"
                    + (f"  \n<small style='color:#888;'>" + (f"{assist} — " if assist else "") + f"{detail_text}</small>" if detail_text or assist else ""),
                    unsafe_allow_html=True,
                )


STAT_ORDER = [
    "Ball Possession", "expected_goals", "Total Shots", "Shots on Goal",
    "Shots off Goal", "Blocked Shots", "Corner Kicks", "Offsides",
    "Fouls", "Yellow Cards", "Red Cards", "Goalkeeper Saves",
    "Total passes", "Passes accurate", "Passes %",
]

STAT_LABELS = {
    "Ball Possession": "Possession", "expected_goals": "xG",
    "Total Shots": "Total Shots", "Shots on Goal": "On Target",
    "Shots off Goal": "Off Target", "Blocked Shots": "Blocked",
    "Corner Kicks": "Corners", "Goalkeeper Saves": "Saves",
    "Total passes": "Passes", "Passes accurate": "Acc. Passes",
    "Passes %": "Pass Accuracy",
}


def parse_num(val):
    if val is None:
        return 0.0
    s = str(val).replace("%", "")
    try:
        return float(s)
    except ValueError:
        return 0.0


def render_team_stats(detail):
    statistics = detail.get("statistics", [])
    if not statistics:
        st.info("No team statistics available.")
        return

    teams = detail.get("teams", {})
    home_name = teams.get("home", {}).get("name", "Home")
    away_name = teams.get("away", {}).get("name", "Away")
    home_id = teams.get("home", {}).get("id")

    by_side = {}
    for block in statistics:
        tid = block.get("team", {}).get("id")
        side = "home" if tid == home_id else "away"
        for s in block.get("statistics", []):
            by_side.setdefault(side, {})[s.get("type", "")] = s.get("value")

    home_stats = by_side.get("home", {})
    away_stats = by_side.get("away", {})
    available = [s for s in STAT_ORDER if s in home_stats or s in away_stats]

    for stat_key in available:
        h_val = home_stats.get(stat_key)
        a_val = away_stats.get(stat_key)
        h_num = parse_num(h_val)
        a_num = parse_num(a_val)
        total = h_num + a_num or 1
        h_pct = h_num / total * 100

        label = STAT_LABELS.get(stat_key, stat_key)
        h_display = h_val if h_val is not None else "0"
        a_display = a_val if a_val is not None else "0"
        h_bold = "font-weight:700;" if h_num >= a_num else ""
        a_bold = "font-weight:700;" if a_num >= h_num else ""

        st.markdown(
            f"""
            <div style="margin-bottom: 12px;">
                <div style="display: flex; justify-content: space-between; margin-bottom: 4px; font-size: 14px;">
                    <span style="{h_bold}">{h_display}</span>
                    <span style="color: #888; font-size: 12px; text-transform: uppercase; letter-spacing: 0.5px;">{label}</span>
                    <span style="{a_bold}">{a_display}</span>
                </div>
                <div style="display: flex; gap: 2px; height: 8px; border-radius: 8px; overflow: hidden; background: #f3f4f6;">
                    <div style="width: {h_pct}%; background: {PURPLE}; border-radius: 8px 0 0 8px;"></div>
                    <div style="width: {100 - h_pct}%; background: {MAGENTA}; border-radius: 0 8px 8px 0;"></div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_comparison_chart(detail):
    statistics = detail.get("statistics", [])
    if not statistics:
        return

    teams = detail.get("teams", {})
    home_name = teams.get("home", {}).get("name", "Home")
    away_name = teams.get("away", {}).get("name", "Away")
    home_id = teams.get("home", {}).get("id")

    by_side = {}
    for block in statistics:
        tid = block.get("team", {}).get("id")
        side = "home" if tid == home_id else "away"
        for s in block.get("statistics", []):
            by_side.setdefault(side, {})[s.get("type", "")] = s.get("value")

    chart_stats = ["Ball Possession", "Total Shots", "Shots on Goal", "Corner Kicks",
                   "Total passes", "Passes accurate", "Fouls"]
    home_s = by_side.get("home", {})
    away_s = by_side.get("away", {})
    available = [s for s in chart_stats if s in home_s or s in away_s]
    labels = [STAT_LABELS.get(s, s) for s in available]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name=home_name,
        x=labels,
        y=[parse_num(home_s.get(s)) for s in available],
        marker_color=PURPLE,
    ))
    fig.add_trace(go.Bar(
        name=away_name,
        x=labels,
        y=[parse_num(away_s.get(s)) for s in available],
        marker_color=MAGENTA,
    ))
    fig.update_layout(
        barmode="group", height=350, margin=dict(l=0, r=0, t=30, b=0),
        legend=dict(orientation="h", y=1.12),
        plot_bgcolor="white",
        xaxis=dict(tickfont=dict(size=10)),
    )
    st.plotly_chart(fig, use_container_width=True)


def render_player_table(players_data, detail):
    if not players_data:
        st.info("No player stats available.")
        return

    teams_list = players_data.get("teams", [])
    if not teams_list:
        st.info("No player stats available.")
        return

    detail_teams = detail.get("teams", {})
    home_id = detail_teams.get("home", {}).get("id")
    home_name = detail_teams.get("home", {}).get("name", "Home")
    away_name = detail_teams.get("away", {}).get("name", "Away")

    team_tabs = st.tabs([home_name, away_name])

    for tab_idx, tab in enumerate(team_tabs):
        with tab:
            target_side = "home" if tab_idx == 0 else "away"

            for grp in teams_list:
                tid = grp.get("team", {}).get("id")
                is_home = tid == home_id
                if (target_side == "home" and not is_home) or (target_side == "away" and is_home):
                    continue

                players = grp.get("players", [])
                if not players:
                    st.write("No players.")
                    continue

                rows = []
                for p in players:
                    player_info = p.get("player", {})
                    stats_list = p.get("statistics", [])
                    s = stats_list[0] if stats_list else {}

                    games = s.get("games", {})
                    goals_obj = s.get("goals", {})
                    shots = s.get("shots", {})
                    passes = s.get("passes", {})
                    tackles = s.get("tackles", {})
                    duels = s.get("duels", {})
                    dribbles = s.get("dribbles", {})
                    fouls_obj = s.get("fouls", {})
                    cards = s.get("cards", {})

                    rating_raw = games.get("rating")
                    rating = f"{float(rating_raw):.1f}" if rating_raw else "-"

                    rows.append({
                        "Player": player_info.get("name", "?"),
                        "Pos": games.get("position", "-"),
                        "Min": games.get("minutes", "-"),
                        "Rtg": rating,
                        "G": goals_obj.get("total") or 0,
                        "A": goals_obj.get("assists") or 0,
                        "Sh": shots.get("total") or 0,
                        "SoT": shots.get("on") or 0,
                        "Pas": passes.get("total") or 0,
                        "KP": passes.get("key") or 0,
                        "Acc%": passes.get("accuracy") or "-",
                        "Tkl": tackles.get("total") or 0,
                        "Int": tackles.get("interceptions") or 0,
                        "DW": duels.get("won") or 0,
                        "Drb": dribbles.get("success") or 0,
                        "Fls": fouls_obj.get("committed") or 0,
                        "YC": cards.get("yellow") or 0,
                        "RC": cards.get("red") or 0,
                    })

                if rows:
                    st.dataframe(
                        rows,
                        use_container_width=True,
                        hide_index=True,
                        height=min(len(rows) * 38 + 40, 700),
                    )


def render_player_rating_chart(players_data, detail):
    """Bar chart of player ratings for both teams."""
    if not players_data:
        return

    teams_list = players_data.get("teams", [])
    detail_teams = detail.get("teams", {})
    home_id = detail_teams.get("home", {}).get("id")
    home_name = detail_teams.get("home", {}).get("name", "Home")
    away_name = detail_teams.get("away", {}).get("name", "Away")

    fig = go.Figure()

    for grp in teams_list:
        tid = grp.get("team", {}).get("id")
        is_home = tid == home_id
        team_name = home_name if is_home else away_name
        color = PURPLE if is_home else MAGENTA

        names, ratings = [], []
        for p in grp.get("players", []):
            player_info = p.get("player", {})
            stats_list = p.get("statistics", [])
            s = stats_list[0] if stats_list else {}
            r = s.get("games", {}).get("rating")
            if r:
                names.append(player_info.get("name", "?"))
                ratings.append(float(r))

        if names:
            sorted_pairs = sorted(zip(names, ratings), key=lambda x: x[1], reverse=True)
            names, ratings = zip(*sorted_pairs)
            fig.add_trace(go.Bar(
                name=team_name, x=list(ratings), y=list(names),
                orientation="h", marker_color=color,
            ))

    fig.update_layout(
        title="Player Ratings", barmode="group", height=600,
        margin=dict(l=0, r=0, t=40, b=0),
        yaxis=dict(autorange="reversed", tickfont=dict(size=10)),
        xaxis=dict(range=[5, 10], title="Rating"),
        legend=dict(orientation="h", y=1.05),
        plot_bgcolor="white",
    )
    st.plotly_chart(fig, use_container_width=True)

# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    if not REDIS_URL or not REDIS_TOKEN:
        st.error("Set **UPSTASH_REDIS_REST_URL** and **UPSTASH_REDIS_REST_TOKEN** in `temp/.env`")
        st.stop()

    st.markdown(
        f"""
        <div style="text-align:center; padding: 8px 0 16px;">
            <h1 style="color: {PURPLE}; font-size: 28px; margin: 0;">⚽ Gojo Live</h1>
            <p style="color: #888; font-size: 13px; margin-top: 4px;">Redis Match Viewer</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    fixture_ids = redis_scan_fixtures()

    if not fixture_ids:
        st.warning("No fixtures found in Redis. Run `job_b_live` with a fixture ID first.")
        st.stop()

    selected = st.selectbox(
        "Select fixture",
        fixture_ids,
        format_func=lambda fid: f"Fixture {fid}",
    )

    latest = load_latest(selected)
    if not latest:
        st.error(f"No 'latest' key for fixture {selected}")
        st.stop()

    render_score_header(latest)

    minutes = load_minutes(selected)

    # Find latest heavy bucket (detail + players)
    detail = None
    players_data = None
    for m in reversed(minutes):
        d = load_detail(selected, m)
        if d:
            detail = d
            players_data = load_players(selected, m)
            break

    tab_overview, tab_stats, tab_players, tab_raw = st.tabs(
        ["Overview", "Stats", "Players", "Raw Data"]
    )

    with tab_overview:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Match Events")
            if detail:
                render_events(detail)
            else:
                st.info("No detail bucket available (heavy writes happen every 5 elapsed minutes).")

        with col2:
            st.subheader("Team Statistics")
            if detail:
                render_team_stats(detail)
            elif latest.get("stats"):
                st.markdown("*From light snapshot:*")
                for side in ["home", "away"]:
                    side_stats = latest["stats"].get(side, {})
                    if side_stats:
                        team_name = latest.get("teams", {}).get(side, {}).get("name", side.title())
                        st.markdown(f"**{team_name}**")
                        for k, v in side_stats.items():
                            st.text(f"  {k}: {v}")
            else:
                st.info("No stats available.")

    with tab_stats:
        st.subheader("Team Comparison")
        if detail:
            render_comparison_chart(detail)
            st.divider()
            st.subheader("Player Ratings")
            if players_data:
                render_player_rating_chart(players_data, detail)
            else:
                st.info("No player data in this bucket.")
        else:
            st.info("No detail bucket available.")

    with tab_players:
        st.subheader("Player Match Statistics")
        if players_data and detail:
            render_player_table(players_data, detail)
        else:
            st.info("No player stats bucket available.")

    with tab_raw:
        st.subheader("Available minute buckets")
        st.write(f"Minutes in Redis: {minutes}")

        with st.expander("Latest snapshot (light)"):
            st.json(latest)

        if detail:
            with st.expander("Detail bucket (heavy)"):
                st.json(detail)

        if players_data:
            with st.expander("Players bucket (heavy)"):
                st.json(players_data)


if __name__ == "__main__":
    main()
