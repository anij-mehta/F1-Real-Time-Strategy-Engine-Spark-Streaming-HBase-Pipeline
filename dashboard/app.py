"""Live F1 strategy pit-wall dashboard."""

from __future__ import annotations

import math
import os
import sys
from html import escape
from pathlib import Path
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent))
from hbase_sink import get_history, get_latest_per_driver, get_pipeline_health  # noqa: E402


POLL_SECONDS = 2
HISTORY_LENGTH = 60
DEFAULT_RACE_LAPS = max(1, int(os.getenv("F1_RACE_TOTAL_LAPS", "57")))
UNKNOWN = "—"

st.set_page_config(
    page_title="F1 Pit Wall",
    page_icon="🏎️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .stApp { background: #0b0f14; color: #e6edf3; }
    [data-testid="stHeader"] { background: rgba(11, 15, 20, .95); }
    [data-testid="stSidebar"] { background: #111821; border-right: 1px solid #263241; }
    .block-container { padding-top: 1.25rem; }
    .pit-tile { padding: 1rem 1.1rem; border-radius: .7rem; min-height: 135px;
                margin: .25rem 0 .75rem; border: 1px solid #394452; background: #151c25; }
    .pit-tile h3 { margin: 0 0 .4rem; }
    .pit-tile p { margin: .35rem 0; color: #d2dae3; }
    .pit-tile.pit { border-color: #ff5263; background: linear-gradient(140deg, #421c25, #21151c); }
    .pit-tile.stay { border-color: #3ac98a; background: linear-gradient(140deg, #123b2d, #15231f); }
    .eyebrow { color: #93a4b7; text-transform: uppercase; letter-spacing: .08em;
               font-size: .75rem; }
    </style>
    """,
    unsafe_allow_html=True,
)


def numeric(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def display_number(value: Any, digits: int = 1, suffix: str = "") -> str:
    parsed = numeric(value)
    return UNKNOWN if parsed is None else f"{parsed:.{digits}f}{suffix}"


def render_leaderboard(latest: dict[str, dict[str, Any]]) -> None:
    st.subheader("Live leaderboard")
    if not latest:
        st.info("Waiting for stream — leaderboard data will appear when the first Gold records arrive.")
        return

    def position_key(record: dict[str, Any]) -> float:
        position = numeric(record.get("position"))
        return float("inf") if position is None else position

    records = sorted(latest.values(), key=position_key)
    table_data = [
        {
            "Pos": display_number(record.get("position"), 0),
            "Driver": record.get("driver", UNKNOWN),
            "Compound": record.get("compound", UNKNOWN),
            "Tyre life": display_number(record.get("tyre_life"), 1, " laps"),
            "Gap": display_number(record.get("avg_gap"), 3, "s"),
            "Lap": display_number(record.get("lap"), 0),
        }
        for record in records
    ]
    st.dataframe(pd.DataFrame(table_data), hide_index=True, use_container_width=True)


def render_decisions(latest: dict[str, dict[str, Any]]) -> None:
    st.subheader("Strategy calls")
    if not latest:
        st.caption("Strategy tiles are waiting for the first Gold trigger.")
        return
    columns_per_row = 4
    drivers = sorted(latest)
    for offset in range(0, len(drivers), columns_per_row):
        tile_columns = st.columns(columns_per_row)
        for tile, driver in zip(tile_columns, drivers[offset : offset + columns_per_row]):
            record = latest[driver]
            decision = record.get("decision", "STAY").upper()
            style = "pit" if decision == "PIT" else "stay"
            probability = record.get("pit_prob", "unavailable")
            probability_text = (
                "unavailable"
                if probability == "unavailable"
                else display_number(numeric(probability) * 100 if numeric(probability) is not None else None, 1, "%")
            )
            tile.markdown(
                f"""
                <div class="pit-tile {style}">
                  <span class="eyebrow">{escape(driver)} · {escape(str(record.get('timestamp', UNKNOWN)))}</span>
                  <h3>{escape(decision)}</h3>
                  <p>Pit probability: <strong>{probability_text}</strong></p>
                  <p>{escape(str(record.get('reason', 'No reason recorded')))}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )


def render_degradation(drivers: list[str]) -> None:
    st.subheader("Tyre degradation")
    if not drivers:
        st.caption("A driver degradation curve will appear once Gold history is available.")
        return
    selected = st.selectbox("Driver", drivers, key="degradation_driver")
    history = get_history(selected, HISTORY_LENGTH)
    points = [
        {
            "tyre_life": numeric(record.get("tyre_life")),
            "lap_time_delta": numeric(record.get("lap_time_delta")),
            "lap": numeric(record.get("lap")),
            "reason": record.get("reason", ""),
        }
        for record in history
    ]
    points = [
        point for point in points
        if point["tyre_life"] is not None and point["lap_time_delta"] is not None
    ]
    if not points:
        st.info("Waiting for tyre-life and lap-time-delta history for this driver.")
        return

    points.sort(key=lambda point: (point["tyre_life"], point["lap"] or 0))
    frame = pd.DataFrame(points)
    cliff_points = frame[frame["reason"].str.contains("cliff=true", case=False, na=False)]
    chart = go.Figure()
    chart.add_trace(
        go.Scatter(
            x=frame["tyre_life"],
            y=frame["lap_time_delta"],
            mode="lines+markers",
            name="Lap-time delta",
            customdata=frame[["lap"]],
            hovertemplate="Tyre life %{x:.1f} laps<br>Lap-time delta %{y:.3f}s<br>Lap %{customdata[0]:.0f}<extra></extra>",
            line={"color": "#43b9ff", "width": 2},
            marker={"color": "#43b9ff", "size": 7},
        )
    )
    if not cliff_points.empty:
        chart.add_trace(
            go.Scatter(
                x=cliff_points["tyre_life"],
                y=cliff_points["lap_time_delta"],
                mode="markers",
                name="Detected cliff",
                marker={"color": "#ff5263", "size": 14, "symbol": "diamond"},
                hovertemplate="Detected cliff<br>Tyre life %{x:.1f} laps<br>Lap-time delta %{y:.3f}s<extra></extra>",
            )
        )
    chart.update_layout(
        template="plotly_dark",
        paper_bgcolor="#0b0f14",
        plot_bgcolor="#111821",
        margin={"l": 10, "r": 10, "t": 25, "b": 10},
        xaxis_title="Tyre life (laps)",
        yaxis_title="Lap-time delta (s)",
        legend={"orientation": "h", "y": 1.12},
        height=390,
    )
    st.plotly_chart(chart, use_container_width=True)
    if cliff_points.empty:
        st.caption("No tyre cliff is currently flagged for this driver's stored history.")


def render_health(health: dict[str, str] | None) -> None:
    st.subheader("Pipeline health")
    if not health:
        st.info("Waiting for stream health metrics.")
        latency = rows = lag = updated = UNKNOWN
    else:
        latency = display_number(health.get("batch_latency_ms"), 1, " ms")
        rows = health.get("rows_processed", UNKNOWN)
        lag = display_number(health.get("stream_lag_seconds"), 2, " s")
        updated = health.get("updated_at", UNKNOWN)
    metrics = st.columns(4)
    metrics[0].metric("Last batch latency", latency)
    metrics[1].metric("Rows processed", rows)
    metrics[2].metric("Stream lag", lag)
    metrics[3].metric("Health updated (UTC)", updated)



def render_race_progress(
    latest: dict[str, dict[str, Any]],
    total_laps: int,
) -> None:
    st.subheader("Race progress")
    current_lap = max(
        (numeric(record.get("lap")) or 0 for record in latest.values()),
        default=0,
    )
    progress = min(1.0, current_lap / total_laps)
    st.progress(
        progress,
        text=f"Lap {int(current_lap)} / {total_laps} · {progress:.0%}",
    )



total_laps = st.sidebar.number_input(
    "Race distance (laps)",
    min_value=1,
    max_value=100,
    value=DEFAULT_RACE_LAPS,
    step=1,
    key="race_distance_laps",
)


@st.fragment(run_every=f"{POLL_SECONDS}s")
def live_dashboard(total_laps: int) -> None:
    st.title("🏎️ F1 Strategy · Pit Wall")
    st.caption(f"Live HBase telemetry · refresh every {POLL_SECONDS} seconds")
    try:
        latest = get_latest_per_driver()
        health = get_pipeline_health()
    except Exception as exc:
        st.error(f"HBase is temporarily unavailable: {exc}")
        st.info("Waiting for stream — the dashboard will retry automatically.")
        return

    render_health(health)
    st.divider()
    render_race_progress(latest, total_laps)
    render_leaderboard(latest)
    render_decisions(latest)
    st.divider()
    try:
        render_degradation(sorted(latest))
    except Exception as exc:
        st.error(f"Could not load driver history from HBase: {exc}")


live_dashboard(int(total_laps))
