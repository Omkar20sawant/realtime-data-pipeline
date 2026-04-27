"""
Real-time Data Pipeline Monitoring Dashboard
─────────────────────────────────────────────
Changes from previous version:
  1. Sidebar date-range picker (replaces "last N days" selector).
     Dates shown in PST; converted to UTC for Parquet queries so
     partition pruning still works.
  2. All displayed timestamps converted to PST (America/Los_Angeles).
  3. New "Run History" tab — detects distinct pipeline sessions per day
     and shows start/end (PST), duration, events processed, and health.
"""

import logging

import duckdb
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from dashboard.config_loader import load_config
from dashboard.queries import (
    latest_kpi_query,
    trend_query,
    recent_rows_query,
    run_sessions_query,
)
from dashboard.utils import (
    to_pst,
    fmt_pst,
    get_freshness_status,
    get_latency_status,
)

# ── Config ────────────────────────────────────────────────────────────────────
config = load_config()
METRICS_PATH      = config["metrics_path"]
REFRESH_INTERVAL  = config["refresh_interval_ms"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("pipeline_dashboard")
logger.info("Dashboard starting")
logger.info(f"METRICS_PATH={METRICS_PATH}")

PST_TZ = "America/Los_Angeles"

# ── Page setup ────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Pipeline Monitoring Dashboard", layout="wide")
st_autorefresh(interval=REFRESH_INTERVAL, key="pipeline_dashboard_refresh")

st.title("Real-time Data Pipeline Monitoring Dashboard")
st.caption("Gold metrics queried with DuckDB and displayed in Streamlit — times shown in PST")

# ── Sidebar: date-range picker (PST) ─────────────────────────────────────────
st.sidebar.header("Filters")

# Default: today in PST
today_pst = pd.Timestamp.now(tz=PST_TZ).date()
default_start = today_pst - pd.Timedelta(days=1)

date_range = st.sidebar.date_input(
    "Select date range (PST)",
    value=(default_start, today_pst),
    min_value=today_pst - pd.Timedelta(days=90),
    max_value=today_pst,
    help="Pick a start and end date. Times are in PST (America/Los_Angeles).",
)

# Guard: user may click only one date while picking
if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    pst_start_date, pst_end_date = date_range
else:
    pst_start_date = pst_end_date = date_range if not isinstance(date_range, (list, tuple)) else date_range[0]

# Convert PST midnight → UTC for Parquet queries
#   PST start of day  →  UTC  (e.g. 2026-04-24 00:00 PST = 2026-04-24 08:00 UTC)
#   PST end of day +1 →  UTC  (exclusive upper bound)
pst_start_ts  = pd.Timestamp(pst_start_date).tz_localize(PST_TZ)
pst_end_ts    = pd.Timestamp(pst_end_date).tz_localize(PST_TZ) + pd.Timedelta(days=1)

utc_start_str = pst_start_ts.tz_convert("UTC").strftime("%Y-%m-%d %H:%M:%S")
utc_end_str   = pst_end_ts.tz_convert("UTC").strftime("%Y-%m-%d %H:%M:%S")

st.caption(
    f"Showing data from **{pst_start_date}** to **{pst_end_date}** (PST)"
)

# ── Query helper ──────────────────────────────────────────────────────────────
def run_query(query_name: str, query: str) -> pd.DataFrame:
    logger.info(f"Executing query={query_name}")
    con = duckdb.connect()
    try:
        df = con.execute(query).df()
        logger.info(f"Query succeeded | query={query_name} rows={len(df)}")
        return df
    except Exception:
        logger.exception(f"Query failed | query={query_name}")
        raise
    finally:
        con.close()


# ── Load data ─────────────────────────────────────────────────────────────────
try:
    df_latest   = run_query("latest_kpi",    latest_kpi_query(METRICS_PATH))
    df_trend    = run_query("trend",         trend_query(METRICS_PATH, utc_start_str, utc_end_str))
    df_recent   = run_query("recent_rows",   recent_rows_query(METRICS_PATH, utc_start_str, utc_end_str))
    df_sessions = run_query("run_sessions",  run_sessions_query(METRICS_PATH, utc_start_str, utc_end_str))
except Exception as e:
    st.error(f"Failed to load dashboard datasets: {e}")
    st.stop()

if df_latest.empty:
    st.warning("No metrics data found yet. Make sure the Gold pipeline has produced Parquet output.")
    st.stop()

# ── Timestamp cleanup + PST conversion ───────────────────────────────────────
TS_COLS_LATEST  = ["window_start", "window_end", "latest_event_ts_seen", "computed_at"]
TS_COLS_TREND   = ["window_start", "latest_event_ts_seen", "computed_at"]
TS_COLS_RECENT  = ["window_start", "window_end", "latest_event_ts_seen", "computed_at"]
TS_COLS_SESSION = ["session_start_utc", "session_end_utc"]

for col_name in TS_COLS_LATEST:
    if col_name in df_latest.columns:
        df_latest[col_name] = pd.to_datetime(df_latest[col_name], errors="coerce")

for col_name in TS_COLS_TREND:
    if col_name in df_trend.columns:
        df_trend[col_name] = pd.to_datetime(df_trend[col_name], errors="coerce")

for col_name in TS_COLS_RECENT:
    if col_name in df_recent.columns:
        df_recent[col_name] = pd.to_datetime(df_recent[col_name], errors="coerce")

for col_name in TS_COLS_SESSION:
    if col_name in df_sessions.columns:
        df_sessions[col_name] = pd.to_datetime(df_sessions[col_name], errors="coerce")

# ── KPI values ────────────────────────────────────────────────────────────────
latest_row = df_latest.iloc[0]

freshness_gap_sec = None
if pd.notnull(latest_row["latest_event_ts_seen"]):
    latest_ts = latest_row["latest_event_ts_seen"]
    if getattr(latest_ts, "tzinfo", None) is None:
        latest_ts = latest_ts.tz_localize("UTC")
    freshness_gap_sec = max(
        0,
        int((pd.Timestamp.now(tz="UTC") - latest_ts).total_seconds()),
    )

events_per_min  = int(latest_row["events_valid_count"])   if pd.notnull(latest_row["events_valid_count"])   else 0
avg_delay       = float(latest_row["avg_processing_delay_sec"]) if pd.notnull(latest_row["avg_processing_delay_sec"]) else 0.0
p95_delay       = float(latest_row["p95_processing_delay_sec"]) if pd.notnull(latest_row["p95_processing_delay_sec"]) else 0.0

freshness_status, freshness_delta_color = get_freshness_status(freshness_gap_sec)
latency_status,  latency_delta_color    = get_latency_status(p95_delay)

# ── Status bar ────────────────────────────────────────────────────────────────
scol1, scol2 = st.columns(2)
scol1.markdown(f"**Freshness Status:** {freshness_status}")
scol2.markdown(f"**Latency Status:** {latency_status}")

# ── Top KPI strip ─────────────────────────────────────────────────────────────
st.subheader("Latest KPI Metrics")
k1, k2, k3, k4 = st.columns(4)
k1.metric("Events / min",      f"{events_per_min:,}")
k2.metric("Avg Delay (sec)",   f"{avg_delay:.2f}")
k3.metric("P95 Delay (sec)",   f"{p95_delay:.2f}",
          delta=latency_status,  delta_color=latency_delta_color)
k4.metric("Freshness Gap (sec)", str(freshness_gap_sec) if freshness_gap_sec is not None else "—",
          delta=freshness_status, delta_color=freshness_delta_color)

# ── Trend data prep ───────────────────────────────────────────────────────────
df_trend = df_trend.sort_values("window_start")

# Convert window_start to PST for chart axis labels
if not df_trend.empty:
    df_trend["window_start_pst"] = to_pst(df_trend["window_start"])
    events_chart_df  = df_trend.set_index("window_start_pst")[["events_valid_count"]]
    latency_chart_df = df_trend.set_index("window_start_pst")[
        ["avg_processing_delay_sec", "p95_processing_delay_sec"]
    ]
else:
    events_chart_df  = pd.DataFrame()
    latency_chart_df = pd.DataFrame()

# ── Build PST-display version of recent rows ──────────────────────────────────
df_recent_display = df_recent.copy()
for col_name in TS_COLS_RECENT:
    if col_name in df_recent_display.columns:
        df_recent_display[col_name] = fmt_pst(df_recent_display[col_name])

# ── Build PST-display version of sessions ────────────────────────────────────
df_sessions_display = df_sessions.copy()
if not df_sessions_display.empty:
    df_sessions_display["session_start_pst"] = fmt_pst(df_sessions_display["session_start_utc"])
    df_sessions_display["session_end_pst"]   = fmt_pst(df_sessions_display["session_end_utc"])
    # date column for grouping label
    df_sessions_display["date_pst"] = to_pst(df_sessions_display["session_start_utc"]).dt.strftime("%Y-%m-%d")

    sessions_table = df_sessions_display[[
        "date_pst",
        "session_start_pst",
        "session_end_pst",
        "duration_minutes",
        "total_events",
        "avg_delay_sec",
        "p95_delay_sec",
        "health",
    ]].rename(columns={
        "date_pst":          "Date (PST)",
        "session_start_pst": "Run Start (PST)",
        "session_end_pst":   "Run End (PST)",
        "duration_minutes":  "Duration (min)",
        "total_events":      "Total Events",
        "avg_delay_sec":     "Avg Delay (sec)",
        "p95_delay_sec":     "P95 Delay (sec)",
        "health":            "Health",
    })

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_overview, tab_trends, tab_raw, tab_runs = st.tabs(
    ["Overview", "Trends", "Raw Data", "Run History"]
)

# ── Tab 1: Overview ───────────────────────────────────────────────────────────
with tab_overview:
    st.subheader("Pipeline Overview")
    c1, c2 = st.columns(2)
    with c1:
        st.metric("Events / min",        f"{events_per_min:,}")
        st.metric("Freshness Gap (sec)", str(freshness_gap_sec) if freshness_gap_sec is not None else "—",
                  delta=freshness_status, delta_color=freshness_delta_color)
    with c2:
        st.metric("Avg Delay (sec)",  f"{avg_delay:.2f}")
        st.metric("P95 Delay (sec)", f"{p95_delay:.2f}",
                  delta=latency_status, delta_color=latency_delta_color)

# ── Tab 2: Trends ─────────────────────────────────────────────────────────────
with tab_trends:
    st.subheader("Events per Minute Trend (PST)")
    if events_chart_df.empty:
        st.info("No trend data for the selected date range.")
    else:
        st.line_chart(events_chart_df)

    st.subheader("Processing Delay Trend (PST)")
    if latency_chart_df.empty:
        st.info("No latency data for the selected date range.")
    else:
        st.line_chart(latency_chart_df)

# ── Tab 3: Raw Data ───────────────────────────────────────────────────────────
with tab_raw:
    st.subheader("Recent Metrics Records (times in PST)")
    if df_recent_display.empty:
        st.info("No records for the selected date range.")
    else:
        st.dataframe(df_recent_display, use_container_width=True)

# ── Tab 4: Run History ────────────────────────────────────────────────────────
with tab_runs:
    st.subheader("Pipeline Run History (PST)")
    st.caption(
        "Each row is one continuous pipeline run. A new run is detected when "
        "there is a gap of more than 5 minutes between consecutive 1-minute windows."
    )

    if df_sessions_display.empty:
        st.info("No run sessions found for the selected date range.")
    else:
        st.dataframe(sessions_table, use_container_width=True)

        # Summary: total run time per day
        st.subheader("Total Pipeline Time per Day (PST)")
        daily = (
            sessions_table
            .groupby("Date (PST)")["Duration (min)"]
            .sum()
            .reset_index()
            .rename(columns={"Duration (min)": "Total Run Time (min)"})
            .sort_values("Date (PST)", ascending=False)
        )
        daily["Total Run Time (hrs)"] = (daily["Total Run Time (min)"] / 60).round(2)
        st.dataframe(daily, use_container_width=True)

# ── Footer ────────────────────────────────────────────────────────────────────
st.divider()
st.markdown(
    """
**How to read this dashboard**
- **Events / min** — throughput for the latest 1-minute processing window.
- **Avg Delay / P95 Delay** — processing latency (seconds from event time to ingest time).
- **Freshness Gap** — how many seconds behind the latest seen event is relative to now.
- **Run History** — each row is a distinct pipeline session; multiple rows on the same date
  mean the pipeline was started, stopped, and restarted that day.
- All times displayed in **PST (America/Los_Angeles)**.
"""
)
