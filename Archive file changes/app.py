import duckdb
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import yaml
import os
import logging
from dashboard.config_loader import load_config
from dashboard.queries import latest_kpi_query, trend_query, recent_rows_query
from dashboard.utils import get_freshness_status, get_latency_status

config = load_config()

METRICS_PATH = config["metrics_path"]
DEFAULT_WINDOW_DAYS = config["default_window_days"]
REFRESH_INTERVAL_MS = config["refresh_interval_ms"]

logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("pipeline_dashboard")

logger.info("Dashboard starting")
logger.info(f"METRICS_PATH={METRICS_PATH}")
logger.info(f"DEFAULT_WINDOW_DAYS={DEFAULT_WINDOW_DAYS}")
logger.info(f"REFRESH_INTERVAL_MS={REFRESH_INTERVAL_MS}")

### PAGE CONFIG ###

st.set_page_config(
    page_title = "Pipeline Monitoring Dashboard",
    layout = "wide")

st_autorefresh(interval= REFRESH_INTERVAL_MS, key="pipeline_dashboard_refresh")
st.title("Real-time Data Pipeline Monitoring Dashboard")
st.caption("Gold metrics queried with DuckDB and displayed in Streamlit")

st.sidebar.header("Filters")

day_option = [1, 3, 7, 14]
days = st.sidebar.selectbox(
    "Select Time Window (days)",
    day_option,
    index=day_option.index(DEFAULT_WINDOW_DAYS) if DEFAULT_WINDOW_DAYS in day_option else 0
)
st.caption(f"Showing data for the last {days} day(s)")

### DATA SOURCE ###
# METRICS_PATH = "gold_v2/pipeline_metrics_per_minute/**/*.parquet"

def run_query(query_name: str, query: str) -> pd.DataFrame:
    """Execute a DuckDB SQL query and return a Pandas DataFrame."""
    logger.info(f"Executing query={query_name}")
    con = duckdb.connect()
    try:
        df = con.execute(query).df()
        logger.info(f"Query succeeded | query={query_name} rows_returned={len(df)}")
        return df
    except Exception:
        logger.exception(f"Query failed | query={query_name}")
        raise
    finally:
        con.close()

try:
    logger.info("Loading dashboard datasets")
    df_latest = run_query("latest_kpi", latest_kpi_query(METRICS_PATH))
    df_trend = run_query("trend", trend_query(METRICS_PATH, days))
    df_recent = run_query("recent_rows", recent_rows_query(METRICS_PATH, days))
    logger.info(
        f"Datasets loaded successfully | latest={len(df_latest)} trend={len(df_trend)} recent={len(df_recent)}"
    )
except Exception as e:
    logger.exception("Failed to load dashboard datasets")
    st.error(f"Failed to load dashboard dataset: {e}")
    st.stop()


### EMPTY DATA HANDLING ###
if df_latest.empty:
    logger.warning("No metrics data found in Gold layer output")
    st.warning("No metrics data found yet. Make sure the Gold pipeline has produced parquet output.")
    st.stop()

### TYPE CLEANUP ###
timestamp_cols_latest = ["window_start", "window_end", "latest_event_ts_seen", "computed_at"]
for col in timestamp_cols_latest:
    if col in df_latest.columns:
        df_latest[col] = pd.to_datetime(df_latest[col], errors='coerce')

timestamp_cols_trend = ["window_start", "latest_event_ts_seen", "computed_at"]
for col in timestamp_cols_trend:
    if col in df_trend.columns:
        df_trend[col] = pd.to_datetime(df_trend[col], errors='coerce')      

timestamp_cols_recent = ["window_start", "window_end", "latest_event_ts_seen", "computed_at"]
for col in timestamp_cols_recent:
    if col in df_recent.columns:
        df_recent[col] = pd.to_datetime(df_recent[col], errors="coerce")


### PREPARE LATEST KPI ROW ###
latest_row = df_latest.iloc[0]

freshness_gap_sec = None
if pd.notnull(latest_row["latest_event_ts_seen"]):
    freshness_gap_sec = max(
        0,
        int((pd.Timestamp.now() - latest_row["latest_event_ts_seen"]).total_seconds())
    )
events_per_min = int(latest_row["events_valid_count"]) if pd.notnull(latest_row["events_valid_count"]) else 0
avg_delay = float(latest_row["avg_processing_delay_sec"]) if pd.notnull(latest_row["avg_processing_delay_sec"]) else 0.0
p95_delay = float(latest_row["p95_processing_delay_sec"]) if pd.notnull(latest_row["p95_processing_delay_sec"]) else 0.0
freshness_status = get_freshness_status(freshness_gap_sec)
latency_status = get_latency_status(p95_delay)

status_col1, status_col2 = st.columns(2)
status_col1.markdown(f"**Freshness Status:** {freshness_status}")
status_col2.markdown(f"**Latency Status:** {latency_status}")

freshness_status, freshness_delta_color = get_freshness_status(freshness_gap_sec)
latency_status, latency_delta_color = get_latency_status(p95_delay)

### KPI SECTION ###

st.subheader("Latest KPI Metrics")

col1, col2, col3, col4 = st.columns(4)

col1.metric("Events/min", f"{events_per_min:,}")
col2.metric("Avg Delay (sec)", f"{avg_delay:.2f}")
col3.metric("P95 Delay (sec)", f"{p95_delay:.2f}", delta=latency_status, delta_color=latency_delta_color)
col4.metric("Freshness Gap (sec)", f"{freshness_gap_sec}", delta=freshness_status, delta_color=freshness_delta_color)



### TREND DATA PREPARATION ###
df_trend = df_trend.sort_values("window_start")

events_chart_df = df_trend[["window_start", "events_valid_count"]].copy()
events_chart_df = events_chart_df.set_index("window_start")

latency_chart_df = df_trend[["window_start", "avg_processing_delay_sec", "p95_processing_delay_sec"]].copy()
latency_chart_df = latency_chart_df.set_index("window_start")

tab1, tab2, tab3 = st.tabs(["Overview", "Trends", "Raw Data"])

with tab1:
    st.subheader("Pipeline Overview")

    col1, col2 = st.columns(2)

    with col1:
        st.metric("Events/min", f"{events_per_min:,}")
        st.metric("Freshness Gap (sec)", f"{freshness_gap_sec}", delta=freshness_status, delta_color=freshness_delta_color)

    with col2:
        st.metric("Avg Delay (sec)", f"{avg_delay:.2f}")
        st.metric("P95 Delay (sec)", f"{p95_delay:.2f}", delta=latency_status, delta_color=latency_delta_color)

with tab2:
    st.subheader("Events per Minute Trend")
    st.line_chart(events_chart_df)

    st.subheader("Processing Delay Trend")
    st.line_chart(latency_chart_df)

with tab3:
    st.subheader("Recent Metrics Records")
    st.dataframe(df_recent, use_container_width=True)

st.divider()

### FOOTER / CONTEXT ###
st.markdown(
    """
**How to read this dashboard**
- **Events / min** shows throughput for the latest 1-minute window.
- **Avg Delay / P95 Delay** shows processing latency.
- **Freshness Gap** shows how far behind the latest event is from current time.
- The table below helps inspect the actual latest metric records.
"""
)