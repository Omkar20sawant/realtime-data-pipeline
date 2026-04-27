import duckdb
import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh

### PAGE CONFIF ###

st.set_page_config(
    page_title = "Pipeline Monitoring Dashboard",
    layout = "wide")

st_autorefresh(interval=5 * 1000, key="pipeline_dashboard_refresh")
st.title("Real-time Data Pipeline Monitoring Dashboard")
st.caption("Gold metrics queried with DuckDB and displayed in Streamlit")

st.sidebar.header("Filters")

days = st.sidebar.selectbox(
    "Select Time Window (days)",
    [1, 3, 7, 14],
    index=0
)
st.caption(f"Showing data for the last {days} day(s)")

### DATA SOURCE ###
METRICS_PATH = "gold_v2/pipeline_metrics_per_minute/**/*.parquet"

def run_query(query:str)->pd.DataFrame:
    """Execute a DUCKDB SQL and return a Pandas DataFrame."""
    con = duckdb.connect()
    try:
        df = con.execute(query).df()
        return df
    finally:
        con.close()


### LOAD DATA ###
latest_kpi_query = f"""
SELECT 
Window_start,
Window_end,
events_valid_count,
avg_processing_delay_sec,
p95_processing_delay_sec,
latest_event_ts_seen,
computed_at
FROM read_parquet('{METRICS_PATH}')
ORDER BY window_start DESC
LIMIT 1
"""

trend_query = f"""
SELECT 
Window_start,
events_valid_count,
avg_processing_delay_sec,
p95_processing_delay_sec,
latest_event_ts_seen,
computed_at
FROM read_parquet('{METRICS_PATH}')
WHERE window_start >= NOW() - INTERVAL '{days} days'
ORDER BY window_start ASC
"""

recent_rows_query = f"""
SELECT
    window_start,
    window_end,
    events_valid_count,
    avg_processing_delay_sec,
    p95_processing_delay_sec,
    latest_event_ts_seen,
    computed_at
FROM read_parquet('{METRICS_PATH}')
WHERE window_start >= NOW() - INTERVAL '{days} days'
ORDER BY window_start DESC
LIMIT 100
"""

try:
    df_latest = run_query(latest_kpi_query)
    df_trend = run_query(trend_query)
    df_recent = run_query(recent_rows_query)
except Exception as e:
    st.error(f"Failed to load dashboard dataset: {e}")
    st.stop()


### EMPTY DATA HANDLING ###
if df_latest.empty:
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

def get_freshness_status(freshness_gap_sec: int | None) -> str:
    if freshness_gap_sec is None:
        return "⚪ Unknown"
    if freshness_gap_sec < 10:
        return "🟢 Healthy"
    elif freshness_gap_sec < 30:
        return "🟡 Warning"
    else:
        return "🔴 Critical"


def get_latency_status(p95_delay: float | None) -> str:
    if p95_delay is None:
        return "⚪ Unknown"
    if p95_delay < 500:
        return "🟢 Healthy"
    elif p95_delay < 1500:
        return "🟡 Warning"
    else:
        return "🔴 Critical"
    
def get_freshness_status(freshness_gap_sec):
    if freshness_gap_sec is None:
        return "⚪ Unknown", "unknown"
    if freshness_gap_sec < 10:
        return "🟢 Healthy", "normal"
    elif freshness_gap_sec < 30:
        return "🟡 Warning", "inverse"
    else:
        return "🔴 Critical", "off"


def get_latency_status(p95_delay):
    if p95_delay is None:
        return "⚪ Unknown", "unknown"
    if p95_delay < 300:
        return "🟢 Healthy", "normal"
    elif p95_delay < 900:
        return "🟡 Warning", "inverse"
    else:
        return "🔴 Critical", "off"


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