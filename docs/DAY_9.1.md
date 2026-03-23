# Day 9.1 — Analytics + Dashboard Layer (Streamlit v1)

## Overview

Day 9.1 focused on adding an **analytics and observability layer** on top of the existing Bronze → Silver → Gold pipeline.

Up to this point, the project was already producing processed and aggregated data. In Day 9.1, the goal was to make that data **queryable, monitorable, and visually inspectable**.

This phase introduced:

- DuckDB-based analytics over Gold Parquet files
- reusable SQL queries for operational metrics
- a Streamlit dashboard for live-ish monitoring of the pipeline
- KPI, trend, and table-based observability views

This makes the project feel much closer to a real production-style data platform rather than only a transformation pipeline.

---

## What was already completed before the dashboard

Before building the dashboard, the following analytics foundations were already completed:

### 1. Metrics table validation
The Gold metrics table was validated to confirm that:

- `window_start` and `window_end` progress correctly in 1-minute windows
- `events_valid_count` is non-negative
- delay metrics are present and sensible
- recent rows represent expected streaming behavior

This step ensured that the downstream dashboard would be built on reliable metrics.

### 2. DuckDB SQL queries
Reusable SQL files were created to query the Gold metrics Parquet output. These queries made it possible to inspect pipeline behavior without loading the data into an external warehouse.

Typical query goals included:

- pipeline health in the last 60 minutes
- delay trend analysis
- recent event volume trend

This demonstrated a lightweight analytics pattern:
**DuckDB directly querying Parquet produced by Spark**.

### 3. `run_query.py` query runner
A Python query runner was created to execute saved SQL files through DuckDB.

Example usage:

```bash
python analytics/run_query.py analytics/sql/pipeline_health_last_60m.sql
python analytics/run_query.py analytics/sql/delay_trend.sql
python analytics/run_query.py analytics/sql/events_volume_last_60m.sql
```

This proved that the SQL artifacts were reusable, testable, and independent of the dashboard layer.

---

## Main Day 9.1 deliverable: Streamlit dashboard v1

The main goal of Day 9.1 was to create the first working version of a Streamlit dashboard.

### File created

```bash
dashboard/app.py
```

### Technologies used

- **Streamlit** for dashboard rendering
- **DuckDB** for SQL queries over Parquet
- **Pandas** for DataFrame handling and timestamp cleanup

### Purpose of the dashboard

The dashboard provides a lightweight monitoring interface over the Gold metrics layer.

Instead of checking pipeline state only from terminal outputs or raw SQL results, the dashboard gives a more operational view of the system by surfacing:

- current throughput
- current latency
- data freshness
- recent metric history

---

## Dashboard architecture

The monitoring flow now looks like this:

```text
Event Generator
      │
      ▼
Bronze (raw events)
      │
      ▼
Silver (cleaned + deduplicated events)
      │
      ▼
Gold (per-minute metrics and aggregations)
      │
      ▼
DuckDB analytics on Parquet
      │
      ▼
Streamlit dashboard
```

This establishes a proper separation of responsibilities:

- **Spark** computes the metrics
- **DuckDB** queries the metrics
- **Streamlit** displays the metrics

---

## Dashboard v1 sections

The first version of the dashboard includes four main sections.

### 1. Latest KPI Metrics

The dashboard reads the latest row from the metrics dataset and displays key operational signals.

KPIs shown:

- **Events / min**
- **Avg Delay (sec)**
- **P95 Delay (sec)**
- **Freshness Gap (sec)**

#### Why these metrics matter

**Events / min**
- Represents throughput for the latest one-minute processing window
- Helps identify whether the system is processing a healthy amount of data

**Avg Delay**
- Shows average processing delay across events in the latest window
- Gives a general latency signal

**P95 Delay**
- Shows tail latency
- More useful than average alone because it highlights slower outlier behavior

**Freshness Gap**
- Measures how far behind the latest observed event is relative to current time
- Indicates whether the pipeline is current or becoming stale

These four metrics together cover the most important dimensions of pipeline observability:

- throughput
- latency
- freshness

---

### 2. Events per Minute Trend

A line chart was added to display:

- `window_start`
- `events_valid_count`

This chart shows how throughput changes over time and helps identify:

- stable processing
- sudden drops
- spikes
- missing windows
- producer slowdowns or stoppages

This makes the pipeline behavior much easier to interpret than looking at one KPI alone.

---

### 3. Latency Trend

A second line chart was added to display:

- `avg_processing_delay_sec`
- `p95_processing_delay_sec`

This chart helps monitor processing efficiency over time and reveals:

- gradual latency increases
- short-term spikes
- p95 vs average divergence
- general health of the pipeline under recent load

This is one of the most useful operational charts in the dashboard because it reflects real processing quality.

---

### 4. Recent Metrics Rows

A raw table was added to display the latest metric rows directly.

This is useful for:

- debugging
- validating chart behavior
- inspecting exact timestamps
- checking recent metric values behind the visuals

This section makes the dashboard more trustworthy because it exposes the actual underlying rows and not just summaries.

---

## Implementation details

The Streamlit app was built with the following logic.

### 1. Query Gold Parquet directly
The app uses DuckDB `read_parquet(...)` to query metrics files under the Gold path.

This avoids the need for:

- a separate analytical database
- a warehouse setup
- a serving API layer

This keeps the analytics stack lightweight and appropriate for a portfolio project.

### 2. Use helper query functions
A reusable query function executes SQL and returns Pandas DataFrames.

This keeps the application modular and easier to maintain.

### 3. Normalize timestamps
Timestamp columns are converted to Pandas datetime objects so they can be:

- sorted reliably
- subtracted safely
- charted correctly

### 4. Compute freshness gap
The dashboard computes freshness as:

```text
current_time - latest_event_ts_seen
```

This adds a practical real-time health signal on top of the stored metrics.

### 5. Render UI using Streamlit components
The UI uses:

- `st.metric(...)` for KPI cards
- `st.line_chart(...)` for trends
- `st.dataframe(...)` for raw table inspection

This gives a compact but useful operational dashboard with very little frontend complexity.

---

## Problems encountered and fixed

During implementation, several real-world issues were encountered and resolved.

### 1. Environment mismatch
At first, `duckdb` was missing because the script was run from the wrong Python environment (`base` instead of the project environment).

This reinforced the importance of:

- activating the correct project environment
- isolating dependencies per project

### 2. Freshness gap calculation bug
There was an error caused by incorrect parentheses when converting the freshness timedelta into seconds.

This was fixed by computing:

```python
(pd.Timestamp.now() - latest_row["latest_event_ts_seen"]).total_seconds()
```

before converting to `int`.

### 3. Schema / DataFrame column mismatch
A chart error occurred because expected latency columns did not initially line up with the DataFrame view used in the dashboard.

This highlighted an important real-world lesson:
**downstream analytics depend heavily on schema consistency**.

Fixing this required validating the actual columns present in the queried dataset.

---

## Why Day 9.1 matters

This phase is important because it transforms the project from a processing-only system into a system that can be **observed and explained**.

It demonstrates that the project is not just generating outputs, but also supporting:

- monitoring
- inspection
- analytics consumption
- operational visibility

That is a major step toward production-style thinking.

Most beginner projects stop at:
- ingest
- transform
- save

This project now additionally shows:
- queryability
- observability
- dashboarding

---

## Interview value

Day 9.1 adds strong talking points for interviews.

Example explanation:

> I built an observability layer on top of my Gold metrics table using DuckDB and Streamlit. Spark produced per-minute operational metrics, DuckDB queried the Parquet outputs directly, and Streamlit displayed throughput, latency, and freshness in a live-ish monitoring dashboard.

Key concepts demonstrated:

- medallion architecture consumption
- direct analytics on Parquet
- KPI design for data pipeline monitoring
- operational observability
- lightweight dashboard engineering in Python

This is a strong portfolio signal because it shows that the pipeline is not only functionally correct, but also measurable and monitorable.

---

## What is complete at the end of Day 9.1

By the end of Day 9.1, the project includes:

- validated Gold metrics
- reusable DuckDB SQL files
- a Python SQL query runner
- a working Streamlit dashboard
- KPI-based operational monitoring
- trend charts for throughput and latency
- raw metrics table inspection

This makes the project significantly more complete and more compelling to present.

---

## Next planned enhancements (Day 9.2)

The current dashboard is a solid first version. The next enhancement phase will improve usability and interactivity.

Planned dashboard upgrades:

- auto-refresh
- sidebar filters
- last N minutes selector
- status colors
- anomaly notes
- separate tabs

These will make the dashboard feel more polished and more production-like.

---

## Upcoming roadmap

### Day 9.2 — Dashboard Enhancements
Planned improvements:

- add auto-refresh for live-ish updates
- add sidebar controls
- add a configurable recent time window selector
- add status indicators / colors
- add anomaly notes or warnings
- organize the dashboard into separate tabs

### Day 10 — Production Polish
Planned work:

- config file support
- structured logging
- environment variables

This will make the pipeline more configurable and easier to operate across environments.

### Day 11 — Kafka Mode (Optional but Impressive)
Planned work:

- introduce Kafka-based ingestion mode
- simulate or connect to a real streaming source
- show the same downstream Bronze → Silver → Gold flow with Kafka as the upstream transport

This would make the project even stronger from a streaming systems perspective.

---

## Summary

Day 9.1 completed the first full analytics and observability layer for the project.

The pipeline now supports:

- data ingestion
- transformation
- aggregation
- SQL analytics
- dashboard monitoring

This is a significant milestone because the project now resembles a real end-to-end analytics system rather than only a set of Spark jobs.
