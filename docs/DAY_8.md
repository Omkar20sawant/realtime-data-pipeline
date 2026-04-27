# DAY 8 – Analytics Layer & Pipeline Metrics

## Overview

Day 8 focuses on extending the streaming data pipeline beyond data processing into **analytics, monitoring, and observability**.

By this stage, the pipeline already produces Bronze, Silver, and Gold datasets. Day 8 introduces a **metrics layer and analytics interface** that enables:

- Monitoring pipeline health
- Measuring latency and throughput
- Running analytical queries on Gold data
- Preparing data for dashboards and BI tools

This transforms the project from a basic ETL pipeline into a **mini analytics platform**.

---

## Key Additions in Day 8

### 1. Pipeline Metrics Table (Gold Layer)

A new Gold dataset was created:

gold_v2/pipeline_metrics_per_minute

This table captures operational metrics computed using **event-time windowing (1-minute windows)**.

### Metrics Captured

- events_valid_count  
- avg_processing_delay_sec  
- p95_processing_delay_sec  
- latest_event_ts_seen  
- computed_at  

---

## Why This Matters

This metrics table introduces a **lightweight observability layer**:

- Enables tracking of pipeline throughput
- Helps identify latency issues
- Provides insight into data freshness
- Supports SLA/SLO-style monitoring

---

## Validation Performed

- Window boundaries progress correctly in 1-minute intervals
- events_valid_count is always non-negative
- p95 >= avg delay in most cases
- latest_event_ts_seen falls within window
- computed_at reflects processing time

---

## Analytics Layer with DuckDB

DuckDB was introduced as a lightweight OLAP engine to query Gold data directly from Parquet files.

### Example Usage

import duckdb

con = duckdb.connect()

df = con.execute("""
SELECT *
FROM read_parquet('gold_v2/pipeline_metrics_per_minute/**/*.parquet')
ORDER BY window_start DESC
LIMIT 10
""").df()

print(df)

---

## Saved SQL Queries

- pipeline_health_last_60m.sql  
- delay_trend.sql  
- events_volume_last_60m.sql  

---

## Query Runner

analytics/run_query.py

Usage:

python analytics/run_query.py analytics/sql/pipeline_health_last_60m.sql

---

## Architecture

Event Generator
      ↓
Bronze
      ↓
Silver
      ↓
Gold (metrics)
      ↓
DuckDB

---

## Interview Talking Points

- Built real-time metrics layer
- Used event-time windowing
- Enabled observability without external tools
- Queried Parquet directly using DuckDB

---

## Next Step (Day 9)

- Build Streamlit dashboard
- Add live metrics visualization
- Optional Tableau dashboard

---

## Summary

Day 8 adds analytics and observability, making the pipeline closer to a real production system.
