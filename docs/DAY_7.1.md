# DAY 7 — Performance & Production Hardening (Part 1)

## Objective
Transform the streaming pipeline from a functional system into a production-grade, scalable architecture by introducing:
- Config-driven design
- Partitioned data layout (Silver + Gold)
- Improved file organization for analytics
- Streaming performance tuning
- Compatibility fixes for partitioned datasets

---

## What Was Built

### 1) Configuration Standardization
All paths, checkpoints, and streaming parameters were centralized in config.py.

Key benefits:
- No hardcoded paths
- Easy environment switching
- Cleaner reusable code

---

### 2) Silver Layer Hardening (v2)
- Validation rules
- Deduplication using watermark
- Enrichment fields (event_date, ingest_date, delay metrics)

Partitioning added:
.partitionBy("event_date")

Output structure:
silver/events_v2/event_date=YYYY-MM-DD/

---

### 3) Gold Layer Hardening (v2)
Aggregations:
- Events per minute
- Delay buckets
- Latency percentiles

Windowing:
window(event_ts, "1 minute")

Watermark:
2 minutes

---

### 4) Gold Partitioning
Added:
window_date = to_date(window_start)

Partitioning:
.partitionBy("window_date")

Output:
gold_v2/.../window_date=YYYY-MM-DD/

---

### 5) Schema Inference Fix
Used recursive glob to handle partitioned Silver:
glob("**/*.parquet", recursive=True)

---

### 6) Performance Tuning
- shuffle partitions
- maxFilesPerTrigger
- trigger intervals

---

## Pipeline Flow
Generator → Bronze → Silver v2 → Gold v2

---

## How to Run

1. Generator
python data_generator/generate_events.py

2. Bronze
PYTHONPATH=spark_jobs spark-submit spark_jobs/bronze_ingest.py

3. Silver
PYTHONPATH=spark_jobs spark-submit spark_jobs/silver_stream.py

4. Gold
PYTHONPATH=spark_jobs spark-submit spark_jobs/gold_events_per_minute.py

---

## Validation

Bronze:
SELECT COUNT(*) FROM read_parquet('bronze/events/**/*.parquet');

Silver:
SELECT event_date, COUNT(*) FROM read_parquet('silver/events_v2/**/*.parquet') GROUP BY event_date;

Gold:
SELECT COUNT(*) FROM read_parquet('gold_v2/events_per_minute/**/*.parquet');

---

## Key Learnings
- Structured Streaming
- Watermarks & deduplication
- Partitioned data lakes
- Config-driven pipelines
- Exactly-once semantics

---

## Next Steps (Part 2)
- Monitoring metrics
- Structured logging
- Config validation
- Runbook
