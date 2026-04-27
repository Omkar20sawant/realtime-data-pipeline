# Real-Time Data Pipeline - End-to-End Startup Guide

This guide explains how to start the full real-time pipeline locally, including Kafka ingestion, Bronze -> Silver -> Gold processing, DuckDB validation, and the Streamlit dashboard.

---

## Architecture Overview

```text
Zookeeper / Kafka
   ↓
Producer
   ↓
Bronze (Kafka -> Parquet)
   ↓
Silver (Clean + Dedup)
   ↓
Gold (Aggregations / Metrics)
   ↓
DuckDB Queries
   ↓
Streamlit Dashboard
```

---

## Prerequisites

- Docker installed and running if you want to use Kafka through Docker
- Kafka installed locally if you want to start Kafka from command line instead
- Conda environment:

```bash
conda activate realtime-pipeline
```

- Project root:

```bash
cd /Users/omkarsawant/realtime-data-pipeline
```

---

## Step-by-Step Startup Order

### 0. Start Zookeeper and Kafka

You can start Kafka either through Docker or through local command line.

### Option A - Start with Docker

Check whether the containers are already running:

```bash
docker ps
```

If not running, start them with Docker Compose:

```bash
docker-compose up -d
```

If your setup uses the newer syntax, this also works:

```bash
docker compose up -d
```

To confirm Kafka and Zookeeper are up:

```bash
docker ps
```

You should see containers for both:
- Zookeeper
- Kafka

### Option B - Start with local command line

If Kafka is installed locally, start Zookeeper first:

```bash
zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties
```

Then in a new terminal, start Kafka broker:

```bash
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```

If your Kafka config files are in a different location, use your local paths instead.

---

### 1. Start Bronze (Kafka -> Parquet)

```bash
cd /Users/omkarsawant/realtime-data-pipeline
conda activate realtime-pipeline
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
  spark_jobs/bronze_ingest.py
```

This reads from Kafka and writes raw events to:

```text
bronze/events/
```

---

### 2. Start Silver (Cleaned Stream)

```bash
cd /Users/omkarsawant/realtime-data-pipeline
conda activate realtime-pipeline
spark-submit spark_jobs/silver_stream.py
```

This performs:
- deduplication
- schema enforcement
- filtering

Output:

```text
silver/events/
```

---

### 3. Start Gold (Aggregations)

```bash
cd /Users/omkarsawant/realtime-data-pipeline
conda activate realtime-pipeline
spark-submit spark_jobs/gold_events_per_minute.py
```

This produces Gold-layer metrics and aggregations in:

```text
gold_v2/
```

---

### 4. Verify the streams are alive

Before starting the producer, confirm:
- Bronze is running without errors
- Silver is processing
- Gold is active

This ensures the pipeline is ready to consume events as soon as they are produced.

---

### 5. Start Kafka Producer (Event Generator)

```bash
cd /Users/omkarsawant/realtime-data-pipeline
conda activate realtime-pipeline
python producer/kafka_event_producer.py
```

This starts sending real-time events into Kafka.

Important: start the producer after Bronze, Silver, and Gold are already running.

Recommended order:
1. Zookeeper / Kafka
2. Bronze
3. Silver
4. Gold
5. Producer
6. DuckDB checks
7. Streamlit

---

### 6. Validate with DuckDB

Use DuckDB to confirm that data is flowing through each layer.

#### Bronze checks

Count rows:

```sql
SELECT count(*)
FROM read_parquet('bronze/events/**/*.parquet');
```

Preview records:

```sql
SELECT *
FROM read_parquet('bronze/events/**/*.parquet')
LIMIT 10;
```

#### Silver checks

Count rows:

```sql
SELECT count(*)
FROM read_parquet('silver/events_v2/**/*.parquet');
```

Preview records:

```sql
SELECT *
FROM read_parquet('silver/events_v2/**/*.parquet')
LIMIT 10;
```

#### Gold checks

Count rows for delay buckets:

```sql
SELECT count(*)
FROM read_parquet('gold_v2/delay_buckets_per_minute/**/*.parquet');
```

Latest Gold records:

```sql
SELECT *
FROM read_parquet('gold_v2/delay_buckets_per_minute/**/*.parquet')
ORDER BY window_start DESC
LIMIT 20;
```

Latest Gold window:

```sql
SELECT MAX(window_start) AS latest_window
FROM read_parquet('gold_v2/delay_buckets_per_minute/**/*.parquet');
```

Latest full snapshot for one window:

```sql
SELECT *
FROM read_parquet('gold_v2/delay_buckets_per_minute/**/*.parquet')
WHERE window_start = (
    SELECT MAX(window_start)
    FROM read_parquet('gold_v2/delay_buckets_per_minute/**/*.parquet')
)
ORDER BY delay_bucket;
```

#### Useful generic Gold check

```sql
SELECT count(*)
FROM read_parquet('gold_v2/**/*.parquet');
```

---

### 7. Start Streamlit Dashboard

```bash
cd /Users/omkarsawant/realtime-data-pipeline
conda activate realtime-pipeline
streamlit run app.py
```

Open in browser:

```text
http://localhost:8501
```

---

## Verification Flow

When the system is healthy:

- Producer terminal shows events being sent
- Bronze terminal shows Kafka ingestion / micro-batch activity
- Silver terminal shows records being processed
- Gold terminal shows aggregations being written
- DuckDB counts increase over time
- Streamlit dashboard refreshes with live values

---

## Full Terminal Layout

### Terminal 1 - Zookeeper (only if using local command line)

```bash
zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties
```

### Terminal 2 - Kafka broker (only if using local command line)

```bash
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```

### Terminal 3 - Bronze

```bash
cd /Users/omkarsawant/realtime-data-pipeline
conda activate realtime-pipeline
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
  spark_jobs/bronze_ingest.py
```

### Terminal 4 - Silver

```bash
cd /Users/omkarsawant/realtime-data-pipeline
conda activate realtime-pipeline
spark-submit spark_jobs/silver_stream.py
```

### Terminal 5 - Gold

```bash
cd /Users/omkarsawant/realtime-data-pipeline
conda activate realtime-pipeline
spark-submit spark_jobs/gold_events_per_minute.py
```

### Terminal 6 - Producer

```bash
cd /Users/omkarsawant/realtime-data-pipeline
conda activate realtime-pipeline
python producer/kafka_event_producer.py
```

### Terminal 7 - Streamlit

```bash
cd /Users/omkarsawant/realtime-data-pipeline
conda activate realtime-pipeline
streamlit run app.py
```

---

## Common Issues and Fixes

### Error: Failed to find data source: kafka

Run Bronze with the Kafka connector package:

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
  spark_jobs/bronze_ingest.py
```

### Bronze files are not increasing

Check:
- producer is started
- Bronze job is still running
- Kafka topic matches between Bronze and producer
- Kafka bootstrap server is correct, usually `localhost:9092`

### DuckDB query fails

Make sure file paths are quoted:

```sql
SELECT *
FROM read_parquet('bronze/events/**/*.parquet');
```

### Streamlit app path error

Use the correct command from project root:

```bash
streamlit run app.py
```

### Streamlit port already in use

```bash
streamlit run app.py --server.port 8502
```

---

## Shutdown Order

Stop everything in reverse order:

1. Streamlit
2. Producer
3. Gold
4. Silver
5. Bronze
6. Kafka broker
7. Zookeeper

If Kafka is running through Docker, stop containers only if you want to fully shut down infrastructure.

---

## Final State

When everything is running, you have:

- real-time event ingestion through Kafka
- Bronze raw event persistence
- Silver cleaned and deduplicated stream
- Gold aggregations and metrics
- DuckDB-based analytical validation
- Streamlit dashboard for live monitoring

This is a complete local real-time data platform demo.
