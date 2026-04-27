# Day 3 – Bronze Layer (Raw Ingestion)

Day 3 focused on implementing the Bronze layer — the raw ingestion layer of the pipeline.

## Objective

The Bronze layer ingests streaming files from:

data/raw_events/

and writes them to:

bronze/events/

Bronze acts as the immutable source of truth.

## Bronze Layer Responsibilities

- Read streaming JSON files
- Preserve raw data
- Add ingestion metadata
- Write in append-only mode

No business transformations occur here.

## Implementation

A Spark Structured Streaming job (bronze_ingest.py) was created to:

- Use readStream on the raw events directory
- Apply schema
- Write to Parquet format
- Maintain checkpointing

Example:

.writeStream \
  .format("parquet") \
  .option("checkpointLocation", "checkpoints/bronze") \
  .start("bronze/events")

## Why Bronze Must Be Immutable

Bronze must:
- Never delete data
- Never update records
- Never apply filters

Ensures:
- Safe replay
- Auditable history
- Reliable downstream rebuilding

## Metadata Added

Bronze adds:
- _ingest_ts
- _source_file

Used for:
- Latency tracking
- Observability
- Debugging

## Outcome of Day 3

- Streaming ingestion working
- JSON converted to Parquet
- Checkpointing enabled
- Bronze confirmed append-only
