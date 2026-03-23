# Day 4 – Silver Layer (Cleansing & Deduplication)

Day 4 focused on building the Silver layer — the cleaned and validated transformation layer.

## Objective

The Silver layer reads from:

bronze/events/

and writes to:

silver/events/

It performs data quality enforcement and transformation.

## Silver Layer Responsibilities

- Schema enforcement
- Data validation
- Null filtering
- Deduplication
- Event-time handling
- Derived column creation

## Data Validation Rules

Examples:
- event_id must not be null
- price > 0
- qty > 0
- Valid channel values only

Invalid records are written to:

silver/bad_records/

## Deduplication Strategy

A composite key was created:

store_id + sku + channel + event_ts + price + qty

Hashed using sha2.

Ensures:
- Duplicate events removed
- Replay does not amplify duplicates
- Idempotent output

## Watermarking

Example:

.withWatermark("event_ts", "10 minutes")

Purpose:
- Handle late events
- Limit state store size
- Support correct window aggregations

## Derived Columns

Silver adds:
- event_date
- ingest_date
- event_hour
- processing_delay_sec
- delay_bucket

Prepares data for Gold analytics.

## Outcome of Day 4

- Cleaned dataset created
- Deduplication implemented
- Watermarking applied
- Derived metrics added
- Silver confirmed replay-safe
