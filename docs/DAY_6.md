# Day 6 – Backfill & Replay Engineering

Day 6 focused on validating replayability, checkpoint behavior, and deterministic processing.

## Objective

Simulate a production failure by deleting Silver and Gold layers and rebuilding them from Bronze to verify deterministic replay.

## Cold Replay Procedure

1) Stop streaming jobs (Silver + Gold)

Ctrl + C in each terminal

2) Delete derived layer outputs + checkpoints

rm -rf silver/events
rm -rf checkpoints/silver

rm -rf gold/delay_buckets_per_minute
rm -rf checkpoints/gold_delay_buckets

3) Restart in order

python spark_jobs/silver_stream.py
python spark_jobs/gold_events_per_minute.py

## Replay Guarantee

Replay is deterministic because:

- Bronze is immutable
- Silver transformations are idempotent
- Gold aggregations are deterministic
- Checkpoints ensure safe progress tracking
- _spark_metadata ensures safe file commits

## Common Failure Mode

Deleting only the output path or only the checkpoint leads to errors such as:

BATCH_METADATA_NOT_FOUND

Fix:
Delete both output and checkpoint together, then restart.

## Outcome of Day 6

- Replay validated
- Crash recovery tested
- Metadata behavior understood
- Exactly-once semantics demonstrated
