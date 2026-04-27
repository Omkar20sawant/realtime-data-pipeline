# Day 5 – Gold Layer (Aggregations & Business Metrics)

Day 5 implemented the Gold layer — the analytics and aggregation layer.

## Objective

Gold produces business-ready datasets derived from Silver.

Outputs include:
- events_per_minute
- delay_buckets_per_minute
- latency_percentiles_per_minute

## Events Per Minute

Window aggregation:

groupBy(window("event_ts", "1 minute"))

Purpose:
- Monitor traffic volume
- Detect spikes or drops

## Delay Buckets Per Minute

Groups events by delay category:
- <5s
- 5–30s
- 30–120s
- >=120s

Purpose:
- Monitor data freshness
- Track processing lag
- Validate SLA performance

## Latency Percentiles

Computes:
- p50
- p90
- p95
- p99

Purpose:
- Measure performance distribution
- Detect outliers
- Monitor reliability

## Output Mode & Watermarks

Gold uses:
- Event-time windows
- Watermark (e.g., 2 minutes)
- Append mode

Append mode ensures:
- Only finalized windows emitted
- No duplicate window outputs

## Checkpointing

Each Gold dataset has its own checkpoint:

checkpoints/gold_*

Ensures:
- Safe restart
- No duplicate writes
- Exactly-once file sink behavior

## Outcome of Day 5

- Business metrics computed
- Window aggregations implemented
- Latency monitoring enabled
- Multiple Gold datasets created
- Streaming aggregation validated
