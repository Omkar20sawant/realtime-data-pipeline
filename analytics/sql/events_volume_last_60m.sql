SELECT
    window_start,
    events_valid_count
FROM read_parquet('gold_v2/pipeline_metrics_per_minute/**/*.parquet')
ORDER BY window_start DESC
LIMIT 60;