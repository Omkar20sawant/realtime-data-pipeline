SELECT 
    window_start, 
    window_end, 
    events_valid_count, 
    Round(Avg_processing_delay_sec,2) as avg_processing_delay_sec,
    Round(p95_processing_delay_sec,2) as p95_processing_delay_sec,
    latest_event_ts_seen, computed_at, 
    DATE_DIFF('second', latest_event_ts_seen, computed_at) AS freshness_gap_sec 
    -- , Round(Avg_processing_delay_sec/ 60, 2) AS avg_processing_delay_min
    -- , Round(p95_processing_delay_sec / 60, 2) AS p95_processing_delay_min
    -- , Round(Avg_processing_delay_sec / 3600, 2) AS avg_processing_delay_hr
    -- , Round(p95_processing_delay_sec / 3600, 2) AS p95_processing_delay_hr
FROM read_parquet('gold_v2/pipeline_metrics_per_minute/**/*.parquet')
ORDER BY window_start DESC
LIMIT 60;