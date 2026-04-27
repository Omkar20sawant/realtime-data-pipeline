SELECT
    window_start,
    ROUND(avg_processing_delay_sec, 2) AS avg_processing_delay_sec,
    ROUND(p95_processing_delay_sec, 2) AS p95_processing_delay_sec
FROM read_parquet('gold_v2/pipeline_metrics_per_minute/**/*.parquet')
ORDER BY window_start;