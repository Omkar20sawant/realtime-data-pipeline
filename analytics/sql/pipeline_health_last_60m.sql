-- pipeline_health_last_60m.sql
-- Shows the last 60 minutes of pipeline health metrics.
-- Timestamps stored in UTC; displayed in PST using AT TIME ZONE.

SELECT
    window_start AT TIME ZONE 'America/Los_Angeles'  AS window_start_pst,
    window_end   AT TIME ZONE 'America/Los_Angeles'  AS window_end_pst,
    events_valid_count,
    ROUND(avg_processing_delay_sec, 2)               AS avg_processing_delay_sec,
    ROUND(p95_processing_delay_sec, 2)               AS p95_processing_delay_sec,
    latest_event_ts_seen AT TIME ZONE 'America/Los_Angeles' AS latest_event_ts_pst,
    computed_at          AT TIME ZONE 'America/Los_Angeles' AS computed_at_pst,
    DATE_DIFF('second', latest_event_ts_seen, computed_at)  AS freshness_gap_sec
FROM read_parquet('gold_v2/pipeline_metrics_per_minute/**/*.parquet')
WHERE window_start >= NOW() - INTERVAL '60 minutes'
ORDER BY window_start DESC
LIMIT 60;
