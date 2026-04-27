-- pipeline_run_history.sql
-- Detects distinct pipeline run sessions and summarises each one.
-- A new session starts when consecutive windows are more than 5 minutes apart.
-- All times displayed in PST (America/Los_Angeles).

WITH base AS (
    SELECT
        window_start,
        window_end,
        events_valid_count,
        avg_processing_delay_sec,
        p95_processing_delay_sec
    FROM read_parquet('gold_v2/pipeline_metrics_per_minute/**/*.parquet')
    ORDER BY window_start
),
with_prev AS (
    SELECT *,
        LAG(window_end) OVER (ORDER BY window_start) AS prev_window_end
    FROM base
),
with_gap AS (
    SELECT *,
        CASE
            WHEN prev_window_end IS NULL                                       THEN 1
            WHEN DATEDIFF('minute', prev_window_end, window_start) > 5        THEN 1
            ELSE 0
        END AS is_new_session
    FROM with_prev
),
with_session AS (
    SELECT *,
        SUM(is_new_session) OVER (ORDER BY window_start ROWS UNBOUNDED PRECEDING) AS session_id
    FROM with_gap
)
SELECT
    session_id,
    (MIN(window_start) AT TIME ZONE 'America/Los_Angeles') AS session_start_pst,
    (MAX(window_end)   AT TIME ZONE 'America/Los_Angeles') AS session_end_pst,
    ROUND(DATEDIFF('minute', MIN(window_start), MAX(window_end)), 0) AS duration_minutes,
    SUM(events_valid_count)                                AS total_events,
    ROUND(AVG(avg_processing_delay_sec), 2)                AS avg_delay_sec,
    ROUND(MAX(p95_processing_delay_sec), 2)                AS p95_delay_sec,
    CASE
        WHEN MAX(p95_processing_delay_sec) < 300  THEN 'Healthy'
        WHEN MAX(p95_processing_delay_sec) < 900  THEN 'Warning'
        ELSE 'Critical'
    END AS health
FROM with_session
GROUP BY session_id
ORDER BY MIN(window_start) DESC;
