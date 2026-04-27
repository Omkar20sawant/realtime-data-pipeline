"""
DuckDB query builders for the pipeline monitoring dashboard.

All timestamps stored in Parquet are UTC.  Queries keep UTC for filtering
(so partition pruning still works); PST conversion is done in Python after
the DataFrame is returned.
"""


def latest_kpi_query(metrics_path: str) -> str:
    return f"""
    SELECT
        window_start,
        window_end,
        events_valid_count,
        avg_processing_delay_sec,
        p95_processing_delay_sec,
        latest_event_ts_seen,
        computed_at
    FROM read_parquet('{metrics_path}')
    ORDER BY window_start DESC
    LIMIT 1
    """


def trend_query(metrics_path: str, start_utc: str, end_utc: str) -> str:
    """
    Trend data for the selected date range.
    start_utc / end_utc are ISO-8601 strings in UTC, e.g. '2026-04-24 07:00:00'
    """
    return f"""
    SELECT
        window_start,
        events_valid_count,
        avg_processing_delay_sec,
        p95_processing_delay_sec,
        latest_event_ts_seen,
        computed_at
    FROM read_parquet('{metrics_path}')
    WHERE window_start >= TIMESTAMP '{start_utc}'
      AND window_start <  TIMESTAMP '{end_utc}'
    ORDER BY window_start ASC
    """


def recent_rows_query(metrics_path: str, start_utc: str, end_utc: str) -> str:
    return f"""
    SELECT
        window_start,
        window_end,
        events_valid_count,
        avg_processing_delay_sec,
        p95_processing_delay_sec,
        latest_event_ts_seen,
        computed_at
    FROM read_parquet('{metrics_path}')
    WHERE window_start >= TIMESTAMP '{start_utc}'
      AND window_start <  TIMESTAMP '{end_utc}'
    ORDER BY window_start DESC
    LIMIT 100
    """


def run_sessions_query(metrics_path: str, start_utc: str, end_utc: str) -> str:
    """
    Detect distinct pipeline run sessions within the date range.

    A new session starts whenever the gap between consecutive windows
    exceeds GAP_MINUTES minutes (default 5).  Each session row contains:
        session_id        – integer, 1 = most recent
        session_start     – UTC timestamp when the run started
        session_end       – UTC timestamp of the last window in the run
        duration_minutes  – how long the run lasted
        total_events      – sum of events_valid_count for the run
        avg_delay_sec     – average processing delay across all windows
        p95_delay_sec     – max of per-window p95 (conservative estimate)
        health            – Healthy / Warning / Critical based on avg p95
    """
    GAP_MINUTES = 5
    return f"""
    WITH base AS (
        SELECT
            window_start,
            window_end,
            events_valid_count,
            avg_processing_delay_sec,
            p95_processing_delay_sec
        FROM read_parquet('{metrics_path}')
        WHERE window_start >= TIMESTAMP '{start_utc}'
          AND window_start <  TIMESTAMP '{end_utc}'
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
                WHEN prev_window_end IS NULL THEN 1
                WHEN DATEDIFF('minute', prev_window_end, window_start) > {GAP_MINUTES} THEN 1
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
        MIN(window_start)                                        AS session_start_utc,
        MAX(window_end)                                          AS session_end_utc,
        ROUND(
            DATEDIFF('minute', MIN(window_start), MAX(window_end))
        , 0)                                                     AS duration_minutes,
        SUM(events_valid_count)                                  AS total_events,
        ROUND(AVG(avg_processing_delay_sec), 2)                  AS avg_delay_sec,
        ROUND(MAX(p95_processing_delay_sec), 2)                  AS p95_delay_sec,
        CASE
            WHEN MAX(p95_processing_delay_sec) < 300  THEN '🟢 Healthy'
            WHEN MAX(p95_processing_delay_sec) < 900  THEN '🟡 Warning'
            ELSE '🔴 Critical'
        END                                                      AS health
    FROM with_session
    GROUP BY session_id
    ORDER BY session_start_utc DESC
    """
