
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


def trend_query(metrics_path: str, days: int) -> str:
    return f"""
    SELECT 
        window_start,
        events_valid_count,
        avg_processing_delay_sec,
        p95_processing_delay_sec,
        latest_event_ts_seen,
        computed_at
    FROM read_parquet('{metrics_path}')
    WHERE window_start >= NOW() - INTERVAL '{days} days'
    ORDER BY window_start ASC
    """


def recent_rows_query(metrics_path: str, days: int) -> str:
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
    WHERE window_start >= NOW() - INTERVAL '{days} days'
    ORDER BY window_start DESC
    LIMIT 100
    """