def get_freshness_status(freshness_gap_sec):
    if freshness_gap_sec is None:
        return "⚪ Unknown", "unknown"
    if freshness_gap_sec < 10:
        return "🟢 Healthy", "normal"
    elif freshness_gap_sec < 30:
        return "🟡 Warning", "inverse"
    else:
        return "🔴 Critical", "off"


def get_latency_status(p95_delay):
    if p95_delay is None:
        return "⚪ Unknown", "unknown"
    if p95_delay < 300:
        return "🟢 Healthy", "normal"
    elif p95_delay < 900:
        return "🟡 Warning", "inverse"
    else:
        return "🔴 Critical", "off"