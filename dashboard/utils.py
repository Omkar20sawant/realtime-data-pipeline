import pandas as pd

# ---------------------------------------------------------------------------
# Timezone helper
# ---------------------------------------------------------------------------
PST_TZ = "America/Los_Angeles"  # handles both PST (UTC-8) and PDT (UTC-7)


def to_pst(series: pd.Series) -> pd.Series:
    """
    Convert a pandas datetime Series from UTC to America/Los_Angeles (PST/PDT).
    Works whether the series is tz-aware or tz-naive (assumes UTC if naive).
    """
    s = pd.to_datetime(series, errors="coerce")
    if s.dt.tz is None:
        s = s.dt.tz_localize("UTC")
    return s.dt.tz_convert(PST_TZ)


def fmt_pst(series: pd.Series, fmt: str = "%Y-%m-%d %H:%M:%S %Z") -> pd.Series:
    """Convert to PST and return as formatted strings (safe for display)."""
    return to_pst(series).dt.strftime(fmt)


# ---------------------------------------------------------------------------
# Status helpers (unchanged)
# ---------------------------------------------------------------------------

def get_freshness_status(freshness_gap_sec):
    if freshness_gap_sec is None:
        return "⚪ Unknown", "off"
    if freshness_gap_sec < 10:
        return "🟢 Healthy", "normal"
    elif freshness_gap_sec < 30:
        return "🟡 Warning", "inverse"
    else:
        return "🔴 Critical", "off"


def get_latency_status(p95_delay):
    if p95_delay is None:
        return "⚪ Unknown", "off"
    if p95_delay < 300:
        return "🟢 Healthy", "normal"
    elif p95_delay < 900:
        return "🟡 Warning", "inverse"
    else:
        return "🔴 Critical", "off"
