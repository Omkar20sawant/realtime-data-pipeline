import os

def env(key: str, default: str) -> str:
    """
    Read an environment variable with a default.
    Keeps all config in one place and avoids hardcoding in jobs.
    """
    return os.getenv(key, default)

# ---- Bronze and Silver Storage paths ----
BRONZE_EVENTS_PATH = env("BRONZE_EVENTS_PATH", "bronze/events")
# SILVER_EVENTS_PATH = env("SILVER_EVENTS_PATH", "silver/events")


# ---- Gold outputs ----
# GOLD_EVENTS_PATH = env("GOLD_EVENTS_PATH", "gold/events_per_minute")
# GOLD_BUCKETS_PATH = env("GOLD_BUCKETS_PATH", "gold/delay_buckets_per_minute")
# GOLD_PCT_PATH = env("GOLD_PCT_PATH", "gold/latency_percentiles_per_minute")

# ---- Silver Checkpoints ----
# CHECKPOINT_SILVER = env("CHECKPOINT_SILVER", "checkpoints/silver")

# ---- Gold checkpoints ----
# CP_EVENTS = env("CP_EVENTS", "checkpoints/gold_events_per_minute")
# CP_BUCKETS = env("CP_BUCKETS", "checkpoints/gold_delay_buckets")
# CP_PCT = env("CP_PCT", "checkpoints/gold_latency_percentiles")

# ---- Streaming knobs ----
TRIGGER_INTERVAL = env("TRIGGER_INTERVAL", "20 seconds")  # e.g. "2 seconds", "10 seconds"
SHUFFLE_PARTITIONS = int(env("SHUFFLE_PARTITIONS", "8"))

# ---- Optional: schema evolution & file source behavior ----
MAX_FILES_PER_TRIGGER = int(env("MAX_FILES_PER_TRIGGER", "30"))

# ---- Bad records / quarantine ----
SILVER_BAD_RECORDS_PATH = env("SILVER_BAD_RECORDS_PATH", "silver/bad_records")
CHECKPOINT_SILVER_BAD = env("CHECKPOINT_SILVER_BAD", "checkpoints/silver_bad")

# ---- Silver (v2) ----
SILVER_EVENTS_PATH = env("SILVER_EVENTS_PATH", "silver/events_v2")
CHECKPOINT_SILVER = env("CHECKPOINT_SILVER", "checkpoints/silver_v2")

# ---- Gold outputs (v2) ----
GOLD_EVENTS_PATH = env("GOLD_EVENTS_PATH", "gold_v2/events_per_minute")
GOLD_BUCKETS_PATH = env("GOLD_BUCKETS_PATH", "gold_v2/delay_buckets_per_minute")
GOLD_PCT_PATH = env("GOLD_PCT_PATH", "gold_v2/latency_percentiles_per_minute")
GOLD_PPL_PATH = env("GOLD_PPL_PATH", "gold_v2/pipeline_metrics_per_minute")

# ---- Gold checkpoints (v2) ----
CP_EVENTS = env("CP_EVENTS", "checkpoints/gold_events_per_minute_v2")
CP_BUCKETS = env("CP_BUCKETS", "checkpoints/gold_delay_buckets_v2")
CP_PCT = env("CP_PCT", "checkpoints/gold_latency_percentiles_v2")
CP_PPL = env("CP_PPL", "checkpoints/pipeline_metrics_per_minute_v2")

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Validation toggles
VALIDATE_PATHS_ON_START = os.getenv("VALIDATE_PATHS_ON_START", "true").lower() == "true"
FAIL_IF_MISSING_INPUTS = os.getenv("FAIL_IF_MISSING_INPUTS", "true").lower() == "true"

import logging

def setup_logging():
    """
    Simple logger setup.
    - Prints logs to the terminal
    - Adds timestamps + log level (INFO/WARN/ERROR)
    """
    logging.basicConfig(
        level = getattr(logging, LOG_LEVEL.upper(), logging.INFO),
        format = "%(asctime)s %(levelname)s: %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S"
    )
    return logging.getLogger("pipeline")