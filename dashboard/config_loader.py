import os
import yaml

def load_config():
    with open("config/config.yaml", "r") as f:
        config = yaml.safe_load(f)

    return {
        "metrics_path": os.getenv("METRICS_PATH", config["paths"]["gold_metrics"]),
        "default_window_days": int(
            os.getenv("DEFAULT_WINDOW_DAYS", config["dashboard"]["default_window_days"])
        ),
        "refresh_interval_ms": int(
            os.getenv("REFRESH_INTERVAL_MS", config["dashboard"]["refresh_interval_ms"])
        ),
    }