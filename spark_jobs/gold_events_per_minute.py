from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, count, expr
import os, glob


spark = SparkSession.builder.appName("GoldLayerStreaming").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

spark.conf.set("spark.sql.shuffle.partitions", "8")
spark.conf.set("spark.default.parallelism", "8")
spark.conf.set("spark.sql.streaming.stateStore.maintenanceInterval", "300")

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

SILVER_PATH = os.path.join(PROJECT_ROOT, "silver", "events")

# Gold output paths (root)
GOLD_EVENTS_PATH = os.path.join(PROJECT_ROOT, "gold", "events_per_minute")
GOLD_BUCKETS_PATH = os.path.join(PROJECT_ROOT, "gold", "delay_buckets_per_minute")
GOLD_PCT_PATH = os.path.join(PROJECT_ROOT, "gold", "latency_percentiles_per_minute")

# Checkpoints (root)
CP_EVENTS = os.path.join(PROJECT_ROOT, "checkpoints", "gold_events_per_minute")
CP_BUCKETS = os.path.join(PROJECT_ROOT, "checkpoints", "gold_delay_buckets")
CP_PCT = os.path.join(PROJECT_ROOT, "checkpoints", "gold_latency_percentiles")

# Infer schema from existing parquet file
parquet_files = glob.glob(os.path.join(SILVER_PATH, "*.parquet"))
if not parquet_files:
    raise RuntimeError(f"No parquet files found in {SILVER_PATH}. Start Silver first or wait for output.")
silver_schema = spark.read.parquet(parquet_files[0]).schema
print(f"✅ Inferred schema from: {parquet_files[0]}")

silver_df = (
    spark.readStream
    .format("parquet")
    .schema(silver_schema)
    .option("maxFilesPerTrigger", 10)   # start small: 5–20
    .load(SILVER_PATH)
)

# Common watermark
wm_df = silver_df.withWatermark("event_ts", "2 minutes")

# C) Events per minute, flattened columns
events_per_min_df = (
    wm_df
    .groupBy(window(col("event_ts"), "1 minute"))
    .agg(count("*").alias("events_count"))
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("events_count")
    )
)

# B) Delay buckets per minute
delay_buckets_df = (
    wm_df
    .groupBy(window(col("event_ts"), "1 minute"), col("delay_bucket"))
    .agg(count("*").alias("events_count"))
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("delay_bucket"),
        col("events_count")
    )
)

# A) Latency percentiles per minute
latency_pct_df = (
    wm_df
    .groupBy(window(col("event_ts"), "1 minute"))
    .agg(
        expr("percentile_approx(processing_delay_sec, 0.50)").alias("p50_delay_sec"),
        expr("percentile_approx(processing_delay_sec, 0.90)").alias("p90_delay_sec"),
        expr("percentile_approx(processing_delay_sec, 0.99)").alias("p99_delay_sec")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("p50_delay_sec"),
        col("p90_delay_sec"),
        col("p99_delay_sec")
    )
)

# Start 3 streaming writes
q_events = (
    events_per_min_df.writeStream
    .outputMode("append")
    .format("parquet")
    .option("checkpointLocation", CP_EVENTS)
    .start(GOLD_EVENTS_PATH)
)

q_buckets = (
    delay_buckets_df.writeStream
    .outputMode("append")
    .format("parquet")
    .option("checkpointLocation", CP_BUCKETS)
    .start(GOLD_BUCKETS_PATH)
)

q_pct = (
    latency_pct_df.writeStream
    .outputMode("append")
    .format("parquet")
    .option("checkpointLocation", CP_PCT)
    .start(GOLD_PCT_PATH)
)

print("🏆 Gold started (3 outputs)")
print(f"  C events/min: {GOLD_EVENTS_PATH}")
print(f"  B buckets/min: {GOLD_BUCKETS_PATH}")
print(f"  A pct latency/min: {GOLD_PCT_PATH}")

spark.streams.awaitAnyTermination()