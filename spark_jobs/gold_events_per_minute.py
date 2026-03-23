from pyspark.sql import SparkSession
from pyspark.sql.functions import window, col, count, expr, to_date, avg, current_timestamp, max
from pyspark.sql import functions as F

import os, glob
import config
from config import (
    SILVER_EVENTS_PATH, GOLD_EVENTS_PATH, GOLD_BUCKETS_PATH, GOLD_PCT_PATH, GOLD_PPL_PATH, 
    CP_EVENTS, CP_BUCKETS, CP_PCT, CP_PPL,
    TRIGGER_INTERVAL,
    SHUFFLE_PARTITIONS,
    MAX_FILES_PER_TRIGGER
)

spark = SparkSession.builder.appName("GoldLayerStreaming").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

spark.conf.set("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
spark.conf.set("spark.default.parallelism", "8")
spark.conf.set("spark.sql.streaming.stateStore.maintenanceInterval", "300")

logger = config.setup_logging()
logger.info("Gold v2 job starting...")

# Infer schema from existing parquet file

parquet_files = glob.glob(os.path.join(SILVER_EVENTS_PATH, "**", "*.parquet"), recursive=True)
if not parquet_files:
    raise RuntimeError(f"No parquet files found in {SILVER_EVENTS_PATH} (recursive). Start Silver first or wait for output.")

silver_schema = spark.read.parquet(parquet_files[0]).schema
print(f"✅ Inferred schema from: {parquet_files[0]}")

silver_df = (
    spark.readStream
    .format("parquet")
    .schema(silver_schema)
    .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)   # start small: 5–20
    .load(SILVER_EVENTS_PATH)
)

# Common watermark
wm_df = silver_df.withWatermark("event_ts", "2 minutes")

# A) Events per minute, flattened columns
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

# C) Latency percentiles per minute
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

# D) Pipeline metrics per minute (valid-only, no bad_records)
pipeline_metrics_df = (
    wm_df.groupBy(window(col("event_ts"), "1 minute"))
    .agg(
        count("*").alias("events_valid_count"), 
        F.avg(col("processing_delay_sec")).alias("avg_processing_delay_sec"), 
        expr("percentile_approx(processing_delay_sec, 0.95)").alias("p95_processing_delay_sec"),
        F.max(col("event_ts")).alias("latest_event_ts_seen")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("events_valid_count"),
        col("avg_processing_delay_sec"),
        col("p95_processing_delay_sec"),
        col("latest_event_ts_seen"),
        current_timestamp().alias("computed_at"),
    )
)


events_per_min_df = events_per_min_df.withColumn("window_date", to_date(col("window_start")))
delay_buckets_df = delay_buckets_df.withColumn("window_date", to_date(col("window_start")))
latency_pct_df = latency_pct_df.withColumn("window_date", to_date(col("window_start")))
pipeline_metrics_df = pipeline_metrics_df.withColumn("window_date", to_date(col("window_start")))

# Start 3 streaming writes
q_events = (
    events_per_min_df.writeStream
    .outputMode("append")
    .format("parquet")
    .option("checkpointLocation", CP_EVENTS)
    .trigger(processingTime= TRIGGER_INTERVAL)
    .partitionBy("window_date")          # ✅
    .start(GOLD_EVENTS_PATH)
)

q_buckets = (
    delay_buckets_df.writeStream
    .outputMode("append")
    .format("parquet")
    .option("checkpointLocation", CP_BUCKETS)
    .trigger(processingTime= TRIGGER_INTERVAL)
    .partitionBy("window_date")          # ✅
    .start(GOLD_BUCKETS_PATH)
)

q_pct = (
    latency_pct_df.writeStream
    .outputMode("append")
    .format("parquet")
    .option("checkpointLocation", CP_PCT)
    .trigger(processingTime= TRIGGER_INTERVAL)
    .partitionBy("window_date")          # ✅
    .start(GOLD_PCT_PATH)
)

q_ppl = (
    pipeline_metrics_df.writeStream
    .outputMode("append")
    .format("parquet")
    .option("checkpointLocation", CP_PPL)
    .trigger(processingTime=TRIGGER_INTERVAL)
    .partitionBy("window_date")
    .start(GOLD_PPL_PATH)
)

logger.info("🏆 Gold started (3 outputs)")
logger.info(f"C events/min: {GOLD_EVENTS_PATH}")
logger.info(f"B buckets/min: {GOLD_BUCKETS_PATH}")
logger.info(f"A pct latency/min: {GOLD_PCT_PATH}")

logger.info("🏥 Pipeline metrics started")
logger.info(f"PPL metrics/min: {GOLD_PPL_PATH}")

spark.streams.awaitAnyTermination()