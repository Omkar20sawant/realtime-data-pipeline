import os
import uuid
from pyspark.sql import SparkSession
from pyspark.sql.types import(StructType, StructField, StringType, TimestampType, IntegerType, DoubleType)
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_timestamp, to_date, hour,
    unix_timestamp, when, input_file_name,
    sha2, concat_ws, coalesce
)

from config import (
    BRONZE_EVENTS_PATH,
    SILVER_EVENTS_PATH,
    SILVER_BAD_RECORDS_PATH,
    CHECKPOINT_SILVER,
    CHECKPOINT_SILVER_BAD,
    TRIGGER_INTERVAL,
    SHUFFLE_PARTITIONS,
    MAX_FILES_PER_TRIGGER
)

spark = SparkSession.builder.appName("SilverLayerStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
# spark.conf.set("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))

RUN_ID = str(uuid.uuid4())
PIPELINE_VERSION = "v1.0"

print("✅ Silver streaming started.")
print("Reading from: bronze/events")
print("Writing to:  silver/events")

# ---- Infer Schema from Bronze (one-time batch read) ----
schema = spark.read.format("parquet").load(BRONZE_EVENTS_PATH).schema

# ---- Read Bronze as Stream with Schema ----
bronze_df = (
    spark.readStream
    .format("parquet")
    .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
    .option("mergeSchema", "true")
    .schema(schema)
    .load(BRONZE_EVENTS_PATH)
)

# ---- Standardize + Enrich (2) + (5) + (6) ----
base_df = (bronze_df
    .withColumn("event_ts", to_timestamp(col("event_ts")))
    .withColumn("_ingest_ts", current_timestamp())
    .withColumn("_source_file", input_file_name())
    .withColumn("_run_id", lit(RUN_ID))
    .withColumn("_pipeline_version", lit(PIPELINE_VERSION))
    .withColumn("event_date", to_date(col("event_ts")))
    .withColumn("ingest_date", to_date(col("_ingest_ts")))
    .withColumn("event_hour", hour(col("event_ts")))
    .withColumn(
        "processing_delay_sec",
        (unix_timestamp(col("_ingest_ts")) - unix_timestamp(col("event_ts"))).cast("long")
    )
    .withColumn(
        "delay_bucket",
        when(col("processing_delay_sec") < 5, "lt_5s")
        .when(col("processing_delay_sec") < 30, "5_30s")
        .when(col("processing_delay_sec") < 120, "30_120s")
        .otherwise("gte_120s")
    )
)


condition = (
    (col("event_id").isNotNull()) &
    (col("event_ts").isNotNull()) &
    (col("store_id").isNotNull()) &
    (col("sku").isNotNull()) &
    (col("qty").isNotNull()) &
    (col("price").isNotNull()) &
    (col("price") > 0) &
    (col("qty") > 0) &
    (col("channel").isin("web", "mobile", "store"))
)

# ---- Validation Rules ----
valid_df = base_df.filter(condition)
invalid_df = base_df.filter(~condition)


# ---- Deduplication with Watermark (Stateful Streaming) ----
valid_df = valid_df.withColumn(
    "dedup_comp_key",
    sha2(
        concat_ws("||",
            col("store_id"),
            col("sku"),
            col("channel"),
            col("event_ts").cast("string"),
            col("price").cast("string"),
            col("qty").cast("string")
        ),
        256
    )
).withColumn(
    "_dedup_comp_id",
    coalesce(col("event_id"), col("dedup_comp_key"))
)

deduped_df = (
    valid_df
    .withWatermark("event_ts", "10 minutes")
    .dropDuplicates(["_dedup_comp_id"])
    .drop("_dedup_comp_id")
)

# ---- Write Silver Valid ----
silver_query = (
    deduped_df.writeStream
    .format("parquet")
    .option("checkpointLocation", CHECKPOINT_SILVER)
    .outputMode("append")
    .trigger(processingTime=TRIGGER_INTERVAL)
    .partitionBy("event_date")               # ✅ added this? Why : 
    .start(SILVER_EVENTS_PATH)
)

import time
time.sleep(10)
print("Silver lastProgress:", silver_query.lastProgress)

# ---- Write Silver Invalid ----
bad_query = (
    invalid_df.writeStream
    .format("parquet")
    .option("checkpointLocation", CHECKPOINT_SILVER_BAD)
    .outputMode("append")
    .trigger(processingTime=TRIGGER_INTERVAL)   # ✅ add this
    .partitionBy("ingest_date")  # optional
    .start(SILVER_BAD_RECORDS_PATH)
)

spark.streams.awaitAnyTermination()