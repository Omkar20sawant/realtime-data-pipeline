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
spark.conf.set("spark.sql.shuffle.partitions", str(SHUFFLE_PARTITIONS))
spark.conf.set("spark.sql.session.timeZone", "UTC")

RUN_ID = str(uuid.uuid4())
PIPELINE_VERSION = "v1.0"

print("✅ Silver streaming started.")
print("Reading from: bronze/events")
print("Writing to:  silver/events")


# ---- Silver contract: columns Silver should always have ----
EXPECTED_SILVER_COLUMNS = {
    "event_id": StringType(),
    "event_ts": TimestampType(),
    "store_id": IntegerType(),
    "sku": StringType(),
    "qty": IntegerType(),
    "price": DoubleType(),
    "channel": StringType(),
    "_ingest_ts": TimestampType(),
    "_source_file": StringType(), 
    #fuure_fields:
    "customer_type": StringType(),  ##Can be a member or a guest login to buy skus
    "payment_method": StringType()  ##How the customer paid for the order (credit card, cash, Gift Card, Debit card, etc)
}

def extend_schema(base_schema, expected_cols):
    merged = StructType(base_schema.fields[:])
    existing = set(base_schema.fieldNames())

    for col_name, data_type in expected_cols.items():
        if col_name not in existing:
            merged.add(StructField(col_name, data_type, True))
    return merged

def ensure_expected_columns(df, expected_cols):
    for col_name, data_type in expected_cols.items():
        if col_name not in df. columns:
            df = df.withColumn(col_name, lit(None).cast(data_type))
        else: 
            df = df.withColumn(col_name, col(col_name).cast(data_type))
    return df

# ---- Infer Schema from Bronze (one-time batch read) ----
source_schema = spark.read.format("parquet").load(BRONZE_EVENTS_PATH).schema

# ---- Extend schema with optional/future Silver columns ----
stream_schema = extend_schema(source_schema, EXPECTED_SILVER_COLUMNS)

# ---- Read Bronze as Stream with Schema ----
bronze_df = (
    spark.readStream
    .format("parquet")
    .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
    .option("mergeSchema", "true")  # .option("ignoreChanges", "true")  # optional: ignore schema changes after start
    .schema(stream_schema)
    .load(BRONZE_EVENTS_PATH)
)

#  ---- Make Silver schema-safe ----
bronze_df = ensure_expected_columns(bronze_df, EXPECTED_SILVER_COLUMNS)

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

FINAL_COLUMNS = [
    "event_id", "event_ts", "store_id", "sku", "qty", "price", "channel",
    "customer_type", "payment_method",
    "_ingest_ts", "_source_file", "_run_id", "_pipeline_version",
    "event_date", "ingest_date", "event_hour", "processing_delay_sec", "delay_bucket"
]

base_df = base_df.select(*FINAL_COLUMNS)


condition = (
    (col("event_id").isNotNull()) &
    (col("event_ts").isNotNull()) &
    (col("store_id").isNotNull()) &
    (col("sku").isNotNull()) &
    (col("qty").isNotNull()) &
    (col("price").isNotNull()) &
    (col("channel").isNotNull()) &
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