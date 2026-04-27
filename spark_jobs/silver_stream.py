from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = SparkSession.builder.appName("SilverLayerStreaming").getOrCreate()

print("✅ Silver streaming started.")
print("Reading from: bronze/events")
print("Writing to:  silver/events")

# ---- Infer Schema from Bronze (one-time batch read) ----
schema = spark.read.format("parquet").load("bronze/events").schema

# ---- Read Bronze as Stream with Schema ----
bronze_df = (
    spark.readStream
    .format("parquet")
    .schema(schema)
    .load("bronze/events")
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
valid_df = bronze_df.filter(condition)


# ---- Deduplication with Watermark (Stateful Streaming) ----
deduped_df = (
    valid_df.withWatermark("event_ts", "10 minutes")
    .dropDuplicates(["event_id"])   
)

# ---- Invalid Records ----
invalid_df = bronze_df.filter(~condition)

# ---- Write Silver( Valod Records) ----
silver_query = (
    deduped_df.writeStream
    .format("parquet")
    .option("checkpointLocation", "checkpoints/silver")
    .outputMode("append")
    .start("silver/events")
)

# ---- Write Bad Records ----
bad_query = (
    invalid_df.writeStream
    .format("parquet")
    .option("checkpointLocation", "checkpoints/silver_bad")
    .outputMode("append")
    .start("silver/bad_records")
)

# ---- Await Termination ----
silver_query.awaitTermination()
bad_query.awaitTermination()