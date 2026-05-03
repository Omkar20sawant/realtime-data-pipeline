from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

BRONZE_DIR = "bronze/events"
CHECKPOINT_DIR = "bronze/checkpoints/events"

# Known fields we want to parse into columns
parsed_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_ts", StringType(), True),   # convert below
    StructField("store_id", IntegerType(), True),
    StructField("sku", StringType(), True),
    StructField("qty", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("channel", StringType(), True),

    # new optional fields
    StructField("customer_type", StringType(), True),
    StructField("payment_method", StringType(), True),
])

spark = (
    SparkSession.builder
    .appName("BronzeIngest")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")

# Read Kafka stream
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "events_raw")
    .option("startingOffsets", "earliest")
    .load()
)

# Keep raw JSON + parse known fields
bronze_df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS _raw_json")
    .withColumn("data", from_json(col("_raw_json"), parsed_schema))
    .select(
        col("_raw_json"),
        col("data.event_id").alias("event_id"),
        to_timestamp(col("data.event_ts")).alias("event_ts"),
        col("data.store_id").alias("store_id"),
        col("data.sku").alias("sku"),
        col("data.qty").alias("qty"),
        col("data.price").alias("price"),
        col("data.channel").alias("channel"),
        col("data.customer_type").alias("customer_type"),
        col("data.payment_method").alias("payment_method"),
    )
    .withColumn("_ingest_ts", current_timestamp())
    .withColumn("_source_file", lit("kafka"))
)

# Write Bronze as Parquet
query = (
    bronze_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", BRONZE_DIR)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime="10 seconds")
    .start()
)

print("✅ Bronze ingest streaming started.")
print("Reading from Kafka topic: events_raw")
print(f"Writing to: {BRONZE_DIR}")

query.awaitTermination()