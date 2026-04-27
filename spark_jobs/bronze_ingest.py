from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

BRONZE_DIR = "bronze/events"
CHECKPOINT_DIR = "bronze/checkpoints/events"

# Define schema explicitly (required for streaming file source)
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_ts", StringType(), True),   # we'll convert to timestamp below
    StructField("store_id", IntegerType(), True),
    StructField("sku", StringType(), True),
    StructField("qty", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("channel", StringType(), True),
])

spark = (SparkSession.builder
         .appName("BronzeIngest")
         .master("local[*]")
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")

# Read Kafka stream
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "events_raw")
    .option("startingOffsets", "earliest")
    .load()
)

# Parse Kafka JSON payload into Bronze schema
df = (
    raw_df
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
    .withColumn("event_ts", to_timestamp(col("event_ts")))
    .withColumn("_ingest_ts", current_timestamp())
    .withColumn("_source_file", lit("kafka"))
)

# Write Bronze as Parquet (append-only)
query = (
    df.writeStream
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
