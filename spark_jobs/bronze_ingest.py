from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, current_timestamp, col, lit, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

RAW_DIR = "data/raw_events"
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

#  Read JSON Lines files as a stream
# Read JSON Lines files as a stream
df = (
    spark.readStream
    .format("json")
    .schema(schema)
    .load(RAW_DIR)
    .withColumn("event_ts", to_timestamp(col("event_ts")))
    .withColumn("_ingest_ts", current_timestamp())
    .withColumn("_source_file", input_file_name())
)


#Write Bronze as Parquet(append_only)

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
print(f"Reading from: {RAW_DIR}")
print(f"Writing to:  {BRONZE_DIR}")

query.awaitTermination()

