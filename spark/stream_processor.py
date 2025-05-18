from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType

# Define schema for Kafka message value
schema = StructType() \
    .add("user_id", IntegerType()) \
    .add("event_type", StringType()) \
    .add("target_id", IntegerType()) \
    .add("timestamp", StringType()) \
    .add("device", StringType()) \
    .add("location", StringType())

# Create Spark session with Kafka support
spark = SparkSession.builder \
    .appName("SocialEventStreamProcessor") \
    .master("local[*]") \
    .getOrCreate()

# Read from Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "social_events") \
    .option("startingOffsets", "latest") \
    .load()

# Convert value (bytes) to string, then parse JSON
df_json = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Write to parquet output folder
query = df_json.writeStream \
    .format("parquet") \
    .option("path", "../parquet_output/") \
    .option("checkpointLocation", "../parquet_output/checkpoint/") \
    .outputMode("append") \
    .start()

query.awaitTermination()
