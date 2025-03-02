from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructType, IntegerType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaToParquetAndCSV") \
    .getOrCreate()

# Kafka topic details
kafka_brokers = "158.220.124.0:9094"
kafka_topic = "customers_data"

# Read from Kafka topic
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_brokers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()

# Define schema for Kafka value (if it's JSON or other structured data)
customer_schema = StructType() \
    .add("customer_id", IntegerType()) \
    .add("first_name", StringType()) \
    .add("last_name", StringType()) \
    .add("email", StringType()) \
    .add("phone_number", StringType()) \
    .add("passport_number", StringType()) \
    .add("nationality", StringType())

# Parse Kafka message value as JSON and extract fields
parsed_df = kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), customer_schema).alias("data")) \
    .select("data.*")

# Add a 1-minute window to the data
windowed_df = parsed_df.withColumn("window", window(current_timestamp(), "1 minute"))

# Write stream to Parquet files
parquet_query = parsed_df.writeStream \
    .format("parquet") \
    .option("path", "/mnt/data/parquet/output") \
    .option("checkpointLocation", "/path/to/parquet/checkpoint") \
    .outputMode("append") \
    .start()

# Write stream to CSV files
csv_query = windowed_df.writeStream \
    .format("csv") \
    .option("path", "/mnt/data/parquet/output") \
    .option("checkpointLocation", "/path/to/csv/checkpoint") \
    .outputMode("append") \
    .start()

# Await termination
parquet_query.awaitTermination()
csv_query.awaitTermination()
