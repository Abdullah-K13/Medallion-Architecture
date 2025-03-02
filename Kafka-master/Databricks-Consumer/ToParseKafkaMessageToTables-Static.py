# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Kafka Integration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = "158.220.124.0:9094"  # Replace with your Kafka broker address
kafka_topic = "weather-updates"                     # Replace with your Kafka topic  test_topic

# Read from Kafka
df = spark \
  .read \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", kafka_topic) \
  .option("startingOffsets", "earliest") \
  .option("failOnDataLoss", "false")\
  .load()

# Convert Kafka message key and value from binary to string
kafka_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Define a function to print each consumed message
def process_batch(df, batch_id):
    # Print the batch id and each row
    print(f"Processing batch {batch_id}:")
    for row in df.collect():
        print(f"Key: {row['key']}, Value: {row['value']}")

# Output each message and write to console
query = kafka_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "Files/RealTime/checkpoint") \
    .start()

query.awaitTermination()


# COMMAND ----------

default_checkpoint_path = "/tmp/weather-data-checkpoints"
output_path = "/tmp/weather-data-test"  # Path to store the parquet files
(kafka_df.write
  .format("delta")  # Specify Delta format
  .mode("append")   # Append mode for batch processing
  .saveAsTable("weather_test_table")  # Save the DataFrame as a Delta table
)

# COMMAND ----------

from pyspark.sql import functions as F

json_schema = spark.read.json(kafka_df.rdd.map(lambda row: row.value)).schema
parsed_df = df.withColumn("json_data", F.from_json(F.col("value"), json_schema))

# If there are arrays you want to explode, use explode (optional)
# exploded_df = parsed_df.withColumn("exploded_col", F.explode(F.col("json_data.array_field")))

# Select individual fields from the parsed JSON
result_df = parsed_df.select(F.col("json_data.*"))  # This selects all fields in json_data

# Show the result
result_df.show()
