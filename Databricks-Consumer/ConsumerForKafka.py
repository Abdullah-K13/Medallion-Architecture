#!/usr/bin/env python
# coding: utf-8

# ## FabricNotebook-To-ConnectKafka-Cluster
# 
# New notebook

# In[1]:


# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Kafka Integration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()


# In[2]:


# Kafka configuration
kafka_bootstrap_servers = "158.220.124.0:9094"  # Replace with your Kafka broker address
kafka_topic = "weather-updates"                     # Replace with your Kafka topic  test_topic

# Read from Kafka
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", kafka_topic) \
  .option("startingOffsets", "latest") \
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

