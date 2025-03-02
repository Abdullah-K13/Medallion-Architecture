# Databricks notebook source
# Below is a small PySpark notebook script that you can run in Databricks to keep the cluster alive by executing a lightweight action repeatedly. # This script will execute a simple query in an infinite loop with a sleep interval in between to prevent the cluster from going idle.

from pyspark.sql import SparkSession
import time

# Initialize Spark Session (Databricks has an active spark session by default)
spark = SparkSession.builder.getOrCreate()

# Set sleep interval in seconds (e.g., 600 seconds = 10 minutes)
sleep_interval = 600

# Keep the cluster alive by running a small action periodically
while True:
    try:
        # Perform a simple action to keep the cluster active
        df = spark.range(1, 2)  # Create a small DataFrame
        df.count()  # Trigger an action (e.g., count the rows)
        
        print("Cluster kept alive by running a lightweight action.")
        
    except Exception as e:
        print(f"Error keeping the cluster alive: {e}")

    # Sleep for the defined interval before repeating the action
    time.sleep(sleep_interval)
