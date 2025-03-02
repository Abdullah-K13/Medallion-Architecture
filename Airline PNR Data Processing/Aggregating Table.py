# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read Delta Tables Example") \
    .getOrCreate()

customers_df = spark.readStream \
    .format("delta") \
    .table("cust_data_new2")

filtered_stream_df = customers_df.select("*")

query = customers_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()


# COMMAND ----------

#This is the cell which is creating a a golden layer table named "bronze_aggregated_table2"
#It fetches info from two tables "bronze_customer_table2"
#It fetches info from two tables "bronze_pnr_table2"

from pyspark.sql import SparkSession
import time
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Batch Write Example") \
    .getOrCreate()

# Create the aggregated table if it doesn't exist
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze_aggregated_table2 (
    unique_id STRING,
    customer_id_c STRING,
    customer_id_p STRING,
    first_name STRING,
    last_name STRING,
    pnr_id STRING,
    flight_number STRING,
    ticket_price FLOAT
) USING DELTA
""")

while True:
    # Query your data
    df = spark.sql("""
        SELECT c.customer_id AS customer_id_c, 
               p.customer_id AS customer_id_p, 
               c.first_name, 
               c.last_name, 
               p.pnr_id,
               p.flight_number,
               p.ticket_price
        FROM bronze_customer_table2 c 
        INNER JOIN bronze_pnr_table2 p ON c.customer_id = p.customer_id
    """)

    # Create a unique identifier for each record
    df_with_unique_id = df.withColumn("unique_id", F.expr("concat(customer_id_c, pnr_id)"))

    # Perform the merge operation
    df_with_unique_id.createOrReplaceTempView("new_data")

    merge_query = """
    MERGE INTO bronze_aggregated_table2 AS target
    USING new_data AS source
    ON target.unique_id = source.unique_id
    WHEN MATCHED THEN
        UPDATE SET 
            target.customer_id_c = source.customer_id_c,
            target.customer_id_p = source.customer_id_p,
            target.first_name = source.first_name,
            target.last_name = source.last_name,
            target.pnr_id = source.pnr_id,
            target.flight_number = source.flight_number,
            target.ticket_price = source.ticket_price
    WHEN NOT MATCHED THEN
        INSERT (unique_id, customer_id_c, customer_id_p, first_name, last_name, pnr_id, flight_number, ticket_price)
        VALUES (source.unique_id, source.customer_id_c, source.customer_id_p, source.first_name, source.last_name, source.pnr_id, source.flight_number, source.ticket_price)
    """

    # Execute the merge query
    spark.sql(merge_query)

    time.sleep(30)  # Wait for 30 seconds before running the query again


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  bronze_aggregated_table2;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW customer_pnr_aggregated_new AS
# MAGIC SELECT
# MAGIC     c.customer_id AS customer_id_c,
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     p.pnr_id,
# MAGIC     p.flight_number,
# MAGIC     p.ticket_price
# MAGIC FROM bronze_customer_table2 c
# MAGIC LEFT JOIN bronze_pnr_table2 p
# MAGIC ON c.customer_id = p.customer_id;
# MAGIC

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, window
from pyspark.sql.types import StringType, StructType, IntegerType
import os
from datetime import datetime

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

# Add a timestamp column
timestamped_df = parsed_df.withColumn("timestamp", current_timestamp())

# Define the output paths for parquet and csv
parquet_output_path = "/mnt/data/parquet/output"
csv_output_path = "/mnt/data/csv/output"

parquet_query = timestamped_df.writeStream \
    .format("parquet") \
    .option("path", parquet_output_path) \
    .option("checkpointLocation", "/path/to/parquet/checkpoint") \
    .outputMode("append") \
    .start()

# Function to write DataFrame to CSV with a dynamic filename
def write_to_csv(df, epoch_id):
    # Generate filename with the desired format
    filename = f"Airlines_{datetime.now().strftime('%d-%b-%Y %H:%M:%S')}.csv"
    file_path = os.path.join(csv_output_path, filename)

    # Write DataFrame to CSV
    df.write.csv(file_path, mode='overwrite', header=True)

# Write stream to CSV files every minute
csv_query = timestamped_df.writeStream \
    .trigger(processingTime="1 minute") \
    .foreachBatch(write_to_csv) \
    .option("checkpointLocation", "/path/to/csv/checkpoint") \
    .start()



# COMMAND ----------

# Await termination
parquet_query.awaitTermination()
csv_query.awaitTermination()


# COMMAND ----------

display(dbutils.fs.ls("/mnt/data/csv/output"))


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW customer_pnr_aggregated_new AS
# MAGIC SELECT
# MAGIC     c.customer_id AS customer_id_c,  -- Customer ID from the customers table
# MAGIC     c.first_name,
# MAGIC     c.last_name,
# MAGIC     p.pnr_id,
# MAGIC     p.flight_number,
# MAGIC     p.ticket_price,
# MAGIC     p.customer_id AS pnr_customer_id  -- Rename the customer_id from the PNR table
# MAGIC FROM bronze_customer_table2 c
# MAGIC LEFT JOIN bronze_pnr_table2 p  -- Add alias 'p' here
# MAGIC ON c.customer_id = p.customer_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE LIVE TABLE customer_orders
# MAGIC AS
# MAGIC SELECT 
# MAGIC     order_id,
# MAGIC     customer_id,
# MAGIC     product_id,
# MAGIC     quantity,
# MAGIC     order_date
# MAGIC FROM
# MAGIC     STREAM(LIVE.raw_orders)
# MAGIC WHERE
# MAGIC     order_status = 'Completed';

# COMMAND ----------

from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.getOrCreate()

# Read the streaming source (adjust the format and path as necessary)
customer_stream = spark.readStream.table("bronze_customer_table2")
pnr_stream = spark.readStream.table("bronze_pnr_table2")

# Perform the join operation
aggregated_stream = customer_stream.join(pnr_stream, customer_stream.customer_id == pnr_stream.customer_id, "left")

# Write the result to a Delta table continuously
query = (aggregated_stream.writeStream
         .format("delta")
         .outputMode("append")
         .option("checkpointLocation", "/mnt/checkpoints/customer_pnr_aggregated2_checkpoint")
         .table("customer_pnr_aggregated2"))  # Change to a location where you can store the checkpoint

# Start the query
query.awaitTermination()

