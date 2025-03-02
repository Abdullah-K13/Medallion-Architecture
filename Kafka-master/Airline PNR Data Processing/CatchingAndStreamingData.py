# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StringType, IntegerType
from pyspark.sql.types import StructType, StringType, ArrayType


spark = SparkSession.builder \
    .appName("Kafka Integration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = "158.220.124.0:9094"  # Replace with your Kafka broker address
kafka_topic = "test-topic"                     # Replace with your Kafka topic  test_topic

customer_schema = StructType() \
    .add("customer_id", StringType()) \
    .add("first_name", StringType()) \
    .add("last_name", StringType()) \
    .add("email", StringType()) \
    .add("phone_number", StringType()) \
    .add("address", StringType()) \
    .add("city", StringType()) \
    .add("state", StringType()) \
    .add("postal_code", StringType()) \
    .add("country", StringType()) 

test_schema = StructType() \
    .add("id", IntegerType()) \
    .add("name", StringType()) \
    .add("value", StringType()) \

array_schema = ArrayType(customer_schema)

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

# Parse the JSON array data
#parsed_df = kafka_df.withColumn("json_data", from_json(col("value"), array_schema))

# Flatten the array into individual rows
#flattened_df = parsed_df.select(explode(col("json_data")).alias("customer_data"))
# Select individual fields from the parsed JSON
'''
final_df = parsed_df.select(
    col("json_data.customer_id").alias("customer_id"),
    col("json_data.first_name").alias("first_name"),
    col("json_data.last_name").alias("last_name"),
    col("json_data.email").alias("email"),
    col("json_data.phone_number").alias("phone_number"),
    col("json_data.address").alias("address"),
    col("json_data.city").alias("city"),
    col("json_data.state").alias("state"),
    col("json_data.postal_code").alias("postal_code"),
    col("json_data.country").alias("country"),
    

).dropDuplicates(["customer_id"])
'''
# Show the top rows of the structured table
kafka_df.select('value').show( truncate=False)


# COMMAND ----------

# This is the cell that needs to keep running in order to populate bronze layer tables 
# called "bronze_customer_table2" and "bronze_pnr_table2"

# Import necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
from pyspark.sql.functions import from_json, col, count
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Kafka Integration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = "158.220.124.0:9094"  # Replace with your Kafka broker address

# Define the schema for customer data
customer_schema = StructType() \
    .add("customer_id", IntegerType()) \
    .add("first_name", StringType()) \
    .add("last_name", StringType()) \
    .add("email", StringType()) \
    .add("phone_number", StringType()) \
    .add("passport_number", StringType()) \
    .add("nationality", StringType())

pnr_schema = StructType([
    StructField("pnr_id", StringType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("flight_number", StringType(), True),
    StructField("departure_airport", StringType(), True),
    StructField("arrival_airport", StringType(), True),
    StructField("departure_date", TimestampType(), True),
    StructField("arrival_date", TimestampType(), True),
    StructField("ticket_price", FloatType(), True),
    StructField("seat_number", StringType(), True),
    StructField("booking_status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("passport_number", StringType(), True)
])

# Read from Kafka
cus_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "customers_data") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .selectExpr('CAST(value AS STRING)') \
    .select(from_json(col('value'), customer_schema).alias('data')).select('data.*')

pnr_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "pnr_data") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .selectExpr('CAST(value AS STRING)') \
    .select(from_json(col('value'), pnr_schema).alias('data')).select('data.*')

# Add a timestamp column to each DataFrame
cus_df_with_timestamp = cus_df.withColumn("timestamp", F.current_timestamp())
pnr_df_with_timestamp = pnr_df.withColumn("timestamp", F.current_timestamp())

# Apply watermark on the DataFrames
cus_df_with_watermark = cus_df_with_timestamp.withWatermark("timestamp", "10 minutes")
pnr_df_with_watermark = pnr_df_with_timestamp.withWatermark("timestamp", "10 minutes")

# Write to Delta tables
query1 = cus_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "Files/RealTime/customers_checkpoint") \
    .table('bronze_customer_table2')

query2 = pnr_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "Files/RealTime/pnr_checkpoint") \
    .table('bronze_pnr_table2')


# Start the query
query1.awaitTermination()
query2.awaitTermination()

# COMMAND ----------

parsed_df.show(truncate=False)

# COMMAND ----------

json_rdd = kafka_df.select('value').rdd.map(lambda row: row['value'])

# Use spark.read.json on the RDD
df = spark.read.json(json_rdd)

df.show(truncate=False)
default_checkpoint_path = "/tmp/weather-data-checkpoints"
(df.write
  .format("delta")  # Specify Delta format
  .mode("append")   # Append mode for batch processing
  .saveAsTable('`pnr_data_2`')  # Correctly quoted table name
)


# COMMAND ----------

from pyspark.sql.functions import from_json, col, explode

exploded_df = flattened_df.select(explode("customer_data").alias("customer_id"))

# Now each row will have an individual customer_id
exploded_df.show(truncate=False)

#final_df.createOrReplaceTempView("customer_data_view")

# Use Spark SQL to create a permanent Hive-compatible table
#spark.sql("""
#    CREATE TABLE IF NOT EXISTS customer_data_table
#    AS SELECT * FROM customer_data_view
#""")

#print("Data successfully saved into the table.")

# COMMAND ----------


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Kafka Integration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = "158.220.124.0:9094"  # Replace with your Kafka broker address
kafka_topic = "customers_data"                     # Replace with your Kafka topic  test_topic

# Read from Kafka
cus_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", "customers_data") \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss", "false")\
  .load()

pnr_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", "pnr_data") \
  .option("startingOffsets", "latest") \
  .option("failOnDataLoss", "false")\
  .load()
# Convert Kafka message key and value from binary to string
new_cus_df = cus_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
new_pnr_df = pnr_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# Define a function to print each consumed message
def process_batch(df, batch_id):
    # Print the batch id and each row
    print(f"Processing batch {batch_id}:")
    for row in df.collect():
        print(f"Key: {row['key']}, Value: {row['value']}")

# Output each message and write to console
query1 = new_cus_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "Files/RealTime/checkpoint") \
    .start()

query2 = new_pnr_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "Files/RealTime/checkpoint") \
    .start()

query1.awaitTermination()
query2.awaitTermination()




# COMMAND ----------

from pyspark.sql import functions as F

json_schema = spark.read.json(kafka_df.rdd.map(lambda row: row.value)).schema
df = df.withColumn("value", F.col("value").cast("string"))
parsed_df = df.withColumn("json_data", F.from_json(F.col("value"), json_schema))

# If there are arrays you want to explode, use explode (optional)
# exploded_df = parsed_df.withColumn("exploded_col", F.explode(F.col("json_data.array_field")))

# Select individual fields from the parsed JSON
result_df = parsed_df.select(F.col("json_data.*"))  # This selects all fields in json_data

# Show the result
result_df.show()

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, ArrayType

# Schema for "current_units" and "hourly_units"
units_schema = StructType([
    StructField("time", StringType(), True),
    StructField("interval", StringType(), True),
    StructField("temperature_2m", StringType(), True),
    StructField("wind_speed_10m", StringType(), True)
])

# Schema for "current"
current_weather_schema = StructType([
    StructField("time", StringType(), True),
    StructField("interval", IntegerType(), True),  # Changed to IntegerType to match the data
    StructField("temperature_2m", FloatType(), True),
    StructField("wind_speed_10m", FloatType(), True)
])

# Schema for "hourly_units"
hourly_units_Schema = StructType([
    StructField("time", StringType(), True),
    StructField("temperature_2m", StringType(), True),
    StructField("relative_humidity_2m", StringType(), True),
    StructField("wind_speed_10m", StringType(), True)
])

# Schema for "hourly"
hourly_weather_schema = StructType([
    StructField("time", ArrayType(StringType()), True),
    StructField("temperature_2m", ArrayType(FloatType()), True),
    StructField("relative_humidity_2m", ArrayType(IntegerType()), True),  # Changed to ArrayType(IntegerType())
    StructField("wind_speed_10m", ArrayType(FloatType()), True)
])

# Overall schema for the weather data
weather_data_schema = StructType([
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("generationtime_ms", FloatType(), True),
    StructField("utc_offset_seconds", IntegerType(), True),
    StructField("timezone", StringType(), True),
    StructField("timezone_abbreviation", StringType(), True),
    StructField("elevation", IntegerType(), True),
    StructField("current_units", units_schema, True),
    StructField("current", current_weather_schema, True),
    StructField("hourly_units", hourly_units_Schema, True),
    StructField("hourly", hourly_weather_schema, True)
])

# Parse the JSON and create a new column with the structured data
parsed_df = df.withColumn("json_data", F.from_json(F.col("value").cast("string"), weather_data_schema))

# Extract individual fields from the JSON data
final_df = parsed_df.select(F.col("json_data.*"))
final_df.write.mode("overwrite").saveAsTable("parsed_weather_info")


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Kafka Integration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = "158.220.124.0:9094"  # Replace with your Kafka broker address
kafka_topic = "customers_data"                    # Replace with your Kafka topic

# Define the schema for the JSON data
customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("passport_number", StringType(), True),
    StructField("nationality", StringType(), True)
])

# Read from Kafka
df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# Convert Kafka message value from binary to string
kafka_df = df.selectExpr("CAST(value AS STRING)")

# Parse JSON data and create a new DataFrame with the defined schema
customers_df = kafka_df.select(F.from_json(F.col("value"), customer_schema).alias("customer_data"))

# Select the individual fields from the parsed JSON
customers_table = customers_df.select("customer_data.*")

# Show the result DataFrame
customers_table.show(truncate=False)

# Save the DataFrame as a new table (optional)
kafka_df.write.format("parquet").option("path", "Files/RealTime/batch_output").save()
# Stop the Spark session
spark.stop()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze_aggregated_table;

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define the schema for customer data
customer_data_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone_number", StringType(), True),
    StructField("passport_number", StringType(), True),
    StructField("nationality", StringType(), True)
])

