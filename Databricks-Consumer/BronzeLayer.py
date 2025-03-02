
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
