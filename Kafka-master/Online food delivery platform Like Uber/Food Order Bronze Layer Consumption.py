# Databricks notebook source

# Import necessary modules
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, LongType, TimestampType, FloatType, BooleanType, DecimalType
from pyspark.sql.functions import from_json, col, count
from pyspark.sql import functions as F

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("Kafka Integration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = "158.220.124.0:9094"  # Replace with your Kafka broker address

# Schema for 'users_data'
customer_schema = StructType() \
    .add("user_id", LongType()) \
    .add("username", StringType()) \
    .add("email", StringType()) \
    .add("password_hash", StringType()) \
    .add("phone_number", StringType()) \
    .add("address", StringType()) \
    .add("registration_date", TimestampType()) \
    .add("status", StringType())

# Schema for 'restaurant_data'
restaurant_schema = StructType() \
    .add("restaurant_id", LongType()) \
    .add("name", StringType()) \
    .add("address", StringType()) \
    .add("phone_number", StringType()) \
    .add("cuisine", StringType()) \
    .add("rating", FloatType())

# Schema for 'menu_items'
menu_items_schema = StructType() \
    .add("menu_item_id", LongType()) \
    .add("restaurant_id", LongType()) \
    .add("name", StringType()) \
    .add("description", StringType()) \
    .add("price", DecimalType(10, 2)) \
    .add("availability", BooleanType())

# Schema for 'orders'
orders_schema = StructType() \
    .add("order_id", LongType()) \
    .add("user_id", LongType()) \
    .add("restaurant_id", LongType()) \
    .add("order_date", TimestampType()) \
    .add("total_amount", DecimalType(10, 2)) \
    .add("status", StringType())

# Schema for 'order_items'
order_items_schema = StructType() \
    .add("order_item_id", LongType()) \
    .add("order_id", LongType()) \
    .add("menu_item_id", LongType()) \
    .add("quantity", IntegerType()) \
    .add("price", DecimalType(10, 2))

# Schema for 'delivery_detail'
delivery_detail_schema = StructType() \
    .add("delivery_id", LongType()) \
    .add("order_id", LongType()) \
    .add("delivery_address", StringType()) \
    .add("delivery_person", StringType()) \
    .add("contact_number", StringType()) \
    .add("delivery_status", StringType()) \
    .add("delivery_time", TimestampType())

# Read from Kafka
cus_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "users_data") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .selectExpr('CAST(value AS STRING)') \
    .select(from_json(col('value'), customer_schema).alias('data')).select('data.*')


# Read Stream for 'restaurant_data'
rest_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "restaurant_data") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .selectExpr('CAST(value AS STRING)') \
    .select(from_json(col('value'), restaurant_schema).alias('data')).select('data.*')

# Read Stream for 'menu_items'
menu_items_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "menu_items") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .selectExpr('CAST(value AS STRING)') \
    .select(from_json(col('value'), menu_items_schema).alias('data')).select('data.*')

# Read Stream for 'orders'
orders_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "orders") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .selectExpr('CAST(value AS STRING)') \
    .select(from_json(col('value'), orders_schema).alias('data')).select('data.*')

# Read Stream for 'order_items'
order_items_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "order_items") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .selectExpr('CAST(value AS STRING)') \
    .select(from_json(col('value'), order_items_schema).alias('data')).select('data.*')

# Read Stream for 'delivery_detail'
delivery_detail_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "delivery_detail") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .selectExpr('CAST(value AS STRING)') \
    .select(from_json(col('value'), delivery_detail_schema).alias('data')).select('data.*')

# Write to Delta tables
query1 = cus_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "Files/RealTime/users_checkpoint") \
    .table('bronze_users_table')

query2 = rest_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "Files/RealTime/restaurant_checkpoint") \
    .table('bronze_restaurant_table')

query3 = menu_items_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "Files/RealTime/menu_items_checkpoint") \
    .table('bronze_menu_items_table')

query4 = orders_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "Files/RealTime/orders_checkpoint") \
    .table('bronze_orders_table')

query5 = order_items_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "Files/RealTime/order_items_checkpoint") \
    .table('bronze_order_items_table')

query6 = delivery_detail_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "Files/RealTime/delivery_details_checkpoint") \
    .table('bronze_delivery_details_table')

query1.awaitTermination()
query2.awaitTermination()
query3.awaitTermination()
query4.awaitTermination()
query5.awaitTermination()
query6.awaitTermination()






# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from bronze_users_table;
# MAGIC --select * from bronze_restaurant_table;
# MAGIC --select * from bronze_orders_table;
# MAGIC --select * from bronze_order_items_table;
# MAGIC --select * from bronze_menu_items_table;
# MAGIC --select * from bronze_delivery_details_table;
# MAGIC
# MAGIC --drop table bronze_users_table;
# MAGIC --drop table bronze_restaurant_table;
# MAGIC --drop table bronze_orders_table;
# MAGIC --drop table bronze_order_items_table;
# MAGIC --drop table bronze_menu_items_table;
# MAGIC --drop table bronze_delivery_details_table;
# MAGIC

# COMMAND ----------

# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Kafka Integration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

# Kafka configuration
kafka_bootstrap_servers = "158.220.124.0:9094"  # Replace with your Kafka broker address
kafka_topic = "users_data"                     # Replace with your Kafka topic  test_topic

# Read from Kafka
df = spark \
  .readStream \
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

