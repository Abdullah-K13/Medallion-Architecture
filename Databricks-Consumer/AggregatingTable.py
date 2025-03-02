
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

