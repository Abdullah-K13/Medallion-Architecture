# Databricks notebook source
# Databricks notebook source

# Import necessary libraries
import requests
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("APIDataProcessing").getOrCreate()

# Function to fetch data from an online API
def fetch_api_data(url, params=None):
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        response.raise_for_status()

# URL for the API (example: CoinGecko API for Bitcoin prices)
api_url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
params = {
    "vs_currency": "usd",
    "days": "30",  # Fetch data for the last 30 days
    "interval": "daily"
}

# Fetch data from the API
data = fetch_api_data(api_url, params=params)

# Process the data
prices = data['prices']
dates = [pd.to_datetime(price[0], unit='ms') for price in prices]
values = [price[1] for price in prices]

# Create a pandas DataFrame
df = pd.DataFrame(data={'Date': dates, 'Price': values})

# Convert pandas DataFrame to Spark DataFrame
spark_df = spark.createDataFrame(df)

# Show the Spark DataFrame
spark_df.show()

# Save the Spark DataFrame as a Delta table
default_checkpoint_path = "/tmp/bitcoin-checkpoints"
(spark_df.write
  .format("delta")  # Specify Delta format
  .mode("append")   # Append mode for batch processing
  .saveAsTable('`bitcoin_data`')  # Correctly quoted table name
)
# Optional: Create a Delta table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS bitcoin_prices
USING DELTA
LOCATION '{delta_table_path}'
""")

# Display the Delta table
display(spark.sql("SELECT * FROM bitcoin_prices"))
