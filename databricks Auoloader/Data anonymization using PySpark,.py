# Databricks notebook source
# Here's a similar example of data anonymization using PySpark, following the structure of your provided image:
from pyspark.sql.functions import col, sha2, concat_ws, lit
from enum import Enum


# Initialize Spark session
spark = SparkSession.builder.appName("CustomerExample").getOrCreate()

# Define the schema for the customer data
schema = StructType([
    StructField("customerName", StringType(), True),
    StructField("customerID", IntegerType(), True),
    StructField("birthDate", DateType(), True)
])

# Create sample customer data
data = [
    ("John Doe", 12345, date(1990, 5, 21)),
    ("Jane Smith", 23456, date(1985, 8, 15)),
    ("Alice Johnson", 34567, date(1992, 3, 10)),
    ("Bob Lee", 45678, date(1980, 12, 22)),
    ("Charlie Brown", 56789, date(1995, 7, 30))
]

# Create the DataFrame with the schema
df = spark.createDataFrame(data, schema)


# Define data masking approaches
class DataMaskingApproach(Enum):
    MASK_STRING = 1
    MASK_INT = 2
    MASK_DATE = 3

# Masking implementations
def mask_string(col_name):
    return sha2(col(col_name).cast("string"), 256)

def mask_int(col_name):
    # Convert integer to string, hash it, and then convert it back to an integer-like format.
    return sha2(col(col_name).cast("string"), 256)

def mask_date(col_name):
    # Convert the date to a string and apply SHA-256 hashing
    return sha2(concat_ws("-", col(col_name).cast("string"), lit("mask_date")), 256)

# Define personal data column policies
personal_data_columns = {
    "customerName": DataMaskingApproach.MASK_STRING,
    "customerID": DataMaskingApproach.MASK_INT,
    "birthDate": DataMaskingApproach.MASK_DATE
}

# Data anonymization transformer
def dataAnonymizationTransformer(df):
    for column_name, approach in personal_data_columns.items():
        if approach == DataMaskingApproach.MASK_STRING:
            df = df.withColumn(column_name + "_masked", mask_string(column_name))
        elif approach == DataMaskingApproach.MASK_INT:
            df = df.withColumn(column_name + "_masked", mask_int(column_name))
        elif approach == DataMaskingApproach.MASK_DATE:
            df = df.withColumn(column_name + "_masked", mask_date(column_name))
    return df

# Example usage on a DataFrame
# Assuming 'df' is your PySpark DataFrame with customer data
df_anonymized = dataAnonymizationTransformer(df)
df_anonymized.show(truncate=False)
