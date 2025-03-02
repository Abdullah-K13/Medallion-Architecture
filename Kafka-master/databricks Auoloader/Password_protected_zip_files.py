# This file will extract the password protected zip file and then display the data in it
import os
import pyzipper
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Extract Password Protected Zip").getOrCreate()

# Define paths
dbfs_zip_file_path = 'dbfs:/FileStore/Ziptest/Customer2.zip'  # DBFS path to the zip file
local_zip_file_path = '/tmp/Customer.zip'  # Local path to store the copied zip file
extracted_file_path = '/tmp/extracted_files/'  # Local path where files will be extracted

# Step 1: Copy the password-protected zip file from DBFS to the local file system
dbutils.fs.cp(dbfs_zip_file_path, f"file:{local_zip_file_path}")

# Step 2: Create the directory for extracted files if it doesn't exist
os.makedirs(extracted_file_path, exist_ok=True)

# Step 3: Extract the password-protected zip file
zip_password = b'abdullah123'  # Replace with the actual password

with pyzipper.AESZipFile(local_zip_file_path) as zip_ref:
    zip_ref.pwd = zip_password  # Set the password
    zip_ref.extractall(extracted_file_path)

# Step 4: List the extracted files to verify
extracted_files = os.listdir(extracted_file_path)
print("Extracted Files: ", extracted_files)

# Step 5: Read the extracted CSV file into a Spark DataFrame
extracted_csv_file = os.path.join(extracted_file_path, 'Customer2.csv')  # Adjust the file name if necessary

# Reading the CSV from the local file path
df = spark.read.option("header", "true").csv(f"file:{extracted_csv_file}")

# Step 6: Display the DataFrame
df.show()
