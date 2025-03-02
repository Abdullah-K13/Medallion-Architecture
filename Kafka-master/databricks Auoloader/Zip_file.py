# This is our code which can prectly extract and run zip file
import zipfile
import os

files = dbutils.fs.ls("dbfs:/FileStore/Ziptest/Customer.zip")
local_zip_file_path = '/tmp/Customers.zip'  # Local path to store the copied zip file
extracted_file_path = '/tmp/extracted_files/'  # Local path where files will be extracted

# Step 2: Copy the zip file from DBFS to the local file system
dbutils.fs.cp('dbfs:/FileStore/Ziptest/Customer.zip', f"file:{local_zip_file_path}")


# Step 2: Unzip the file locally
with zipfile.ZipFile(local_zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(extracted_file_path)

extracted_csv_file = os.path.join(extracted_file_path, 'Customer.csv')  # Adjust to the correct file name

df = spark.read.option("header", "true").csv(f"file:{extracted_csv_file}")

# Display the DataFrame
df.show()


