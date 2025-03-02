# Databricks notebook source
# Autoloader - 1. Process Gzip file , 2. Process PGP encrypted file , 3. Process Password protected file
# Binaryfile does not work for 1,2,3
# PGP encrypted files are to be decrypted before processing. 
# https://anupamchand.medium.com/pgp-encryption-using-python-in-azure-databricks-ef4bd56145ed
#
#
# Password Protect file to be opened with password and then processed. pywin32 pacakge is not available for databricks
# Library installation attempted on the driver node of cluster 0925-115452-5pvqzq98 and failed. Please refer to the following error message to fix the library or contact Databricks support. Error Code: DRIVER_LIBRARY_INSTALLATION_FAILURE. Error Message: org.apache.spark.SparkException: Process List(/databricks/python/bin/pip, install, pywin32, --disable-pip-version-check) exited with code 1. ERROR: Could not find a version that satisfies the requirement pywin32
# ERROR: No matching distribution found for pywin32
#
#
# Gzip - Working with following Code using CSV format 
# Next we need to cleanse data as we read thru autoloader so every Date comes in same format lets say DD-MON-YYYY, Telephone remains only 12 Numberic numbers (remove any ASCII char) and so on
# Year,Month,DayofMonth  remove any char and column must have only numbers 


# COMMAND ----------

# Creating a gz file.

dbutils.fs.ls("dbfs:/FileStore/airlines")
dbutils.fs.rm("dbfs:/FileStore/airlines_gz", True)
dbutils.fs.mkdirs("dbfs:/FileStore/airlines_gz")
# dbutils.fs.rm("dbfs:/FileStore/airlines_gz/test.csv", True)
h = dbutils.fs.head("dbfs:/FileStore/airlines/part-00000", 1040)
dbutils.fs.put("dbfs:/FileStore/airlines_gz/test.csv", h)
print(h)


# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC gzip /dbfs/FileStore/airlines_gz/test.csv
# MAGIC
# MAGIC ls /dbfs/FileStore/airlines_gz

# COMMAND ----------

schm = spark.read.csv("dbfs:/databricks-datasets/airlines/part-00000", header="true").schema

# COMMAND ----------

# reading a gz csv file with csv format is successful.

display(spark.read.csv("/FileStore/airlines_gz/", schema=schm).where("Year!='Year'"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- set spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite = true;
# MAGIC -- set spark.databricks.delta.properties.defaults.autoOptimize.autoCompact = true;

# COMMAND ----------

# spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
print(spark.conf.get("spark.databricks.delta.optimizeWrite.enabled"))
# spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
print(spark.conf.get("spark.databricks.delta.autoCompact.enabled"))

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# The long execution is because I have missed .trigger(availableNow=True), this runs forever.

tbl = "csv_gz_tbl"
file_path = "/FileStore/airlines_gz/*.csv.gz"
checkpoint = f"/tmp/lavan/csv_gz/{tbl}"

spark.sql(f"drop table if exists {tbl}")
dbutils.fs.rm(checkpoint, True)

(spark.readStream.format("cloudFiles") \
 .option("cloudFiles.format", "csv") \
 .option("header", "true") \
 .option("cloudFiles.schemaLocation", checkpoint) \
 .schema(schm) \
 .load(file_path) \
#  .filter(col("Year")!='Year') \
 .select("*", input_file_name().alias("filename"), current_timestamp().alias("processing_time"), "_metadata") \
 .writeStream \
 .format("delta") \
 .option("checkpointLocation", checkpoint) \
 .trigger(availableNow=True)
 .toTable(tbl)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- print(checkpoint)
# MAGIC select * from cloud_files_state('/tmp/lavan/csv_gz/csv_gz_tbl')

# COMMAND ----------

tbl = "csv_gz_tbl"
display(spark.table(tbl))
# spark.sql("drop table if exists "+tbl)

# COMMAND ----------

# Read Zip files
dbutils.fs.rm("dbfs:/FileStore/airlines_zip", True)
dbutils.fs.mkdirs("dbfs:/FileStore/airlines_zip")

spark.sql(f"select * from {tbl} limit 10").drop("_metadata").coalesce(1).write.mode("overwrite").option("header", "true").format("csv").save("dbfs:/FileStore/airlines_zip/test")



# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC # cd to folder path
# MAGIC cd /dbfs/FileStore/airlines_zip/
# MAGIC
# MAGIC # moving files to main path from test path
# MAGIC mv test/*.csv .
# MAGIC
# MAGIC # removing the test files
# MAGIC rm -r test/
# MAGIC
# MAGIC # Since zip operation on a dbfs file system is not allowed as below.
# MAGIC # ip I/O error: Operation not supported zip error: Input file read failure (was zipping dbfs/FileStore/
# MAGIC
# MAGIC # making directory
# MAGIC mkdir -p /tmp/lavan/airlines_zip/
# MAGIC
# MAGIC # copy files from /dbfs path to /tmp path accessible for zip operation.
# MAGIC cp *.csv /tmp/lavan/airlines_zip/
# MAGIC
# MAGIC # change directory to the tmp file.
# MAGIC cd /tmp/lavan/airlines_zip/
# MAGIC
# MAGIC # Zip file 
# MAGIC zip -m test.zip *.csv
# MAGIC
# MAGIC # move the zip file to dbfs path.
# MAGIC mv test.zip /dbfs/FileStore/airlines_zip/
# MAGIC
# MAGIC # remove the csv file from the dbfs path.
# MAGIC rm /dbfs/FileStore/airlines_zip/*.csv
# MAGIC
# MAGIC # remove the /tmp location data.
# MAGIC rm -r /tmp/lavan/
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC # List the file in dbfs
# MAGIC
# MAGIC ls /dbfs/FileStore/airlines_zip/

# COMMAND ----------

# Reading a zip/gzip file gives content as shown below

display(spark.read.format("binaryFile").option("header", "true").load("/FileStore/airlines_zip/"))

# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC # As per documentation of Databricks on Zip files.
# MAGIC # One have to unzip the files and then process with respective format read.
# MAGIC # https://docs.databricks.com/data/data-sources/zip-files.html
# MAGIC
# MAGIC file_path="/dbfs/FileStore/airlines_zip/"
# MAGIC
# MAGIC cd $file_path
# MAGIC
# MAGIC # Unzipping file
# MAGIC unzip /dbfs/FileStore/airlines_zip/test.zip && rm /dbfs/FileStore/airlines_zip/test.zip
# MAGIC ls /dbfs/FileStore/airlines_zip/
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/airlines_zip/"))

tbl_zip = "csv_zip_tbl"
file_path_zip = "/FileStore/airlines_zip/"
zip_checkpoint = f"/tmp/lavan/csv_zip/{tbl}"

spark.sql(f"drop table if exists {tbl_zip}")
dbutils.fs.rm(zip_checkpoint, True)

(spark.readStream.format("cloudFiles") \
.option("header", "true") \
.option("cloudFiles.schemaLocation", zip_checkpoint) \
.option("cloudFiles.validateOptions", "true") \
.option("cloudFiles.format", "csv") \
.load(file_path_zip) \
.select("*", "_metadata") \
.writeStream \
.format("delta") \
 .option("checkpointLocation", zip_checkpoint) \
 .option("ignoreMissingFiles", "true") \
 .trigger(availableNow=True) \
 .toTable(tbl_zip)
)


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from csv_zip_tbl

# COMMAND ----------


