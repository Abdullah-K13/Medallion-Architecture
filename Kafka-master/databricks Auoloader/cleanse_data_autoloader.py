# Databricks notebook source
# we need to cleanse data as we read thru autoloader so every Date comes in same format lets say DD-MON-YYYY, Telephone remains only 12 Numberic numbers (remove any ASCII char) and so on
# Year,Month,DayofMonth  remove any char and column must have only numbers 

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/airlines_zip"))

# COMMAND ----------

tbl = "airlines_cleanse"
checkpoint = f"/tmp/lavan/cleanse/{tbl}/"
file_path = "/FileStore/airlines_zip"

# Filtering records
from pyspark.sql.types import *
from pyspark.sql.functions import *

(spark.readStream.format("cloudFiles") \
.option("header", "true") \
 .option("cloudFiles.format", "csv") \
 .option("cloudFiles.schemaLocation", checkpoint) \
 .option("cloudFiles.maxFilesPerTrigger", 1) \
 .option("cloudFiles.validateOptions", "true") \
 .load(file_path) \
 .select("*", "_metadata") \
 .filter(col("Year")!='Year') \
 .writeStream \
 .format("delta") \
 .option("checkpointLocation", checkpoint) \
 .trigger(availableNow=True) \
 .toTable(tbl)
)


# COMMAND ----------

display(spark.table(tbl))

# COMMAND ----------

# import Modules

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# Creating Autoloader_config_details table
config_headers = "process_id int, source_s3_bucket string, source_bucket_path string, source_file_format string, sink_delta_table string, sink_error_table string, udf_to_process_data string, max_latency_hrs int"

process_headers = "process_id int, file_name string, autoloader_start_dttm timestamp, autoloader_end_dttm timestamp, file_size bigint, no_of_records bigint, file_import_status string"

config_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), config_headers)
process_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), process_headers)

# COMMAND ----------

config_df.coalesce(1).write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable("Autoloader_config_details")
process_df.coalesce(1).write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable("Autoloader_processed_feeds")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- # insert config for flights
# MAGIC
# MAGIC delete from autoloader_config_details where process_id=1;
# MAGIC -- delete from Autoloader_processed_feeds where process_id=1;
# MAGIC
# MAGIC insert into table autoloader_config_details (process_id, source_s3_bucket, source_bucket_path, source_file_format, sink_delta_table, sink_error_table, udf_to_process_data, max_latency_hrs)
# MAGIC values(1, "dbfs", "/FileStore/airlines_zip", "csv", "tbl_airlines", 'tbl_airlines_error', Null, 1);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from Autoloader_config_details

# COMMAND ----------

# read the config and set parameters

config_read = spark.read.format("delta").table("Autoloader_config_details").filter(col("sink_delta_table")=="tbl_airlines").collect()[0].asDict()
print(config_read)

# COMMAND ----------

# Set variables

source_s3_bucket = config_read["source_s3_bucket"]
source_bucket_path = config_read["source_bucket_path"]
source_file_format = config_read["source_file_format"]
sink_delta_table = config_read["sink_delta_table"]
sink_error_table = config_read["sink_error_table"]
udf_to_process_data = config_read["udf_to_process_data"]
max_latency_hrs = config_read["max_latency_hrs"]
process_id = config_read["process_id"]

# print(source_s3_bucket)
# print(source_bucket_path)
# print(source_file_format)
# print(sink_delta_table)
# print(sink_error_table)
# print(udf_to_process_data)
# print(max_latency_hrs)

if source_s3_bucket is not None and source_bucket_path is not None:
    file_path = source_s3_bucket + ":" + source_bucket_path
elif source_s3_bucket is None and source_bucket_path is not None:
    file_path = source_bucket_path
else:
    print("No file path specified")
    exit()

print(file_path)


# COMMAND ----------

checkpoint = f"/tmp/lavan/_checkpoint/{sink_delta_table}"
start_dttm = spark.sql("select current_timestamp()").collect()[0][0]
print(start_dttm)

(spark.readStream.format("cloudFiles") \
 .option("cloudFiles.format", source_file_format) \
 .option("cloudFiles.schemaLocation", checkpoint) \
 .load(file_path) \
 .filter(col("Year")!='Year') \
 .select("*", input_file_name().alias("file_name"), current_timestamp().alias("c_timestamp"), "_metadata") \
 .writeStream \
 .format("delta") \
 .option("checkpointLocation", checkpoint) \
 .trigger(availableNow=True) \
 .toTable(sink_delta_table)
)

# COMMAND ----------

spark.table(sink_delta_table).select("_metadata.file_size").show()

# COMMAND ----------

# Insert into audit table

spark.sql(f"""insert into table Autoloader_processed_feeds
(process_id, file_name, autoloader_start_dttm, autoloader_end_dttm, file_size, no_of_records, file_import_status)
select '1', file_name, '{start_dttm}', current_timestamp(),
       max(_metadata.file_size), count(1), "success" 
from 
    {sink_delta_table} 
group by 
    file_name
""")

display(spark.table("Autoloader_processed_feeds"))
