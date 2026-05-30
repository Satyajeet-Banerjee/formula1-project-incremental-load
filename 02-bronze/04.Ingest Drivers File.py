# Databricks notebook source
# MAGIC %md
# MAGIC ### **Ingest Drivers.json file**<br>
# MAGIC 1.Read the file using spark dataframe reader API<br>
# MAGIC 2.Define and enforce schema(preserve the nested structure)<br>
# MAGIC 3.Add Metadata Columns<br>
# MAGIC - Source file<br>
# MAGIC - Ingestion Timestamp<br>
# MAGIC
# MAGIC 4.Write to bronze delta table<br>

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

# Define source file and table name
source_file = f"{landing_folder_path}/{v_batch_id}/drivers.json"
table_name = f"{catalog_name}.{bronze_schema}.drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1- Read the json file using the dataframe reader API

# COMMAND ----------

# Define the schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

name_schema = StructType([
    StructField('givenName', StringType()),
    StructField('familyName', StringType())
])

drivers_schema = StructType([
    StructField('driverId', StringType()),
    StructField('name', name_schema),
    StructField('dateOfBirth', DateType()),
    StructField('nationality', StringType()),
    StructField('url', StringType())
])

# COMMAND ----------

# Read data from the drivers file
drivers_df =(
    spark
    .read
    .format('json')
    .schema(drivers_schema)
    .option('mode','FAILFAST')
    .load(source_file)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2- Add Metadata Columns
# MAGIC - Source file<br>
# MAGIC - Ingestion Timestamp

# COMMAND ----------

drivers_final_df = add_ingestion_metadata(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3- Write to bronze delta table

# COMMAND ----------

write_to_bronze(
    input_df = drivers_final_df,
    target_table = table_name,
    batch_id = v_batch_id
)