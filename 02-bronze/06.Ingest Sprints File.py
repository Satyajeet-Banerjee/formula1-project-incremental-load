# Databricks notebook source
# MAGIC %md
# MAGIC ### **Ingest Sprints.json file**<br>
# MAGIC 1.Read the file using spark dataframe reader API<br>
# MAGIC 2.Define and enforce schema<br>
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

source_file = f"{landing_folder_path}/{v_batch_id}/sprints"
table_name = f"{catalog_name}.{bronze_schema}.sprints"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1-Read the json files using the dataframe reader API

# COMMAND ----------

# Define the schema
from pyspark.sql.types import DateType, StringType, IntegerType, DoubleType, StructType, StructField

sprints_schema = StructType([
    StructField('date', DateType()),
    StructField('raceName', StringType()),
    StructField('round', IntegerType()),
    StructField('season', IntegerType()),
    StructField('url', StringType()),
    StructField('constructorId', StringType()),
    StructField('driverId', StringType()),
    StructField('grid', IntegerType()),
    StructField('laps', IntegerType()),
    StructField('number', IntegerType()),
    StructField('points', DoubleType()),
    StructField('position', IntegerType()),
    StructField('positionText', StringType()),
    StructField('status', StringType()),

])

# COMMAND ----------

sprints_df = (
    spark.read
    .format('json')
    .option('header','true')
    .option('mode','FAILFAST')
    .schema(sprints_schema)
    .option('multiLine', True)
    .load(source_file)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2- Add Metadata Columns
# MAGIC - Source file<br>
# MAGIC - Ingestion Timestamp

# COMMAND ----------

sprints_final_df = add_ingestion_metadata(sprints_df)

# COMMAND ----------

write_to_bronze(
    input_df=sprints_final_df,
    target_table=table_name,
    batch_id=v_batch_id
)