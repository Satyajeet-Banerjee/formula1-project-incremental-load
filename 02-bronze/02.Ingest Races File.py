# Databricks notebook source
# MAGIC %md
# MAGIC ### **Ingest Races.csv file**<br>
# MAGIC 1.Read the file using spark dataframe reader API<br>
# MAGIC 2.Add Metadata Columns<br>
# MAGIC - Source file<br>
# MAGIC - Ingestion Timestamp<br>
# MAGIC
# MAGIC 3.Write to bronze delta table<br> 

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/02.bronze-helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/races.csv"
table_name = f"{catalog_name}.{bronze_schema}.races"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

races_schema = StructType([
    StructField('season', IntegerType()),
    StructField('round', IntegerType()),
    StructField('url', StringType()),
    StructField('raceName', StringType()),
    StructField('date', DateType()),
    StructField('circuitId', StringType()),
])

# COMMAND ----------

races_df = (
    spark.read
    .format('csv')
    .option('header','true')
    .option('mode','FAILFAST')
    .schema(races_schema)
    .load(source_file)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2- Add Metadata Columns
# MAGIC - Source file<br>
# MAGIC - Ingestion Timestamp

# COMMAND ----------

races_final_df = add_ingestion_metadata(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3- Write to bronze delta table

# COMMAND ----------

write_to_bronze(
    input_df = races_final_df,
    target_table = table_name,
    batch_id = v_batch_id
)