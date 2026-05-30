# Databricks notebook source
# MAGIC %md
# MAGIC ### **Ingest Circuits.csv file**<br>
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

source_file = f"{landing_folder_path}/{v_batch_id}/circuits.csv"
table_name = f"{catalog_name}.{bronze_schema}.circuits"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1 - Read the csv files using the dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

circuits_schema = StructType([
    StructField('circuitId', StringType()),
    StructField('url', StringType()),
    StructField('circuitName', StringType()),
    StructField('lat', DoubleType()),
    StructField('long', DoubleType()),
    StructField('locality', StringType()),
    StructField('country', StringType())
])

# COMMAND ----------

circuits_df = (
    spark.read
    .format('csv')
    .option('header','true')
    .option('mode','FAILFAST')
    .schema(circuits_schema)
    .load(source_file)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2- Add Metadata Columns
# MAGIC - Source file<br>
# MAGIC - Ingestion Timestamp

# COMMAND ----------

circuits_final_df = add_ingestion_metadata(circuits_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3- Write to bronze delta table

# COMMAND ----------

write_to_bronze(
    input_df = circuits_final_df,
    target_table = table_name,
    batch_id = v_batch_id
)