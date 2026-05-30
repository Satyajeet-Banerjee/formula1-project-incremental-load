# Databricks notebook source
# MAGIC %md
# MAGIC ### **Ingest Constructors.json file**<br>
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

# Define source file and table name
source_file = f"{landing_folder_path}/{v_batch_id}/constructors.json"
table_name = f"{catalog_name}.{bronze_schema}.constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1- Read the json file using the dataframe reader API

# COMMAND ----------

# Define the schema
constructors_schema = """constructorId STRING, 
                         name STRING, 
                         nationality STRING, 
                         url STRING"""

# COMMAND ----------

# Read the data from the constructors file
constructor_df =(
    spark
    .read
    .format('json')
    .schema(constructors_schema)
    .option('mode','FAILFAST')
    .load(source_file)
) 

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2- Add Metadata Columns
# MAGIC - Source file<br>
# MAGIC - Ingestion Timestamp

# COMMAND ----------

constructors_final_df = add_ingestion_metadata(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3- Write to bronze delta table

# COMMAND ----------

write_to_bronze(
    input_df=constructors_final_df,
    target_table=table_name,
    batch_id=v_batch_id
)