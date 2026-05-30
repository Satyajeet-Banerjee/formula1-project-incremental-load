# Databricks notebook source
# MAGIC %md
# MAGIC ### Transform Circuits Data<br>
# MAGIC 1.Read bronze circuits data<br>
# MAGIC 2.Keep only the columns required for analytics(Drop url column)<br>
# MAGIC 3.Standardise column names using snake_case(circuitId -> circuit_id, circuitName -> circuit_name)<br>
# MAGIC 4.Rename columns to make them more meaningful (lat -> latitude, long -> longitude)<br>
# MAGIC 5.Filter out rows where circuit_id is null(business key validation)<br>
# MAGIC 6.Remove duplicate records<br>
# MAGIC 7.Transform values of columns circuit_name and locality to TitleCase<br> 
# MAGIC 8.Write the transformed data to silver circuits table<br>

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

from pyspark.sql import functions as f

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1- Read Bronze Circuits Table

# COMMAND ----------

circuits_df = (
    spark.read.table(bronze_table).filter(f.col("batch_id") == v_batch_id)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2- Keep Only The Columns Required For Analytics(Drop Url Column)

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    f.col("circuitId"),
    f.col("circuitName"),
    f.col("lat"),
    f.col("long"),
    f.col("locality"),
    f.col("country"),
    f.col("ingestion_timestamp"),
    f.col("source_file"),
    f.col("batch_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 & 4 - Standardise Column Names<br>
# MAGIC - Standardise the column name using snake case<br>
# MAGIC - Rename columns to make them more meaningful

# COMMAND ----------

circuits_renamed_df = (
    circuits_selected_df
    .withColumnsRenamed({
        "circuitId": "circuit_id", 
        "circuitName": "circuit_name",
        "lat": "latitude", 
        "long": "longitude"})
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Filter out rows where circuit_id is null (business key validation)

# COMMAND ----------

circuits_valid_df = circuits_renamed_df.filter(
    f.col("circuit_id").isNotNull()
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6 - Remove duplicate records 

# COMMAND ----------

circuits_distinct_df = circuits_valid_df.dropDuplicates(["circuit_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 7 - Transform values of columns `circuit_name` and `locality` to Title case

# COMMAND ----------

circuits_final_df = (
    circuits_distinct_df
    .withColumn('circuit_name', f.initcap(f.col('circuit_name')))
    .withColumn('locality', f.initcap(f.col("locality")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 8 - Write the transformed data to silver `circuits` table

# COMMAND ----------

write_to_silver(
    input_df=circuits_final_df,
    target_table=silver_table,
    merge_condition="t.circuit_id = s.circuit_id",
    columns_to_update=[
        "circuit_name",
        "latitude",
        "longitude",
        "locality",
        "country",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)