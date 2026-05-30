# Databricks notebook source
# MAGIC %md
# MAGIC ### Transform Constructors Data<br>
# MAGIC 1.Read bronze constructors data<br>
# MAGIC 2.Keep only the columns required for analytics(Drop url column)<br>
# MAGIC 3.Standardise column names using snake_case(constructorId -> constructor_id)<br>
# MAGIC 4.Rename columns to make them more meaningful (name -> constructor_name)<br>
# MAGIC 5.Remove duplicate records<br>
# MAGIC 6.Transform values of column `nationality` to TitleCase<br> 
# MAGIC 7.Write the transformed data to silver constructors table<br>

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id") 

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1- Read Constructors Table

# COMMAND ----------

constructors_df = (
    spark.table(bronze_table)
         .filter((F.col("batch_id") == v_batch_id))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2- Keep Only The Columns Required For Analytics(Drop Url Column)

# COMMAND ----------

constructors_dropped_df = constructors_df.drop("url")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 & 4 - Standardise Column Names<br>
# MAGIC - Standardise the column name using snake case<br>
# MAGIC - Rename columns to make them more meaningful

# COMMAND ----------

constructors_renamed_df = (
    constructors_dropped_df
    .withColumnsRenamed({
        "constructorId": "constructors_id", 
        "name": "constructor_name"
    })

)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Remove duplicate records

# COMMAND ----------

constructors_distinct_df = constructors_renamed_df.dropDuplicates(["constructors_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6 - Transformation values of column `nationality` to Title Case

# COMMAND ----------

constructors_final_df = (
    constructors_distinct_df
    .withColumn('nationality',F.initcap(F.col("nationality")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 7 - Write the Transformation data to silver Constructors table

# COMMAND ----------

write_to_silver(
    input_df=constructors_final_df,
    target_table=silver_table,
    merge_condition="t.constructors_id = s.constructors_id",
    columns_to_update=[
        "constructor_name",
        "nationality",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)