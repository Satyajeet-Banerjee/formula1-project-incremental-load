# Databricks notebook source
# MAGIC %md
# MAGIC #### Build Constructors Dimension
# MAGIC 1.Read silver `constructors` table<br>
# MAGIC 2.Read gold ref_nationality_region table<br>
# MAGIC 3.Join the data from `constructors` with `ref_nationality_region` using nationality<br>
# MAGIC 4.Select the required columns<br>
# MAGIC - constructors.constructor_id
# MAGIC - constructors.constructor_name
# MAGIC - constructors.nationality
# MAGIC - ref_nationality_region.region
# MAGIC
# MAGIC 5.Write the transformed data to good dim_constructors table

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/04.gold-helpers

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_constructors"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 - Read source tables
# MAGIC - `silver.constructors`
# MAGIC - `gold.ref_nationality_region`

# COMMAND ----------

constructors_df = (
    spark.table(f"{catalog_name}.{silver_schema}.constructors")
         .filter(F.col("batch_id") == v_batch_id)
)

ref_nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Join `constructors` with `nationality_region_df` using `nationality`
# MAGIC Select the following columns   
# MAGIC 1. constructors.constructor_id 
# MAGIC 1. constructors.constructor_name 
# MAGIC 1. constructors.nationality 
# MAGIC 1. ref_nationality_region.region

# COMMAND ----------

dim_constructors_df = (
    constructors_df
        .join(
            ref_nationality_region_df,
            constructors_df.nationality == ref_nationality_region_df.nationality,
            "left"
            )
        .select (
            constructors_df.constructors_id,
            constructors_df.constructor_name,
            constructors_df.nationality,
            ref_nationality_region_df.region.alias("nationality_region")
        )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Write the transformed data to the gold `dim_constructors` table

# COMMAND ----------

write_to_gold(
    input_df=dim_constructors_df,
    target_table=target_table,
    merge_condition="t.constructors_id = s.constructors_id",
    columns_to_update=[
        "constructor_name",
        "nationality",
        "nationality_region"
    ]
)