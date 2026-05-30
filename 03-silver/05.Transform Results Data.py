# Databricks notebook source
# MAGIC %md
# MAGIC ### Transform Results Data<br>
# MAGIC 1.Read bronze `results` data<br>
# MAGIC 2.Keep only the columns required for analytics(Drop url column)<br>
# MAGIC 3.Standardise column names using snake_case(driverId -> driver_id, constructorId -> constructor_id, raceName -> race_name, positionText -> finish_position_text)<br>
# MAGIC 4.Rename columns to make them more meaningful(date -> race_date, grid -> grid_position, laps -> completed_laps, number -> car_number, position -> finish_position)<br>
# MAGIC 5.Filter out rows where season, round, constructor_id or driver_id is null(business key validation)<br>
# MAGIC 6.Remove duplicate records<br> 
# MAGIC 7.Transform values of column `race_name` to TitleCase<br>
# MAGIC 8.Write the transformed data to silver `results` table

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

# MAGIC %run ../00-common/03.silver-helpers

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.results"
silver_table = f"{catalog_name}.{silver_schema}.results"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 to 4 Read bronze `results` table, select only the required columns and standardise column names

# COMMAND ----------

results_df = (
  spark.table(bronze_table)
       .filter((F.col("batch_id") == v_batch_id))
       .select("season",
                "round",
                "constructorId",
                "driverId",
                "date",
                "raceName",
                "grid",
                "laps",
                "number",
                "points",
                "position",
                "positionText",
                "status",
                "ingestion_timestamp",
                "source_file",
                "batch_id")
       .withColumnsRenamed({
            "constructorId": "constructor_id",
            "driverId": "driver_id",
            "raceName": "race_name",
            "date": "race_date",
            "grid": "grid_position",
            "laps": "completed_laps",
            "number": "car_number",
            "position": "final_position",
            "positionText": "final_position_text"
        })
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 and 6 Apply data quality checks<br>
# MAGIC - Filter out rows where season, round, constructor_id or driver_id is null(business key validation)<br>
# MAGIC - Remove duplicate records

# COMMAND ----------

result_filtered = (
    results_df.filter(
        F.col("season").isNotNull() &
        F.col("round").isNotNull() &
        F.col("constructor_id").isNotNull() &
        F.col("driver_id").isNotNull()
    )
    .dropDuplicates(["season","round","constructor_id","driver_id"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 7 - Transform values of column `race_name` to TitleCase

# COMMAND ----------

results_final_df = (
    result_filtered
    .withColumn('race_name',F.initcap(F.col("race_name")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 8 - Write the Transformation data to silver `results` table

# COMMAND ----------

write_to_silver(
    input_df=results_final_df,
    target_table=silver_table,
    merge_condition="t.season = s.season AND t.round = s.round AND t.constructor_id = s.constructor_id AND t.driver_id = s.driver_id",
    columns_to_update=[
        "race_name",
        "race_date",
        "grid_position",
        "completed_laps",
        "car_number",
        "points",
        "final_position",
        "final_position_text",
        "status",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)