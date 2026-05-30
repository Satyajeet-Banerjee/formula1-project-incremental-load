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

# MAGIC %run ../00-common/01.environment-config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.results"
silver_table = f"{catalog_name}.{silver_schema}.results"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 to 7 Read, Transform, & Perform data quality checks

# COMMAND ----------

results_df = (
    spark.read.table(bronze_table)
    .drop("url")
    .withColumnsRenamed({
        "driverId": "driver_id", 
        "constructorId": "constructor_id",
        "raceName" : "race_name",
        "positionText" : "final_position_text",
        "date" : "race_date",
        "grid" : "grid_position",
        "laps" : "completed_laps",
        "number" : "car_number",
        "position" : "final_position",
        
    })
    .filter(
        F.col("season").isNotNull() &
        F.col("round").isNotNull() &
        F.col("constructor_id").isNotNull() &
        F.col("driver_id").isNotNull()
    )
    .dropDuplicates(["season","round","constructor_id","driver_id"])
    .withColumn('race_name',F.initcap(F.col("race_name")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 8 - Write the Transformation data to silver `results` table

# COMMAND ----------

(
    results_df
    .write
    .mode("overwrite")
    .format("delta")
    .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))

# COMMAND ----------

# MAGIC %md
# MAGIC