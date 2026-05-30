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

# MAGIC %md
# MAGIC #### Step 1- Read Bronze Results Table

# COMMAND ----------

results_df = spark.read.table(bronze_table)

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2- Keep Only The Columns Required For Analytics(Drop Url Column)

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

results_dropped_df = results_df.drop("url")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 & 4 - Standardise Column Names<br>
# MAGIC - Standardise column names using snake_case(driverId -> driver_id, constructorId -> constructor_id, raceName -> race_name, positionText -> final_position_text)<br>
# MAGIC - Rename columns to make them more meaningful(date -> race_date, grid -> grid_position, laps -> completed_laps, number -> car_number, position -> final_position)

# COMMAND ----------

results_renamed_df = (
    results_dropped_df
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

)

# COMMAND ----------

display(results_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Filter out rows where season, round, constructor_id or driver_id is null(business key validation)

# COMMAND ----------

results_valid_df =(
    results_renamed_df
    .filter(
        F.col("season").isNotNull() &
        F.col("round").isNotNull() &
        F.col("constructor_id").isNotNull() &
        F.col("driver_id").isNotNull()
    )

)

# COMMAND ----------

display(results_renamed_df.count() - results_valid_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 6 - Remove duplicate records

# COMMAND ----------

results_distinct_df = results_valid_df.dropDuplicates(["season","round","constructor_id","driver_id"])

# COMMAND ----------

display(results_valid_df.count() - results_distinct_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 7 - Transformation values of column `race_name` to Title Case

# COMMAND ----------

results_final_df = (
    results_distinct_df
    .withColumn('race_name',F.initcap(F.col("race_name")))
)

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 8 - Write the Transformation data to silver `results` table

# COMMAND ----------

(
    results_final_df
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