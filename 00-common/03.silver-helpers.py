# Databricks notebook source
from pyspark.sql import functions as f
from delta.tables import DeltaTable

def write_to_silver(
    input_df,
    target_table,
    merge_condition,
    columns_to_update
):
    """
    Creates the delta table if it does not exist.
    Otherwise merges the input DataFrame into the target table. 
    """
    final_df = (
        input_df
        .withColumn('updated_timestamp', f.current_timestamp())
        .withColumn('created_timestamp', f.current_timestamp())  
    )

    if not spark.catalog.tableExists(target_table):
        (
            final_df
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(target_table)
        )
    else:
        deltatable = DeltaTable.forName(spark, target_table)
        update_map = {column: f"s.{column}" for column in columns_to_update}
        update_map["updated_timestamp"] = "s.updated_timestamp"
        (
            deltatable.alias("t")
            .merge(
                final_df.alias("s"),
                merge_condition
            )
            .whenMatchedUpdate(
                condition="s.batch_id >= t.batch_id",
                set = update_map
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
        