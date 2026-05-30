# Databricks notebook source
# Helper function to add the file metadata for ingestion (source file and ingestion timestamp)
from pyspark.sql import functions as f

def add_ingestion_metadata(df):
    return (
        df.withColumn('ingestion_timestamp', f.current_timestamp())
          .withColumn('source_file', f.col('_metadata.file_path'))
    )


# COMMAND ----------

def write_to_bronze(
    input_df,
    target_table,
    batch_id
):
    final_df = input_df.withColumn(f"batch_id",f.lit(batch_id))
    (
        final_df
        .write
        .format('delta')
        .mode('overwrite')
        .partitionBy('batch_id')
        .option('replaceWhere',f"batch_id = '{batch_id}'")
        .saveAsTable(target_table)
    )

# COMMAND ----------

