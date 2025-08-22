# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import col, lit, regexp_extract

# COMMAND ----------

# DBTITLE 1,Ingestão Bronze

# Files list
csv_files = [
    "s3a://nyc-taxi-data-raul/bronze/yellow_tripdata_2015-01.csv",
    "s3a://nyc-taxi-data-raul/bronze/yellow_tripdata_2016-01.csv", 
    "s3a://nyc-taxi-data-raul/bronze/yellow_tripdata_2016-02.csv",
    "s3a://nyc-taxi-data-raul/bronze/yellow_tripdata_2016-03.csv"
]

# Read files into a Spark DataFrame
df_list = []
for file_path in csv_files:
    df_temp = spark.read.csv(file_path, header=True, inferSchema=False)

    # Add source_file and file_version columns
    df_temp = df_temp.withColumn("source_file", lit(file_path))
    df_temp = df_temp.withColumn("file_version", 
                                regexp_extract(lit(file_path), "yellow_tripdata_(.+)\\.csv", 1))
    df_list.append(df_temp)

# Joining all DataFrames
df_bronze = df_list[0]
for df in df_list[1:]:
    df_bronze = df_bronze.unionByName(df)

# Writing table
df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("nyc_taxi_analysis.bronze.nyc_taxi_data_extract")
