# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import col, when, hour, to_timestamp, round, coalesce, lit, regexp_extract

# COMMAND ----------


# Read table from bronze layer
df_bronze = spark.read.table("nyc_taxi_analysis.bronze.nyc_taxi_data_extract")

# Conversion of numeric columns
numeric_columns = ["passenger_count", "RatecodeID", "payment_type", "trip_distance",
                  "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
                  "improvement_surcharge", "total_amount"]

df_silver = df_bronze

number_regex = "^-?\\d+(\\.\\d+)?$"

for col_name in numeric_columns:
    df_silver = df_silver.withColumn(
        col_name,
        when(
            col(col_name).rlike(number_regex),
            col(col_name).cast("double")
        ).otherwise(lit(0.0))
    )

# Conversion of datetime columns
df_silver = df_silver.withColumn(
    "tpep_pickup_datetime", 
    coalesce(
        to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col("tpep_pickup_datetime"), "MM/dd/yyyy HH:mm:ss"),
        to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd"),
        lit(None)
    )
).withColumn(
    "tpep_dropoff_datetime", 
    coalesce(
        to_timestamp(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(col("tpep_dropoff_datetime"), "MM/dd/yyyy HH:mm:ss"),
        to_timestamp(col("tpep_dropoff_datetime"), "yyyy-MM-dd"),
        lit(None)
    )
)

# Appliying quality filters
df_silver = df_silver.filter(
    col("tpep_pickup_datetime").isNotNull() &
    col("tpep_dropoff_datetime").isNotNull() &
    (col("trip_distance") > 0) & 
    (col("trip_distance") < 100) &
    (col("fare_amount") > 0) &
    (col("fare_amount") < 500) &
    (col("total_amount") > 0) &
    (col("total_amount") < 1000) &
    (col("passenger_count") > 0) &
    (col("passenger_count") <= 6)
)

# Dropping duplicates
df_silver = df_silver.dropDuplicates()

# Adding enrichment columns
df_silver = df_silver.withColumn(
    "trip_duration_minutes",
    round((col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long")) / 60, 2)
).withColumn(
    "pickup_hour", 
    hour(col("tpep_pickup_datetime"))
).withColumn(
    "trip_status",
    when(col("trip_duration_minutes") > 180, "VERY_LONG")
    .when(col("trip_distance") < 1, "VERY_SHORT")
    .when(col("total_amount") > 200, "EXPENSIVE")
    .otherwise("NORMAL")
)

# Writing table
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("nyc_taxi_analysis.silver.nyc_taxi_data_transform")

# Statistics
print(f"Number of registers before treatment: {df_bronze.count():,}")
print(f"Number of registers after treatment: {df_silver.count():,}")
print(f"Registers removed: {df_bronze.count() - df_silver.count():,}")


# COMMAND ----------

# MAGIC %md
# MAGIC