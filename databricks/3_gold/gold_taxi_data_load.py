# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql.functions import col, year, month, dayofmonth, hour, dayofweek, quarter, when, monotonically_increasing_id

# COMMAND ----------

# DBTITLE 1,Carga Gold

# Read table from silver
df_silver = spark.read.table("nyc_taxi_analysis.silver.nyc_taxi_data_transform")

# Dimensions creation

# D_TIME (Table with all the time information)
df_time = df_silver.select(
    col("tpep_pickup_datetime").alias("timestamp"),
    year("tpep_pickup_datetime").alias("year"),
    month("tpep_pickup_datetime").alias("month"), 
    dayofmonth("tpep_pickup_datetime").alias("day"),
    hour("tpep_pickup_datetime").alias("hour"),
    dayofweek("tpep_pickup_datetime").alias("day_of_week"),
    quarter("tpep_pickup_datetime").alias("quarter"),
    when(dayofweek("tpep_pickup_datetime").isin(1, 7), True).otherwise(False).alias("is_weekend")
).distinct().withColumn("time_id", monotonically_increasing_id())

df_time.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("nyc_taxi_analysis.gold.d_time")

# D_VENDOR (Table with all the vendor information)
df_vendor = df_silver.select(
    col("VendorID").alias("vendor_id")
).distinct().withColumn("vendor_name", 
    when(col("vendor_id") == 1, "Creative Mobile Technologies")
    .when(col("vendor_id") == 2, "VeriFone Inc")
    .otherwise("Unknown")
)
df_vendor.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("nyc_taxi_analysis.gold.d_vendor")

# D_PAYMENT_TYPE (Table with all the payment type information)
df_payment = df_silver.select(
    col("payment_type").alias("payment_type_id")
).distinct().withColumn("payment_type_name",
    when(col("payment_type_id") == 1, "Credit card")
    .when(col("payment_type_id") == 2, "Cash")
    .when(col("payment_type_id") == 3, "No charge")
    .when(col("payment_type_id") == 4, "Dispute")
    .when(col("payment_type_id") == 5, "Unknown")
    .when(col("payment_type_id") == 6, "Voided trip")
    .otherwise("Other")
)
df_payment.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("nyc_taxi_analysis.gold.d_payment_type")

# Fact creation

df_time_lookup = spark.read.table("nyc_taxi_analysis.gold.d_time")
df_vendor_lookup = spark.read.table("nyc_taxi_analysis.gold.d_vendor")
df_payment_lookup = spark.read.table("nyc_taxi_analysis.gold.d_payment_type")

df_fact = df_silver \
    .join(df_time_lookup, 
          df_silver.tpep_pickup_datetime == df_time_lookup.timestamp, 
          "left") \
    .join(df_vendor_lookup, 
          df_silver.VendorID == df_vendor_lookup.vendor_id, 
          "left") \
    .join(df_payment_lookup, 
          df_silver.payment_type == df_payment_lookup.payment_type_id, 
          "left") \
    .select(
        col("time_id"),
        col("vendor_id"),
        col("payment_type_id"),
        col("passenger_count"),
        col("trip_distance"),
        col("fare_amount"),
        col("tip_amount"), 
        col("tolls_amount"),
        col("total_amount"),
        col("trip_duration_minutes"),
        col("trip_status"),
        col("pickup_latitude"),
        col("pickup_longitude"),
        col("dropoff_latitude"), 
        col("dropoff_longitude")
    )
df_fact.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("nyc_taxi_analysis.gold.f_trips")


# COMMAND ----------

# DBTITLE 1,Views de análise

# Time analysis view table
spark.sql("""
CREATE OR REPLACE VIEW nyc_taxi_analysis.gold.vw_trips_by_time AS
SELECT 
    t.year,
    t.month,
    t.hour,
    t.is_weekend,
    COUNT(*) as total_trips,
    AVG(f.trip_distance) as avg_distance,
    AVG(f.total_amount) as avg_revenue,
    AVG(f.trip_duration_minutes) as avg_duration
FROM nyc_taxi_analysis.gold.f_trips f
JOIN nyc_taxi_analysis.gold.d_time t ON f.time_id = t.time_id
GROUP BY t.year, t.month, t.hour, t.is_weekend
ORDER BY t.year, t.month, t.hour
""")

# Vendor analysis view table
spark.sql("""
CREATE OR REPLACE VIEW nyc_taxi_analysis.gold.vw_trips_by_vendor AS  
SELECT
    v.vendor_name,
    COUNT(*) as total_trips,
    AVG(f.trip_distance) as avg_distance,
    AVG(f.total_amount) as avg_revenue,
    AVG(f.trip_duration_minutes) as avg_duration
FROM nyc_taxi_analysis.gold.f_trips f
JOIN nyc_taxi_analysis.gold.d_vendor v ON f.vendor_id = v.vendor_id
GROUP BY v.vendor_name
ORDER BY total_trips DESC
""")

display(spark.sql("SELECT * FROM nyc_taxi_analysis.gold.vw_trips_by_time"))
display(spark.sql("SELECT * FROM nyc_taxi_analysis.gold.vw_trips_by_vendor"))