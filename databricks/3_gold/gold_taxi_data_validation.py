# Databricks notebook source
# DBTITLE 1,Integridade Bronze vs Silver

validation_silver = spark.sql("""
WITH bronze_stats AS (
    SELECT 
        COUNT(*) as total_bronze,
        COUNT(DISTINCT source_file) as files_count
    FROM nyc_taxi_analysis.bronze.nyc_taxi_data_extract
),
silver_stats AS (
    SELECT COUNT(*) as total_silver
    FROM nyc_taxi_analysis.silver.nyc_taxi_data_transform
)
SELECT 
    b.total_bronze,
    s.total_silver,
    b.files_count,
    (b.total_bronze - s.total_silver) as records_lost,
    ROUND(((b.total_bronze - s.total_silver) * 100.0 / b.total_bronze), 2) as loss_percentage,
    (b.total_bronze - s.total_silver) = 0 as no_data_loss
FROM bronze_stats b, silver_stats s
""")

display(validation_silver)


# COMMAND ----------

# DBTITLE 1,Integridade Silver vs Gold

validation_gold = spark.sql("""
WITH silver_count AS (
    SELECT COUNT(*) as total_silver
    FROM nyc_taxi_analysis.silver.nyc_taxi_data_transform
),
fact_count AS (
    SELECT COUNT(*) as total_fact
    FROM nyc_taxi_analysis.gold.f_trips
),
dimension_checks AS (
    SELECT 
        (SELECT COUNT(*) FROM nyc_taxi_analysis.gold.d_time) as time_count,
        (SELECT COUNT(*) FROM nyc_taxi_analysis.gold.d_vendor) as vendor_count,
        (SELECT COUNT(*) FROM nyc_taxi_analysis.gold.d_payment_type) as payment_count
)
SELECT 
    s.total_silver,
    f.total_fact,
    d.time_count,
    d.vendor_count,
    d.payment_count,
    (s.total_silver - f.total_fact) as records_lost,
    CASE 
        WHEN s.total_silver = f.total_fact THEN 'OK - No data loss'
        ELSE 'WARNING - Data lost in transformation'
    END as validation_status
FROM silver_count s, fact_count f, dimension_checks d
""")

display(validation_gold)


# COMMAND ----------

# DBTITLE 1,Validação de valores aberrantes

quality_check = spark.sql("""
SELECT 
    'Total Records' as metric,
    COUNT(*) as value
FROM nyc_taxi_analysis.gold.f_trips

UNION ALL

SELECT 
    'Records with Negative Amount' as metric,
    COUNT(*) as value
FROM nyc_taxi_analysis.gold.f_trips
WHERE total_amount < 0

UNION ALL

SELECT 
    'Records with Zero Distance' as metric, 
    COUNT(*) as value
FROM nyc_taxi_analysis.gold.f_trips
WHERE trip_distance = 0

UNION ALL

SELECT
    'Records with Excessive Duration (>6 hours)' as metric,
    COUNT(*) as value  
FROM nyc_taxi_analysis.gold.f_trips
WHERE trip_duration_minutes > 360

UNION ALL

SELECT
    'Records with Excessive Amount (>$500)' as metric,
    COUNT(*) as value
FROM nyc_taxi_analysis.gold.f_trips
WHERE total_amount > 500

UNION ALL

SELECT
    'Null Values in Critical Columns' as metric,
    SUM(CASE WHEN passenger_count IS NULL THEN 1 ELSE 0 END +
        CASE WHEN trip_distance IS NULL THEN 1 ELSE 0 END +
        CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END) as value
FROM nyc_taxi_analysis.gold.f_trips
""")

display(quality_check)

# COMMAND ----------

# DBTITLE 1,Validação de chaves e integridade referencial

referential_integrity = spark.sql("""
SELECT 
    'Fact trips without time reference' as check_type,
    COUNT(*) as invalid_records
FROM nyc_taxi_analysis.gold.f_trips f
LEFT JOIN nyc_taxi_analysis.gold.d_time t ON f.time_id = t.time_id
WHERE t.time_id IS NULL

UNION ALL

SELECT
    'Fact trips without vendor reference',
    COUNT(*)  
FROM nyc_taxi_analysis.gold.f_trips f
LEFT JOIN nyc_taxi_analysis.gold.d_vendor v ON f.vendor_id = v.vendor_id
WHERE v.vendor_id IS NULL

UNION ALL

SELECT
    'Fact trips without payment type reference', 
    COUNT(*)
FROM nyc_taxi_analysis.gold.f_trips f
LEFT JOIN nyc_taxi_analysis.gold.d_payment_type p ON f.payment_type_id = p.payment_type_id
WHERE p.payment_type_id IS NULL
""")

display(referential_integrity)
