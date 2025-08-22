# Databricks notebook source
# DBTITLE 1,Imports
import matplotlib.pyplot as plt

# COMMAND ----------

# DBTITLE 1,Análise gráfica da distribuição de corridas por hora do dia

hourly_distribution = spark.sql("""
SELECT 
    hour,
    COUNT(*) as trip_count,
    AVG(total_amount) as avg_revenue,
    AVG(trip_duration_minutes) as avg_duration
FROM nyc_taxi_analysis.gold.f_trips f
JOIN nyc_taxi_analysis.gold.d_time t ON f.time_id = t.time_id
GROUP BY hour
ORDER BY hour
""")

hourly_distribution_pd = hourly_distribution.toPandas()

plt.figure(figsize=(10, 6))
plt.bar(hourly_distribution_pd['hour'], hourly_distribution_pd['trip_count'], color='blue')
plt.title('Distribuição de Corridas por Hora do Dia')
plt.xlabel('Hora do Dia')
plt.ylabel('Número de Corridas')
plt.xticks(hourly_distribution_pd['hour'])
plt.grid(axis='y')
plt.show()


# COMMAND ----------

# DBTITLE 1,Análise gráfica da distribuição por tipo de pagamento

payment_distribution = spark.sql("""
SELECT 
    p.payment_type_name,
    COUNT(*) as trip_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM nyc_taxi_analysis.gold.f_trips), 2) as percentage,
    AVG(f.total_amount) as avg_amount
FROM nyc_taxi_analysis.gold.f_trips f
JOIN nyc_taxi_analysis.gold.d_payment_type p ON f.payment_type_id = p.payment_type_id
GROUP BY p.payment_type_name
ORDER BY trip_count DESC
""")

display(payment_distribution)

payment_distribution_pd = payment_distribution.toPandas()

plt.figure(figsize=(8, 8))
plt.pie(payment_distribution_pd['percentage'], labels=payment_distribution_pd['payment_type_name'], autopct='%1.1f%%', startangle=140)
plt.title('Distribuição por Tipo de Pagamento')
plt.axis('equal') 
plt.show()


# COMMAND ----------

# DBTITLE 1,Análise gráfica ajustada da distribuição por tipo de pagamento

payment_distribution_adjusted = spark.sql("""
SELECT 
    CASE 
        WHEN p.payment_type_name IN ('No charge', 'Dispute', 'Unknown') THEN 'Others'
        ELSE p.payment_type_name
    END AS payment_type_name,
    COUNT(*) as trip_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM nyc_taxi_analysis.gold.f_trips), 2) as percentage,
    AVG(f.total_amount) as avg_amount
FROM nyc_taxi_analysis.gold.f_trips f
JOIN nyc_taxi_analysis.gold.d_payment_type p ON f.payment_type_id = p.payment_type_id
GROUP BY 
    CASE 
        WHEN p.payment_type_name IN ('No charge', 'Dispute', 'Unknown') THEN 'Others'
        ELSE p.payment_type_name
    END
ORDER BY trip_count DESC
""")

display(payment_distribution_adjusted)

payment_distribution_adj_pd = payment_distribution_adjusted.toPandas()

plt.figure(figsize=(8, 8))
plt.pie(payment_distribution_adj_pd['percentage'], labels=payment_distribution_adj_pd['payment_type_name'], autopct='%1.1f%%', startangle=140)
plt.title('Distribuição por Tipo de Pagamento')
plt.axis('equal')
plt.show()


# COMMAND ----------

# DBTITLE 1,Análise gráfica por vendor

vendor_analysis = spark.sql("""
SELECT 
    v.vendor_name,
    COUNT(*) as trip_count,
    AVG(f.trip_distance) as avg_distance,
    AVG(f.total_amount) as avg_revenue,
    AVG(f.trip_duration_minutes) as avg_duration
FROM nyc_taxi_analysis.gold.f_trips f
JOIN nyc_taxi_analysis.gold.d_vendor v ON f.vendor_id = v.vendor_id
GROUP BY v.vendor_name
ORDER BY trip_count DESC
""")

vendor_analysis_pd = vendor_analysis.toPandas()

plt.figure(figsize=(10, 6))
plt.bar(vendor_analysis_pd['vendor_name'], vendor_analysis_pd['trip_count'], color='green')
plt.title('Market Share por Vendor')
plt.xlabel('Vendor/Viação')
plt.ylabel('Número de Corridas')
#plt.xticks(rotation=45)
plt.grid(axis='y')


# COMMAND ----------

# DBTITLE 1,Análise gráfica de corridas por dia da semana

weekday_analysis = spark.sql("""
SELECT 
    CASE 
        WHEN t.day_of_week = 1 THEN 'Domingo'
        WHEN t.day_of_week = 2 THEN 'Segunda'
        WHEN t.day_of_week = 3 THEN 'Terça'
        WHEN t.day_of_week = 4 THEN 'Quarta'
        WHEN t.day_of_week = 5 THEN 'Quinta'
        WHEN t.day_of_week = 6 THEN 'Sexta'
        WHEN t.day_of_week = 7 THEN 'Sábado'
    END as weekday,
    COUNT(*) as trip_count,
    AVG(f.total_amount) as avg_revenue,
    AVG(f.trip_duration_minutes) as avg_duration
FROM nyc_taxi_analysis.gold.f_trips f
JOIN nyc_taxi_analysis.gold.d_time t ON f.time_id = t.time_id
GROUP BY t.day_of_week
ORDER BY t.day_of_week
""")

weekday_analysis_pd = weekday_analysis.toPandas()

plt.figure(figsize=(10, 6))
plt.plot(weekday_analysis_pd['weekday'], weekday_analysis_pd['trip_count'], marker='o', color='blue')
plt.title('Volume de Corridas por Dia da Semana')
plt.xlabel('Dia da Semana')
plt.ylabel('Número de Corridas')
plt.xticks(weekday_analysis_pd['weekday'])
plt.grid()
plt.tight_layout()
plt.show()


# COMMAND ----------

# DBTITLE 1,Análise gráfica da distância vs Valor

distance_vs_value = spark.sql("""
SELECT 
    ROUND(trip_distance, 1) as distance_km,
    COUNT(*) as trip_count,
    AVG(total_amount) as avg_amount,
    AVG(total_amount / NULLIF(trip_distance, 0)) as avg_price_per_km
FROM nyc_taxi_analysis.gold.f_trips
WHERE trip_distance BETWEEN 1 AND 50
GROUP BY ROUND(trip_distance, 1)
HAVING COUNT(*) > 10
ORDER BY distance_km
""")

distance_vs_value_pd = distance_vs_value.toPandas()

plt.figure(figsize=(10, 6))
plt.scatter(distance_vs_value_pd['distance_km'], distance_vs_value_pd['avg_amount'], color='purple')
plt.title('Correlação: Distância vs Valor da Corrida')
plt.xlabel('Distância')
plt.ylabel('Valor Médio da Corrida')
plt.grid()

plt.tight_layout()
plt.show()
