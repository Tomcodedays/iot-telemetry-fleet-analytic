#!/usr/bin/env python
# coding: utf-8

# ## 02_Silver_to_Gold_IoT
# 
# New notebook

# In[11]:


from pyspark.sql.functions import (
    col, min, max, avg, sum, count, to_date, 
    row_number, when, stddev, percentile_approx,
    date_format, hour
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable


# In[12]:


# Configuración de tablas
silver_table_name = "iot_telemetry_silver_V2"  # Asegúrate que coincide con tu Silver
gold_current_table_name = "current_vehicle_state_V2"
gold_daily_summary_table_name = "daily_fleet_summary_V2"
gold_trends_table_name = "vehicle_trends_analysis"

print("Configuración de tablas:")
print(f"- Silver: {silver_table_name}")
print(f"- Gold (Estado Actual): {gold_current_table_name}")
print(f"- Gold (Resumen Diario): {gold_daily_summary_table_name}")
print(f"- Gold (Análisis de Tendencias): {gold_trends_table_name}")

# --- 1. CARGA DE DATOS SILVER CON VALIDACIÓN ---
def load_silver_data():
    """Carga y valida los datos de la capa Silver"""
    try:
        df_silver = spark.read.format("delta").table(silver_table_name)
        
        # Validación básica de datos
        if df_silver.count() == 0:
            raise ValueError("La tabla Silver está vacía")
        
        required_columns = ['device_id', 'event_timestamp', 'latitude', 'longitude', 
                          'speed_kmh', 'engine_temp_c', 'fuel_level_percent']
        for col_name in required_columns:
            if col_name not in df_silver.columns:
                raise ValueError(f"Columna requerida faltante: {col_name}")
        
        print(f"✅ Datos Silver cargados correctamente ({df_silver.count()} registros)")
        df_silver.printSchema()
        return df_silver
        
    except Exception as e:
        print(f"❌ Error al cargar datos Silver: {str(e)}")
        raise

df_silver = load_silver_data()
df_silver.limit(5).show()



# In[5]:


df_silver.show(2)


# In[13]:


# --- 2. ESTADO ACTUAL DE VEHÍCULOS (GOLD) ---
def process_current_state(df):
    """Procesa el estado más reciente de cada vehículo"""
    window_spec = Window.partitionBy("device_id").orderBy(col("event_timestamp").desc())
    
    df_current = df.withColumn("rn", row_number().over(window_spec)) \
                  .filter(col("rn") == 1) \
                  .select(
                      "device_id",
                      col("event_timestamp").alias("last_update_time"),
                      "latitude",
                      "longitude",
                      col("speed_kmh").alias("current_speed_kmh"),
                      col("engine_temp_c").alias("current_engine_temp_c"),
                      col("fuel_level_percent").alias("current_fuel_percent"),
                      col("door_open").alias("is_door_open"),
                      col("battery_voltage").alias("current_battery_voltage"),
                      when(col("engine_temp_c") > 90, "ALERTA").otherwise("Normal").alias("engine_status"),
                      when(col("oil_pressure_kpa") < 100, "ALERTA").otherwise("Normal").alias("oil_status")
                  )
    
    # Guardado optimizado
    df_current.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(gold_current_table_name)
    
    print(f"✅ Estado actual guardado en {gold_current_table_name}")
    return df_current

current_state_df = process_current_state(df_silver)
current_state_df.show()


# In[14]:


# --- 3. RESUMEN DIARIO DE FLOTA (GOLD) - VERSIÓN CORREGIDA ---
def process_daily_summary(df):
    """Genera métricas agregadas diarias por vehículo"""
    df_daily = df.groupBy(
        to_date(col("event_timestamp")).alias("event_date"), 
        col("device_id"),
        date_format(col("event_timestamp"), "EEEE").alias("day_of_week")
    ).agg(
        count("*").alias("message_count"),
        avg("speed_kmh").alias("avg_speed"),
        max("speed_kmh").alias("max_speed"),
        stddev("speed_kmh").alias("speed_stddev"),
        avg("engine_temp_c").alias("avg_engine_temp"),
        max("engine_temp_c").alias("max_engine_temp"),
        percentile_approx("engine_temp_c", 0.95).alias("p95_engine_temp"),
        avg("fuel_level_percent").alias("avg_fuel_level"),
        sum(when(col("speed_kmh") > 100, 1).otherwise(0)).alias("high_speed_events"),
        sum(when(col("door_open") == True, 1).otherwise(0)).alias("door_open_events"),
        (max("odometer_km") - min("odometer_km")).alias("daily_distance_km")
    ).orderBy("event_date", "device_id")
    
    # VERIFICACIÓN CORREGIDA PARA MICROSOFT FABRIC
    if spark.catalog.tableExists(gold_daily_summary_table_name):
        # Si la tabla existe, hacemos MERGE
        delta_table = DeltaTable.forName(spark, gold_daily_summary_table_name)
        delta_table.alias("target").merge(
            df_daily.alias("source"),
            "target.event_date = source.event_date AND target.device_id = source.device_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    else:
        # Si no existe, creamos la tabla
        df_daily.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(gold_daily_summary_table_name)
    
    print(f"✅ Resumen diario guardado en {gold_daily_summary_table_name}")
    return df_daily

daily_summary_df = process_daily_summary(df_silver)
daily_summary_df.show()


# In[15]:


# --- 4. ANÁLISIS DE TENDENCIAS (NUEVA TABLA GOLD) ---
def process_trends_analysis(df):
    """Analiza patrones temporales y de comportamiento"""
    # Análisis por hora del día
    df_trends = df.withColumn("hour_of_day", hour(col("event_timestamp"))) \
                 .groupBy("device_id", "hour_of_day") \
                 .agg(
                     avg("speed_kmh").alias("avg_speed_by_hour"),
                     count(when(col("speed_kmh") > 80, 1)).alias("high_speed_count"),
                     avg("fuel_level_percent").alias("avg_fuel_by_hour"),
                     count(when(col("door_open") == True, 1)).alias("door_open_count")
                 ) \
                 .orderBy("device_id", "hour_of_day")
    
    # Detección de patrones anómalos
    window_spec = Window.partitionBy("device_id").orderBy("hour_of_day")
    df_trends = df_trends.withColumn("speed_deviation", 
                                   col("avg_speed_by_hour") - avg("avg_speed_by_hour").over(window_spec))
    
    df_trends.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(gold_trends_table_name)
    
    print(f"✅ Análisis de tendencias guardado en {gold_trends_table_name}")
    return df_trends

trends_df = process_trends_analysis(df_silver)
trends_df.show()


# In[16]:


# --- MONITOREO FINAL CORREGIDO ---
from pyspark.sql.functions import min, max  # Asegúrate de importar estas funciones

print("\n" + "="*50)
print("Resumen de ejecución:")
print(f"- Registros procesados: {df_silver.count()}")
print(f"- Vehículos únicos: {df_silver.select('device_id').distinct().count()}")

# Versión corregida para el rango temporal
time_range = df_silver.agg(
    min(col("event_timestamp")).alias("min_time"),
    max(col("event_timestamp")).alias("max_time")
).collect()[0]

print(f"- Rango temporal: {time_range['min_time']} a {time_range['max_time']}")
print("="*50)

