#!/usr/bin/env python
# coding: utf-8

# ## iot_data_processing_bronze_to_silver
# 
# New notebook

# In[1]:


from pyspark.sql.functions import col, from_json, current_timestamp, unbase64, from_unixtime, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType, LongType, IntegerType # Asegúrate de tener IntegerType para oil_pressure y engine_rpm


# In[2]:


bronze_table_name = "iot_raw_telemetry_V2"
silver_table_name = "iot_telemetry_silver_V2"
checkpoint_location_silver = f"Files/checkpoints/{silver_table_name}"


# In[3]:


# --- LECTURA Y TRANSFORMACIÓN DE BRONZE A SILVER ---
df_bronze_stream = spark.readStream \
                         .format("delta") \
                         .option("ignoreChanges", "true") \
                         .table(bronze_table_name)
                        
print("Schema de Bronze:")
df_bronze_stream.printSchema()


# In[4]:


df_silver_transformed = df_bronze_stream.select(
    col("deviceId").alias("device_id"),
    col("timestamp").alias("device_timestamp"),
    col("latitude"),
    col("longitude"),
    col("speed_kmh"),
    col("engine_temp_c"),
    col("vibration_level"),
    col("fuel_level_percent"),
    col("door_open"),
    col("fuel_type"),
    col("battery_voltage"),
    col("oil_pressure_kpa"),
    col("engine_rpm"),
    col("odometer_km"),
    col("EventEnqueuedUtcTime").alias("enqueued_time"),
    col("EventProcessedUtcTime").alias("processed_time")
).withColumn(
    # Convertir timestamp de milisegundos a datetime
    "event_timestamp", 
    from_unixtime(col("device_timestamp") / 1000).cast(TimestampType())
).withColumn(
    # Agregar fecha para particiones
    "event_date", 
    to_date(col("event_timestamp"))
).withColumn(
    # Timestamp de procesamiento en Silver
    "silver_processed_time", 
    current_timestamp()
)


# In[5]:


query_silver = df_silver_transformed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_location_silver) \
    .option("mergeSchema", "true") \
    .trigger(processingTime="1 minute") \
    .toTable(silver_table_name)

print(f"Streaming desde '{bronze_table_name}' a '{silver_table_name}' iniciado.")


# In[ ]:




