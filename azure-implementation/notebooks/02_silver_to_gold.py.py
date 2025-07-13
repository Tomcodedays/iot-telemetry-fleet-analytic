# Databricks notebook source
# Notebook: iot_data_processing_silver_to_gold


from pyspark.sql.functions import *
from pyspark.sql.types import *
import json


storage_account_name = "iotstorageadls"
scope_name = "iot-secrets-scope"


try:
    client_id = dbutils.secrets.get(scope=scope_name, key="client-id")
    client_secret = dbutils.secrets.get(scope=scope_name, key="client-secret")
    tenant_id = dbutils.secrets.get(scope=scope_name, key="tenant-id")

    # Configurar Spark para autenticarse con ADLS Gen2 usando la Entidad de Servicio
    spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

    print("\n¡Conexión a ADLS Gen2 establecida para el Notebook Gold!")

except Exception as e:
    print(f"\nError al acceder a los secretos o establecer la conexión: {e}")
    print("Por favor, verifica la configuración de tus secretos y permisos en el Key Vault y en el Service Principal.")



silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/iot_fleet_data"
print(f"Ruta de la capa Silver configurada: {silver_path}")

# Ruta base donde se guardarán los datos Gold (puedes crear subcarpetas para diferentes vistas Gold)
gold_base_path = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/"
print(f"Ruta base de la capa Gold configurada: {gold_base_path}")

# COMMAND ----------

# Cargar el DataFrame de la capa Silver
try:
    df_silver = spark.read.format("delta").load(silver_path)
    print(f"DataFrame Silver cargado con {df_silver.count()} registros.")
    df_silver.printSchema()
    df_silver.limit(5).display()
except Exception as e:
    print(f"ERROR al cargar datos de la capa Silver: {e}")
    print(f"Verifica que los datos Delta existen en la ruta: {silver_path}")
    print("Asegúrate de haber ejecutado el Notebook de Bronze a Silver y que haya guardado datos.")

# COMMAND ----------

# Registrar el DataFrame Silver como una vista temporal para poder consultarlo con SQL
df_silver.createOrReplaceTempView("iot_fleet_data_silver")
print("Vista temporal 'iot_fleet_data_silver' creada.")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold;
# MAGIC -- O, si estás en Unity Catalog y quieres especificar el catálogo:
# MAGIC -- CREATE DATABASE IF NOT EXISTS <your_catalog_name>.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT deviceId, COUNT(*) as total_messages FROM iot_fleet_data_silver GROUP BY deviceId ORDER BY deviceId;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.current_vehicle_state AS
# MAGIC WITH latest_readings AS (
# MAGIC     SELECT
# MAGIC         deviceId,
# MAGIC         eventTimestamp,
# MAGIC         latitude,
# MAGIC         longitude,
# MAGIC         speed_kmh,
# MAGIC         engine_temp_c,
# MAGIC         vibration_level,
# MAGIC         fuel_level_percent,
# MAGIC         door_open,
# MAGIC         fuel_type,
# MAGIC         battery_voltage,
# MAGIC         oil_pressure_kpa,
# MAGIC         engine_rpm,
# MAGIC         odometer_km,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY deviceId ORDER BY eventTimestamp DESC) as rn
# MAGIC     FROM delta.`abfss://silver@iotstorageadls.dfs.core.windows.net/iot_fleet_data`
# MAGIC )
# MAGIC SELECT
# MAGIC     deviceId,
# MAGIC     eventTimestamp,
# MAGIC     latitude,
# MAGIC     longitude,
# MAGIC     speed_kmh,
# MAGIC     engine_temp_c,
# MAGIC     vibration_level,
# MAGIC     fuel_level_percent,
# MAGIC     door_open,
# MAGIC     fuel_type,
# MAGIC     battery_voltage,
# MAGIC     oil_pressure_kpa,
# MAGIC     engine_rpm,
# MAGIC     odometer_km
# MAGIC FROM latest_readings
# MAGIC WHERE rn = 1;
# MAGIC
# MAGIC -- Verificar la vista Gold
# MAGIC SELECT * FROM gold.current_vehicle_state;

# COMMAND ----------

current_vehicle_state_df = spark.sql("SELECT * FROM gold.current_vehicle_state")

gold_current_state_path = f"{gold_base_path}/current_vehicle_state"

current_vehicle_state_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(gold_current_state_path)

print(f"Tabla Gold 'current_vehicle_state' guardada en: {gold_current_state_path}")

spark.read.format("delta").load(gold_current_state_path).limit(5).display()

# COMMAND ----------

num_filas = current_vehicle_state_df.count()
print(f"El número de filas es: {num_filas}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.daily_fleet_summary AS
# MAGIC SELECT
# MAGIC     CAST(eventTimestamp AS DATE) as eventDate,
# MAGIC     deviceId,
# MAGIC     COUNT(*) as total_messages,
# MAGIC     AVG(speed_kmh) as avg_speed_kmh,
# MAGIC     MAX(speed_kmh) as max_speed_kmh,
# MAGIC     AVG(engine_temp_c) as avg_engine_temp_c,
# MAGIC     MAX(engine_temp_c) as max_engine_temp_c,
# MAGIC     AVG(vibration_level) as avg_vibration_level,
# MAGIC     MAX(vibration_level) as max_vibration_level,
# MAGIC     AVG(fuel_level_percent) as avg_fuel_level_percent,
# MAGIC     MIN(fuel_level_percent) as min_fuel_level_percent,
# MAGIC     SUM(CASE WHEN door_open = TRUE THEN 1 ELSE 0 END) as total_door_open_events,
# MAGIC     AVG(battery_voltage) as avg_battery_voltage,
# MAGIC     MIN(battery_voltage) as min_battery_voltage,
# MAGIC     AVG(oil_pressure_kpa) as avg_oil_pressure_kpa,
# MAGIC     MIN(oil_pressure_kpa) as min_oil_pressure_kpa,
# MAGIC     AVG(engine_rpm) as avg_engine_rpm,
# MAGIC     MAX(engine_rpm) as max_engine_rpm,
# MAGIC     MAX(odometer_km) as odometer_km_end,
# MAGIC     MIN(odometer_km) as odometer_km_start,
# MAGIC     (MAX(odometer_km) - MIN(odometer_km)) as distance_traveled_km,
# MAGIC     FIRST(fuel_type) as fuel_type
# MAGIC FROM delta.`abfss://silver@iotstorageadls.dfs.core.windows.net/iot_fleet_data`
# MAGIC GROUP BY CAST(eventTimestamp AS DATE), deviceId
# MAGIC ORDER BY eventDate, deviceId;
# MAGIC
# MAGIC -- Verificar la vista Gold
# MAGIC SELECT * FROM gold.daily_fleet_summary;

# COMMAND ----------

daily_fleet_summary_df = spark.sql("SELECT * FROM gold.daily_fleet_summary")
gold_daily_summary_path = f"{gold_base_path}/daily_fleet_summary"

daily_fleet_summary_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(gold_daily_summary_path)
print(f"Tabla Gold 'daily_fleet_summary' guardada en: {gold_daily_summary_path}")

spark.read.format("delta").load(gold_daily_summary_path).limit(5).display()

# COMMAND ----------

num_filas = daily_fleet_summary_df.count()
print(f"El número de filas es: {num_filas}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.vehicle_health_alerts AS
# MAGIC SELECT
# MAGIC     deviceId,
# MAGIC     eventTimestamp,
# MAGIC     CASE
# MAGIC         WHEN engine_temp_c > 100 THEN 'HIGH_ENGINE_TEMP'
# MAGIC         WHEN battery_voltage < 11.5 THEN 'LOW_BATTERY'
# MAGIC         WHEN oil_pressure_kpa < 200 THEN 'LOW_OIL_PRESSURE'
# MAGIC         WHEN engine_rpm > 6000 THEN 'HIGH_RPM'
# MAGIC         WHEN fuel_level_percent < 10 THEN 'LOW_FUEL'
# MAGIC         WHEN vibration_level > 8 THEN 'HIGH_VIBRATION'
# MAGIC         ELSE 'NORMAL'
# MAGIC     END as alert_type,
# MAGIC     engine_temp_c,
# MAGIC     battery_voltage,
# MAGIC     oil_pressure_kpa,
# MAGIC     engine_rpm,
# MAGIC     fuel_level_percent,
# MAGIC     vibration_level
# MAGIC FROM delta.`abfss://silver@iotstorageadls.dfs.core.windows.net/iot_fleet_data`
# MAGIC WHERE 
# MAGIC     engine_temp_c > 100 OR
# MAGIC     battery_voltage < 11.5 OR
# MAGIC     oil_pressure_kpa < 200 OR
# MAGIC     engine_rpm > 6000 OR
# MAGIC     fuel_level_percent < 10 OR
# MAGIC     vibration_level > 8
# MAGIC ORDER BY eventTimestamp DESC;
# MAGIC
# MAGIC -- Verificar la vista Gold
# MAGIC SELECT * FROM gold.vehicle_health_alerts LIMIT 10;

# COMMAND ----------

vehicle_health_alerts_df = spark.sql("SELECT * FROM gold.vehicle_health_alerts")
gold_health_alerts_path = f"{gold_base_path}/vehicle_health_alerts"

vehicle_health_alerts_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(gold_health_alerts_path)
print(f"Tabla Gold 'vehicle_health_alerts' guardada en: {gold_health_alerts_path}")

spark.read.format("delta").load(gold_health_alerts_path).limit(5).display()

# COMMAND ----------

num_filas = vehicle_health_alerts_df.count()
print(f"El número de filas de alertas es: {num_filas}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.fuel_efficiency_analysis AS
# MAGIC SELECT
# MAGIC     deviceId,
# MAGIC     fuel_type,
# MAGIC     CAST(eventTimestamp AS DATE) as eventDate,
# MAGIC     AVG(speed_kmh) as avg_speed_kmh,
# MAGIC     (MAX(odometer_km) - MIN(odometer_km)) as distance_traveled_km,
# MAGIC     (MIN(fuel_level_percent) - MAX(fuel_level_percent)) as fuel_consumed_percent,
# MAGIC     CASE 
# MAGIC         WHEN (MIN(fuel_level_percent) - MAX(fuel_level_percent)) > 0 
# MAGIC         THEN (MAX(odometer_km) - MIN(odometer_km)) / (MIN(fuel_level_percent) - MAX(fuel_level_percent))
# MAGIC         ELSE NULL
# MAGIC     END as km_per_fuel_percent,
# MAGIC     AVG(engine_rpm) as avg_engine_rpm,
# MAGIC     COUNT(*) as total_readings
# MAGIC FROM delta.`abfss://silver@iotstorageadls.dfs.core.windows.net/iot_fleet_data`
# MAGIC GROUP BY deviceId, fuel_type, CAST(eventTimestamp AS DATE)
# MAGIC HAVING (MAX(odometer_km) - MIN(odometer_km)) > 0
# MAGIC ORDER BY eventDate, deviceId;
# MAGIC
# MAGIC -- Verificar la vista Gold
# MAGIC SELECT * FROM gold.fuel_efficiency_analysis;

# COMMAND ----------

fuel_efficiency_analysis_df = spark.sql("SELECT * FROM gold.fuel_efficiency_analysis")
gold_fuel_efficiency_path = f"{gold_base_path}/fuel_efficiency_analysis"

fuel_efficiency_analysis_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(gold_fuel_efficiency_path)
print(f"Tabla Gold 'fuel_efficiency_analysis' guardada en: {gold_fuel_efficiency_path}")

spark.read.format("delta").load(gold_fuel_efficiency_path).limit(5).display()

# COMMAND ----------

num_filas = fuel_efficiency_analysis_df.count()
print(f"El número de filas de análisis de eficiencia es: {num_filas}")

# COMMAND ----------