from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

dbutils.secrets.listScopes()


storage_account_name = "iotstorageadls"
scope_name = "iot-secrets-scope"

try:
    client_id = dbutils.secrets.get(scope=scope_name, key="client-id")
    client_secret = dbutils.secrets.get(scope=scope_name, key="client-secret")
    tenant_id = dbutils.secrets.get(scope=scope_name, key="tenant-id")

    spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
    spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
    spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

    print("\n¡Conexión exitosa a ADLS Gen2 usando Service Principal y Key Vault!")

    adls_bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/iotFleetMonitorHub"
    print(f"Ruta base de ADLS Bronze configurada: {adls_bronze_path}")


except Exception as e:
    print(f"\nError al acceder a los secretos o contenedores: {e}")
    print("Verifica:")
    print("1. Tus secretos 'client-id', 'client-secret', 'tenant-id' en el ámbito 'iot-secrets-scope' en Key Vault.")
    print("2. Los permisos del Service Principal en la Storage Account (Storage Blob Data Contributor).")
    print("3. Los permisos del Service Principal en el Key Vault (Get/List Secrets).")
    print("4. La configuración de tu Secret Scope 'iot-secrets-scope' en Databricks (DNS Name, Resource ID).")
    print(f"5. Que el contenedor 'telemetry-data' exista en tu Storage Account '{storage_account_name}'.")


try:
    df_bronze = spark.read.format("avro").load(f"{adls_bronze_path}/*/*/*/*/*")
    print(f"DataFrame Bronze creado con {df_bronze.count()} registros.")
    df_bronze.printSchema()
    df_bronze.limit(5).display() 
except Exception as e:
    print(f"ERROR al leer los datos AVRO: {e}")
    print("Verifica:")
    print("- Que tu generador de datos esté enviando mensajes y que IoT Hub los esté enrutando al Data Lake.")
    print("- Que los archivos .avro estén efectivamente presentes en tu Data Lake en esa ruta.")

try:
    df_bronze = spark.read.format("avro").load(f"{adls_bronze_path}/*/*/*/*/*")
    print(f"DataFrame Bronze creado con {df_bronze.count()} registros.")
    df_bronze.printSchema() 
    df_bronze.limit(5).display() 
except Exception as e:
    print(f"ERROR al leer los datos AVRO: {e}")
    print("Verifica:")
    print(f"- Que la ruta 'adls_bronze_path': {adls_bronze_path} sea correcta y que el contenedor 'telemetry-data' exista.")
    print("- Que tu generador de datos esté enviando mensajes y que IoT Hub los esté enrutando al Data Lake.")
    print("- Que los archivos .avro estén efectivamente presentes en tu Data Lake en esa ruta.")


json_schema = StructType([
    StructField("deviceId", StringType(), True),
    StructField("timestamp", LongType(), True), 
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("speed_kmh", DoubleType(), True),
    StructField("engine_temp_c", DoubleType(), True),
    StructField("vibration_level", DoubleType(), True),
    StructField("fuel_level_percent", DoubleType(), True),
    StructField("door_open", BooleanType(), True).
    StructField("fuel_type", StringType(), True),
    StructField("battery_voltage", DoubleType(), True),
    StructField("oil_pressure_kpa", DoubleType(), True),
    StructField("engine_rpm", DoubleType(), True),
    StructField("odometer_km", DoubleType(), True)
])


df_silver = df_bronze.withColumn("json_body", unbase64(col("body")).cast("string")) \
                     .withColumn("json_data", from_json(col("json_body"), json_schema)) \
                     .select(
                         col("json_data.deviceId").alias("deviceId"),
                         from_unixtime(col("json_data.timestamp")).alias("eventTimestamp"),
                         col("json_data.latitude").alias("latitude"),
                         col("json_data.longitude").alias("longitude"),
                         col("json_data.speed_kmh").alias("speed_kmh"),
                         col("json_data.engine_temp_c").alias("engine_temp_c"),
                         col("json_data.vibration_level").alias("vibration_level"),
                         col("json_data.fuel_level_percent").alias("fuel_level_percent"),
                         col("json_data.door_open").alias("door_open"),
                         col("json_data.fuel_type").alias("fuel_type"),
                         col("json_data.battery_voltage").alias("battery_voltage"),
                         col("json_data.oil_pressure_kpa").alias("oil_pressure_kpa"),
                         col("json_data.engine_rpm").alias("engine_rpm"),
                         col("json_data.odometer_km").alias("odometer_km"),
                         col("EnqueuedTimeUtc").alias("iothub_enqueued_time_utc"),
                         current_timestamp().alias("processed_timestamp")
                     )


print(f"DataFrame Silver creado con {df_silver.count()} registros.")
df_silver.printSchema()
df_silver.limit(5).display()


silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/iot_fleet_data"

df_silver.write.format("delta") \
          .mode("append") \
          .option("mergeSchema", "true") \
          .partitionBy("deviceId") \
          .save(silver_path)

print(f"Datos Silver guardados en Delta Lake en: {silver_path}")


print("Verificando datos leídos de la capa Silver:")
df_silver_read = spark.read.format("delta").load(silver_path)
print(f"Total de registros en la capa Silver: {df_silver_read.count()}")
df_silver_read.limit(5).display()