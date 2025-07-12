# 🚗 IoT Fleet Monitoring: Azure & Microsoft Fabric Data Pipeline

## ✨ Project Overview

This comprehensive project demonstrates IoT telemetry data processing using **two different Microsoft cloud approaches**: traditional Azure services and the modern Microsoft Fabric platform. The solution simulates vehicle fleet telemetry data and processes it through a structured Medallion Architecture (Bronze, Silver, Gold layers), showcasing versatility across Microsoft's data ecosystem.

**Key Differentiators:**
- **Dual Platform Implementation**: Compare Azure traditional services vs. Microsoft Fabric
- **Medallion Architecture**: Structured data layers for both platforms
- **Real-time Processing**: Streaming data pipelines with anomaly detection
- **Docker Integration**: Containerized IoT data generation
- **Business Intelligence**: End-to-end analytics and visualization

## 🎯 Why This Project?

This project strategically demonstrates enterprise-level data engineering skills across Microsoft's evolving data platform:

**Technical Skills Demonstrated:**
- **Multi-Platform Expertise**: Azure Data Services + Microsoft Fabric
- **Data Lake Architecture**: Medallion pattern implementation
- **Real-time Streaming**: IoT data ingestion and processing
- **Delta Lake Integration**: ACID transactions and schema evolution
- **Container Orchestration**: Docker-based data generation
- **Cloud Security**: Service Principal and Key Vault integration
- **Business Intelligence**: Power BI integration with Direct Lake

## 🏗️ Architecture Comparison

### Azure Traditional Architecture
```
IoT Device Simulator (Docker) 
    ↓
Azure IoT Hub (Data Ingestion)
    ↓
Azure Data Lake Storage Gen2 (ADLS Gen2)
    ├── Bronze Container (Raw AVRO data)
    ├── Silver Container (Clean Delta Lake data)
    └── Gold Container (Aggregated analytics)
    ↓
Azure Databricks (Spark Processing)
    ↓
Power BI (Business Intelligence)
```

### Microsoft Fabric Architecture
```
IoT Device Simulator (Docker)
    ↓
Azure IoT Hub (Data Ingestion)
    ↓
Microsoft Fabric Eventstream (Real-time Processing)
    ↓
Microsoft Fabric Lakehouse (Delta Lake Storage)
    ├── Bronze Layer (iot_raw_telemetry_v2)
    ├── Silver Layer (iot_telemetry_silver_v2)
    └── Gold Layer (current_vehicle_state_v2, daily_fleet_summary_v2)
    ↓
Fabric Notebooks (PySpark Processing)
    ↓
Power BI (Direct Lake Integration)
```

## 🛠️ Technology Stack

### Core Technologies
- **Python**: Data generation and processing
- **Docker**: Containerized IoT simulator
- **Apache Spark (PySpark)**: Data transformations
- **Delta Lake**: Reliable data storage format
- **Apache Avro/Parquet**: Data serialization formats

### Azure Traditional Stack
- **Azure IoT Hub**: Scalable telemetry ingestion
- **Azure Data Lake Storage Gen2**: Multi-layer data storage
- **Azure Databricks**: Managed Spark platform
- **Azure Key Vault**: Secure credential management

### Microsoft Fabric Stack
- **Microsoft Fabric Eventstream**: Real-time data ingestion
- **Microsoft Fabric Lakehouse**: Unified analytics platform
- **Microsoft Fabric Notebooks**: Integrated PySpark environment
- **Power BI Direct Lake**: High-performance analytics

## 🚀 Implementation Guide

### Prerequisites
- Azure subscription with appropriate permissions
- Microsoft Fabric workspace with active capacity
- Docker Desktop installed
- Python 3.9+ with pip
- Power BI Desktop (latest version)
- Azure CLI (optional, for automation)

### Step 1: IoT Data Generator Setup

#### Create the Data Generator
```python
# iot_data_generator.py
import random
import json
import time
from datetime import datetime, timezone
from faker import Faker
from azure.iot.device import IoTHubDeviceClient, Message

class VehicleSimulator:
    def __init__(self, device_id, connection_string):
        self.device_id = device_id
        self.client = IoTHubDeviceClient.create_from_connection_string(connection_string)
        self.faker = Faker()
        
    def generate_telemetry(self):
        # Generate realistic vehicle telemetry with anomaly injection
        base_temp = random.uniform(70, 90)
        
        # Inject anomalies (10% chance)
        if random.random() < 0.1:
            engine_temp = random.uniform(110, 130)  # Overheating
            vibration = random.uniform(8, 12)       # Excessive vibration
        else:
            engine_temp = base_temp + random.uniform(-5, 15)
            vibration = random.uniform(0, 3)
            
        return {
            "deviceId": self.device_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "engineTemperature": round(engine_temp, 2),
            "vibration": round(vibration, 2),
            "fuelLevel": round(random.uniform(0, 100), 2),
            "speed": round(random.uniform(0, 80), 2),
            "latitude": round(self.faker.latitude(), 6),
            "longitude": round(self.faker.longitude(), 6),
            "rpm": random.randint(800, 6000),
            "isAnomalous": engine_temp > 105 or vibration > 7
        }
```

#### Dockerfile
```dockerfile
FROM python:3.9-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY iot_data_generator.py .

CMD ["python", "iot_data_generator.py"]
```

#### Build and Deploy
```bash
# Build Docker image
docker build -t iot-fleet-simulator:latest .

# Run locally for testing
docker run --env-file .env iot-fleet-simulator:latest
```

### Step 2: Choose Your Platform

## Option A: Azure Traditional Implementation

### Azure IoT Hub Setup
```bash
# Create IoT Hub
az iot hub create --resource-group myResourceGroup --name myIoTHub --sku S1

# Register device
az iot hub device-identity create --device-id vehicle-001 --hub-name myIoTHub

# Get connection string
az iot hub device-identity show-connection-string --device-id vehicle-001 --hub-name myIoTHub
```

### Azure Data Lake Storage Gen2 Setup
```bash
# Create storage account
az storage account create --name mystorageaccount --resource-group myResourceGroup --sku Standard_LRS

# Create containers
az storage container create --name bronze --account-name mystorageaccount
az storage container create --name silver --account-name mystorageaccount
az storage container create --name gold --account-name mystorageaccount
```

### Azure Databricks Processing
```python
# 01_bronze_to_silver_processing.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder.appName("IoTBronzeToSilver").getOrCreate()

# Read from Bronze layer (AVRO format from IoT Hub)
bronze_df = spark.read.format("avro").load("/mnt/bronze/iot-hub-data/")

# Parse JSON body content
parsed_df = bronze_df.select(
    col("EnqueuedTimeUtc").alias("ingestion_time"),
    from_json(col("Body").cast("string"), telemetry_schema).alias("telemetry")
).select("ingestion_time", "telemetry.*")

# Write to Silver layer (Delta Lake format)
parsed_df.write.format("delta").mode("append").partitionBy("deviceId").save("/mnt/silver/iot_telemetry/")
```

## Option B: Microsoft Fabric Implementation

### Fabric Eventstream Setup
1. Create new **Eventstream** in your Fabric workspace
2. Add **Azure IoT Hub** as source
3. Add **Lakehouse** as destination
4. Configure table name: `iot_raw_telemetry_v2`
5. Set input format to JSON

### Fabric Lakehouse Processing
```python
# 01_Bronze_to_Silver_Fabric.ipynb
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Read from Bronze table
bronze_df = spark.sql("SELECT * FROM iot_raw_telemetry_v2")

# Clean and transform data
silver_df = bronze_df.select(
    col("deviceId"),
    col("timestamp").cast("timestamp"),
    col("engineTemperature").cast("double"),
    col("vibration").cast("double"),
    col("fuelLevel").cast("double"),
    col("speed").cast("double"),
    col("latitude").cast("double"),
    col("longitude").cast("double"),
    col("rpm").cast("int"),
    col("isAnomalous").cast("boolean"),
    current_timestamp().alias("processed_time")
).filter(col("deviceId").isNotNull())

# Write to Silver table
silver_df.write.format("delta").mode("append").saveAsTable("iot_telemetry_silver_v2")
```

### Step 3: Gold Layer Implementation

#### Current Vehicle State (Real-time View)
```python
# Create current vehicle state table
current_state_df = spark.sql("""
    SELECT 
        deviceId,
        timestamp,
        engineTemperature,
        vibration,
        fuelLevel,
        speed,
        latitude,
        longitude,
        rpm,
        isAnomalous,
        ROW_NUMBER() OVER (PARTITION BY deviceId ORDER BY timestamp DESC) as rn
    FROM iot_telemetry_silver_v2
""").filter(col("rn") == 1).drop("rn")

# Save as Gold table
current_state_df.write.format("delta").mode("overwrite").saveAsTable("current_vehicle_state_v2")
```

#### Daily Fleet Summary (Aggregated Analytics)
```python
# Create daily aggregations
daily_summary_df = spark.sql("""
    SELECT 
        deviceId,
        DATE(timestamp) as date,
        COUNT(*) as total_readings,
        AVG(engineTemperature) as avg_engine_temp,
        MAX(engineTemperature) as max_engine_temp,
        AVG(vibration) as avg_vibration,
        MAX(vibration) as max_vibration,
        AVG(fuelLevel) as avg_fuel_level,
        MIN(fuelLevel) as min_fuel_level,
        AVG(speed) as avg_speed,
        MAX(speed) as max_speed,
        SUM(CASE WHEN isAnomalous THEN 1 ELSE 0 END) as anomaly_count
    FROM iot_telemetry_silver_v2
    GROUP BY deviceId, DATE(timestamp)
""")

# Save as Gold table
daily_summary_df.write.format("delta").mode("overwrite").saveAsTable("daily_fleet_summary_v2")
```

### Step 4: Power BI Integration

#### For Azure Implementation
1. Connect Power BI to Azure Data Lake Storage Gen2
2. Use DirectQuery mode for real-time data
3. Create relationships between Gold tables

#### For Microsoft Fabric Implementation
1. Use **Direct Lake** mode for optimal performance
2. Connect to Fabric Lakehouse SQL endpoint
3. Automatic schema refresh with semantic models

#### Sample Power BI Visualizations
- **Fleet Overview Dashboard**: Current status of all vehicles
- **Anomaly Detection**: Real-time alerts for engine overheating
- **Fuel Efficiency Trends**: Historical fuel consumption patterns
- **Geographic Distribution**: Vehicle locations on interactive maps
- **Performance Metrics**: KPIs for fleet utilization

## 📁 Project Structure

```
iot-fleet-monitoring/
├── docker/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── iot_data_generator.py
├── azure-implementation/
│   ├── databricks/
│   │   ├── notebooks/
│   │   │   ├── 01_bronze_to_silver.py
│   │   │   ├── 02_silver_to_gold.py
│   │   │   └── 03_anomaly_detection.py
│   │   └── config/
│   ├── azure-resources/
│   │   ├── arm-templates/
│   │   └── deployment-scripts/
│   └── README-Azure.md
├── fabric-implementation/
│   ├── notebooks/
│   │   ├── 01_Bronze_to_Silver_IoT.ipynb
│   │   ├── 02_Silver_to_Gold_IoT.ipynb
│   │   └── 03_Advanced_Analytics.ipynb
│   ├── eventstream-config/
│   └── README-Fabric.md
├── powerbi/
│   ├── azure-dashboard.pbix
│   ├── fabric-dashboard.pbix
│   └── shared-templates/
├── docs/
│   ├── architecture-diagrams/
│   ├── deployment-guide/
│   └── performance-comparison/
└── README.md
```

## 🎯 Key Features Demonstrated

### Technical Architecture
- **Medallion Architecture**: Structured Bronze → Silver → Gold data layers
- **Delta Lake Integration**: ACID transactions, schema evolution, time travel
- **Real-time Processing**: Streaming ingestion with low latency
- **Data Partitioning**: Optimized for query performance
- **Containerization**: Docker-based scalable data generation

### Cloud Integration
- **Azure Services**: IoT Hub, Data Lake, Databricks, Key Vault
- **Microsoft Fabric**: Eventstream, Lakehouse, Notebooks, Direct Lake
- **Security**: Service Principal authentication, secure credential management
- **Monitoring**: Built-in observability and alerting

### Business Intelligence
- **Real-time Dashboards**: Current fleet status monitoring
- **Historical Analytics**: Trend analysis and predictive insights
- **Anomaly Detection**: Proactive maintenance alerts
- **Performance Metrics**: Fleet utilization and efficiency KPIs

## 💼 Business Value & Use Cases

### Fleet Management
- **Real-time Monitoring**: Track vehicle location, performance, and health
- **Predictive Maintenance**: Identify potential issues before failures
- **Route Optimization**: Analyze traffic patterns and fuel efficiency
- **Compliance Reporting**: Automated regulatory compliance tracking

### Operational Analytics
- **Cost Optimization**: Fuel consumption and maintenance cost analysis
- **Performance Benchmarking**: Compare vehicle and driver performance
- **Capacity Planning**: Optimize fleet size based on demand patterns
- **Risk Management**: Identify high-risk driving behaviors and routes

## 🔧 Getting Started

### Quick Start (5 minutes)
1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/iot-fleet-monitoring.git
   cd iot-fleet-monitoring
   ```

2. **Choose your platform**
   - For Azure: Follow `azure-implementation/README-Azure.md`
   - For Fabric: Follow `fabric-implementation/README-Fabric.md`

3. **Deploy IoT simulator**
   ```bash
   cd docker
   docker build -t iot-simulator .
   docker run --env-file .env iot-simulator
   ```

4. **Configure data pipeline**
   - Import notebooks to your chosen platform
   - Set up connection strings and authentication
   - Run pipeline notebooks

5. **Connect Power BI**
   - Open appropriate .pbix file from `powerbi/` folder
   - Configure data source connections
   - Refresh and explore dashboards

## 📊 Performance Comparison

| Feature | Azure Traditional | Microsoft Fabric |
|---------|------------------|------------------|
| **Setup Complexity** | High (Multiple services) | Medium (Integrated platform) |
| **Real-time Processing** | Manual configuration | Built-in streaming |
| **Cost Management** | Pay-per-service | Capacity-based |
| **Integration** | Complex connections | Native integration |
| **Scalability** | Highly scalable | Auto-scaling |
| **Learning Curve** | Steep | Moderate |





## 📞 Contact & Support

- **Project Maintainer**: [Your Name]
- **Email**: [your.email@example.com]
- **LinkedIn**: [Your LinkedIn Profile]
- **Issues**: [GitHub Issues Page]

---

*This project demonstrates real-world data engineering practices using Microsoft's cloud platforms. It's designed to showcase technical skills while solving practical business problems in IoT fleet management.*
