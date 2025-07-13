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

## Azure Architecture

<img src="images/azure-project3.png" width="70%">


## Microsoft Fabric Architecture

<img src="images/fabric-project3.png" width="70%">

## 🛠️ Technology Stack

### Core Technologies
- **Python**: Data generation and processing
- **Docker**: Containerized IoT simulator
- **Apache Spark (PySpark)**: Data transformations
- **Delta Lake**: Reliable data storage format
- **Apache Avro/Parquet**: Data serialization formats

### Azure Stack
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

✅ Shared Step: Simulate IoT Data

Run iot_data_generator.py in Docker
Sends data to Azure IoT Hub

 Azure Implementation

IoT Hub: Device registration and connection string setup

Databricks:

01_bronze_to_silver.py: Avro to cleaned Delta

02_silver_to_gold.py: Aggregated tables: current_state, fleet_summary, trends

Data Lake: Organize into Bronze/Silver/Gold containers

Power BI: Connect via Azure connector or SQL endpoint


🟣 Fabric Implementation

Eventstream: Ingest from IoT Hub

Lakehouse:

Bronze: iot_raw_telemetry_v2

Silver: Streaming notebook applies schema validation, partitioning, enrichment

Gold Tables:

current_vehicle_state_v2 → Latest vehicle status

daily_fleet_summary_v2 → Daily KPIs

vehicle_trends_analysis → Hourly pattern insights

Power BI: Connect via Direct Lake for instant access


### 📊 Results and Visualizations

🔄 Deployment Snapshots & Resources

Azure Resource Group

<img src="images/resource-group.png" width="70%">

Microsoft Fabric Workspace

<img src="images/workspace.png" width="70%">

Microsoft Fabric Lakehouse Overview

<img src="images/Lakehouse.png" width="70%">

🥇 Fabric Eventstream

<img src="images/eventstream.png" width="70%">

📈 Power BI Dashboards

Fleet State: Live engine, oil, location, velocity

<img src="images/currentstate.png" width="70%">

Daily Summary (Aggregates)

<img src="images/summary.png" width="70%">






🧠 Business Use Cases

Real-time vehicle & fleet monitoring

Operational KPI dashboards

Foundation for anomaly detection and route optimization




## 📁 Project Structure

iot-fleet-monitoring/
├── docker/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── iot_data_generator.py
├── azure-implementation/
│   ├── notebooks/
│   │   ├── 01_bronze_to_silver.py
│   │   │   ├── 02_silver_to_gold.py
│   │   │   └── 03_anomaly_detection.py
│   │   └── config/
├── fabric-implementation/
│   └──notebooks/
│       ├── 01_Bronze_to_Silver_IoT.ipynb
│       ├── 02_Silver_to_Gold_IoT.ipynb 
└── README.md


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



## 📞 Contact & Support

- **Project Maintainer**: [Your Name]
- **Email**: [your.email@example.com]
- **LinkedIn**: [Your LinkedIn Profile]
- **Issues**: [GitHub Issues Page]

---

*This project demonstrates real-world data engineering practices using Microsoft's cloud platforms. It's designed to showcase technical skills while solving practical business problems in IoT fleet management.*
