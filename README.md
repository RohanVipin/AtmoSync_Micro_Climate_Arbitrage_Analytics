# AtmoSync – Micro-Climate Arbitrage Analytics (dbt Core)

## Overview
This dbt Core project powers Task 2 (Warehouse & ELT) for **AtmoSync**. It processes real-time and micro-batched IoT container sensor telemetry landed from Apache Kafka into Snowflake, cleans and standardizes the readings, assesses micro-climate excursions (temperature, humidity, vibration), and computes arbitrage risk and salvage values for perishable commodities.

## Architecture Pipeline
```
ATMOSYNC_DB
├── RAW (Bronze: CONTAINER_TELEMETRY_RAW via Kafka Connector)
├── STG (Silver: stg_sensor_readings)
├── INT (Silver: int_sensor_excursions)
└── MARTS (Gold: dim_commodity, dim_container, fct_sensor_telemetry, fct_arbitrage_risk_summary)
```

## Quick Start Guide

### 1. Configure Snowflake Credentials
Set the following environment variables or edit `profiles.yml`:
```bash
export SNOWFLAKE_ACCOUNT="<your_account_identifier>"
export SNOWFLAKE_USER="<your_username>"
export SNOWFLAKE_PASSWORD="<your_password>"
export SNOWFLAKE_ROLE="ATMOSYNC_DBT_ROLE"
export SNOWFLAKE_DATABASE="ATMOSYNC_DB"
export SNOWFLAKE_WAREHOUSE="ATMOSYNC_DBT_WH"
```

### 2. Test Connection
```bash
dbt debug --profiles-dir .
```

### 3. Load Seed Data (Commodity Thresholds)
```bash
dbt seed --profiles-dir .
```

### 4. Run Transformations
```bash
dbt run --profiles-dir .
```

### 5. Execute Data Quality Tests
```bash
dbt test --profiles-dir .
```

