-- =====================================================================
-- PROJECT: AtmoSync – Micro-Climate Arbitrage Analytics
-- TASK 2: Warehouse and ELT Setup
-- NOTE: ALL STATEMENTS ARE NON-DESTRUCTIVE (CREATE IF NOT EXISTS)
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. ROLES & ACCESS CONTROL SETUP
-- ---------------------------------------------------------------------
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS ATMOSYNC_ADMIN
    COMMENT = 'Administrative role for AtmoSync data platform';

CREATE ROLE IF NOT EXISTS ATMOSYNC_KAFKA_ROLE
    COMMENT = 'Service account role for Kafka Connector ingestion';

CREATE ROLE IF NOT EXISTS ATMOSYNC_DBT_ROLE
    COMMENT = 'Service account role for dbt Core transformations';

CREATE ROLE IF NOT EXISTS ATMOSYNC_BI_ROLE
    COMMENT = 'Read-only role for Apache Superset reporting';

-- Establish role hierarchy
GRANT ROLE ATMOSYNC_KAFKA_ROLE TO ROLE ATMOSYNC_ADMIN;
GRANT ROLE ATMOSYNC_DBT_ROLE   TO ROLE ATMOSYNC_ADMIN;
GRANT ROLE ATMOSYNC_BI_ROLE    TO ROLE ATMOSYNC_ADMIN;

-- ---------------------------------------------------------------------
-- 2. VIRTUAL WAREHOUSES (COMPUTE ISOLATION)
-- ---------------------------------------------------------------------
USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS ATMOSYNC_INGEST_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Continuous Kafka Connector / Snowpipe Streaming ingestion';

CREATE WAREHOUSE IF NOT EXISTS ATMOSYNC_DBT_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Transformation compute for dbt Core pipeline';

CREATE WAREHOUSE IF NOT EXISTS ATMOSYNC_BI_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Query compute for Apache Superset analytics';

-- ---------------------------------------------------------------------
-- 3. DATABASE & SCHEMAS (ATMOSYNC_DB -> RAW -> STG -> INT -> MARTS)
-- ---------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS ATMOSYNC_DB
    COMMENT = 'Central database for AtmoSync Micro-Climate Arbitrage Analytics';

USE DATABASE ATMOSYNC_DB;

CREATE SCHEMA IF NOT EXISTS ATMOSYNC_DB.RAW
    COMMENT = 'Bronze: Raw Kafka stream ingestion landing zone';

CREATE SCHEMA IF NOT EXISTS ATMOSYNC_DB.STG
    COMMENT = 'Silver: Cleaned, typed, deduplicated staging views';

CREATE SCHEMA IF NOT EXISTS ATMOSYNC_DB.INT
    COMMENT = 'Silver: Intermediate excursion calculations and risk scoring';

CREATE SCHEMA IF NOT EXISTS ATMOSYNC_DB.MARTS
    COMMENT = 'Gold: Dimensional star schema for Apache Superset';

-- ---------------------------------------------------------------------
-- 4. BRONZE LAYER: INGESTION TABLE (KAFKA CONNECTOR TARGET)
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ATMOSYNC_DB.RAW.CONTAINER_TELEMETRY_RAW (
    RECORD_METADATA     VARIANT       COMMENT 'Kafka topic metadata (offset, partition, timestamp)',
    RECORD_CONTENT      VARIANT       COMMENT 'Raw JSON sensor payload',
    INGESTED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ---------------------------------------------------------------------
-- 5. GOLD REFERENCE / SEED TABLES
-- ---------------------------------------------------------------------
-- Commodity Thresholds (Primary key is standardized lowercase 'COMMODITY')
CREATE TABLE IF NOT EXISTS ATMOSYNC_DB.MARTS.DIM_COMMODITY_THRESHOLDS (
    COMMODITY                       VARCHAR(100) PRIMARY KEY COMMENT 'Normalized commodity key (e.g., strawberries, vaccines)',
    COMMODITY_NAME                  VARCHAR(100)             COMMENT 'Display name',
    CATEGORY                        VARCHAR(50)              COMMENT 'Category (Berries, Dairy, Bio-Pharma, Meat, Seafood)',
    TEMP_MIN_CELSIUS                FLOAT                    COMMENT 'Minimum safe storage temperature',
    TEMP_MAX_CELSIUS                FLOAT                    COMMENT 'Maximum safe storage temperature',
    HUMIDITY_MIN_PCT                FLOAT                    COMMENT 'Minimum relative humidity percentage',
    HUMIDITY_MAX_PCT                FLOAT                    COMMENT 'Maximum relative humidity percentage',
    MAX_VIBRATION_G                 FLOAT                    COMMENT 'Maximum safe vibration threshold in g-force',
    BASE_PRICE_PER_KG_USD           NUMBER(10,2)             COMMENT 'Base market value per kg',
    CRITICAL_EXCURSION_HOURS        NUMBER(5,1)              COMMENT 'Cumulative hours out-of-range before total loss',
    SPOILAGE_RATE_PER_DEGREE_HOUR   NUMBER(6,4)              COMMENT 'Economic depreciation rate per degree-hour violation'
);

-- Container Master Dimension
CREATE TABLE IF NOT EXISTS ATMOSYNC_DB.MARTS.DIM_CONTAINER (
    CONTAINER_ID        VARCHAR(50) PRIMARY KEY,
    REEFER_MODEL        VARCHAR(100),
    INSULATION_GRADE    VARCHAR(20),
    MAX_PAYLOAD_KG      FLOAT,
    COMMISSIONED_YEAR   INTEGER
);

-- ---------------------------------------------------------------------
-- 6. PERMISSIONS & ROLE GRANTS (LEAST PRIVILEGE)
-- ---------------------------------------------------------------------

-- A. Kafka Ingestion Role Grants
GRANT USAGE ON WAREHOUSE ATMOSYNC_INGEST_WH TO ROLE ATMOSYNC_KAFKA_ROLE;
GRANT USAGE ON DATABASE ATMOSYNC_DB TO ROLE ATMOSYNC_KAFKA_ROLE;
GRANT USAGE ON SCHEMA ATMOSYNC_DB.RAW TO ROLE ATMOSYNC_KAFKA_ROLE;
GRANT INSERT, SELECT ON TABLE ATMOSYNC_DB.RAW.CONTAINER_TELEMETRY_RAW TO ROLE ATMOSYNC_KAFKA_ROLE;

-- B. dbt Transformation Role Grants
GRANT USAGE ON WAREHOUSE ATMOSYNC_DBT_WH TO ROLE ATMOSYNC_DBT_ROLE;
GRANT USAGE ON DATABASE ATMOSYNC_DB TO ROLE ATMOSYNC_DBT_ROLE;
GRANT USAGE, CREATE SCHEMA, CREATE TABLE, CREATE VIEW ON DATABASE ATMOSYNC_DB TO ROLE ATMOSYNC_DBT_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE ATMOSYNC_DB TO ROLE ATMOSYNC_DBT_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA ATMOSYNC_DB.RAW TO ROLE ATMOSYNC_DBT_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ATMOSYNC_DB.RAW TO ROLE ATMOSYNC_DBT_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA ATMOSYNC_DB.STG TO ROLE ATMOSYNC_DBT_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA ATMOSYNC_DB.INT TO ROLE ATMOSYNC_DBT_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA ATMOSYNC_DB.MARTS TO ROLE ATMOSYNC_DBT_ROLE;

-- C. Superset BI Role Grants (Read-Only)
GRANT USAGE ON WAREHOUSE ATMOSYNC_BI_WH TO ROLE ATMOSYNC_BI_ROLE;
GRANT USAGE ON DATABASE ATMOSYNC_DB TO ROLE ATMOSYNC_BI_ROLE;
GRANT USAGE ON SCHEMA ATMOSYNC_DB.MARTS TO ROLE ATMOSYNC_BI_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA ATMOSYNC_DB.MARTS TO ROLE ATMOSYNC_BI_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ATMOSYNC_DB.MARTS TO ROLE ATMOSYNC_BI_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA ATMOSYNC_DB.MARTS TO ROLE ATMOSYNC_BI_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ATMOSYNC_DB.MARTS TO ROLE ATMOSYNC_BI_ROLE;

