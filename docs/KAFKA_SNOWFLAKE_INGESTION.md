# AtmoSync Kafka-to-Snowflake Ingestion Consumer Guide
**Module:** Task 2 — Warehouse and ELT Engineering  
**Consumer Script:** `scripts/kafka_to_snowflake_consumer.py`  
**Dependencies:** `requirements-ingestion.txt`  
**Target Table:** `ATMOSYNC_DB.RAW.CONTAINER_TELEMETRY_RAW`  

---

## 1. Architecture & Streaming Contract

The ingestion consumer acts as the bridge between Task A (Apache Kafka streaming broker) and Snowflake RAW (Bronze storage layer):

```
+------------------------------------+
|  Task A: IoT Virtual Simulator     |
|  (3 Containers x 3 Sensors v1)     |
+-----------------+------------------+
                  |
                  v
+-----------------+------------------+
|  Apache Kafka Broker (Port 9092)   |
|  Topic: atmosync.telemetry.v1      |
|  Partitions: 3 (Key: container_id) |
+-----------------+------------------+
                  |
                  v
+-----------------+------------------+
|  kafka_to_snowflake_consumer.py   |
|  - Validates 7-field contract v1   |
|  - Builds RECORD_METADATA          |
|  - Preserves RECORD_CONTENT JSON   |
|  - Transactional Snowflake MERGE   |
|  - Commits Kafka offsets post-load |
+-----------------+------------------+
                  |
                  v
+-----------------+------------------------------------------+
|  Snowflake RAW: ATMOSYNC_DB.RAW.CONTAINER_TELEMETRY_RAW   |
|  - RECORD_METADATA (VARIANT)                              |
|  - RECORD_CONTENT (VARIANT)                               |
|  - INGESTED_AT (TIMESTAMP_NTZ default CURRENT_TIMESTAMP)  |
+------------------------------------------------------------+
```

---

## 2. Canonical 7-Field Event Contract

Task A produces events adhering strictly to the Version-1 Data Contract:

```json
{
  "schema_version": 1,
  "event_id": "e595301f-e4ac-4315-ace4-cf54731cb674",
  "container_id": "CONTAINER_001",
  "sensor_type": "temperature",
  "observed_at": "2026-11-12T12:36:49.626Z",
  "value": 3.29,
  "unit": "celsius"
}
```

* Supported `sensor_type` values: `temperature`, `humidity`, `vibration`.
* `schema_version` must equal `1`.
* `event_id` must be a valid lowercase UUIDv4.
* `observed_at` must be a valid UTC ISO-8601 string terminating in `'Z'`.
* Non-conforming payloads are flagged as malformed, logged with partition/offset details, and safely skipped without crashing the consumer.

---

## 3. Reliability & Transaction Guarantees

1. **Idempotence (Snowflake MERGE):**  
   Target table deduplication is enforced on `target.RECORD_CONTENT:event_id::VARCHAR = source.record_content:event_id::VARCHAR`. Re-ingesting the same Kafka message does not duplicate records in Snowflake.
2. **Transaction Ordering (Snowflake-First):**  
   Snowflake transactions are committed **before** Kafka offsets are committed:
   `connection.commit()` $\rightarrow$ `consumer.commit(message=last_msg)`.
3. **Failure Isolation:**  
   If Snowflake encounters a warehouse timeout, network glitch, or SQL failure, the transaction is rolled back and Kafka offsets are **NOT** committed. Messages remain unacknowledged in the broker for retry upon recovery.
4. **Credential Protection:**  
   Passwords and credentials are never printed to console, logs, or diagnostic reports.

---

## 4. Setup & Installation

### A. Dedicated Virtual Environment (Ubuntu WSL)

Activate your Ubuntu WSL virtual environment:
```bash
source /home/darshan_panchal/atmosync-task-a-venv/bin/activate
```

Install ingestion dependencies without disturbing existing environments:
```bash
pip install -r "/mnt/d/Infotact Internship/dbt_atmosync/requirements-ingestion.txt"
```

### B. Configure Snowflake Environment Variables

Export your Snowflake connection variables in your shell (never hardcode):
```bash
export SNOWFLAKE_ACCOUNT="your_account_identifier"
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PASSWORD="your_secure_password"
export SNOWFLAKE_ROLE="ACCOUNTADMIN"
export SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
export SNOWFLAKE_DATABASE="ATMOSYNC_DB"
export SNOWFLAKE_SCHEMA="RAW"
```

---

## 5. Execution Instructions

### A. Run Unit Tests (Mocked Verification)
Run the 24 offline test cases verifying contract validation, metadata generation, MERGE SQL generation, and rollback semantics:
```bash
python -m unittest discover -s "/mnt/d/Infotact Internship/dbt_atmosync/tests" -p "test_*.py" -v
```

### B. Ingest Exactly 27 Kafka Messages (Task A Batch)
To consume exactly 27 messages (corresponding to 3 cycles $\times$ 3 containers $\times$ 3 sensors from Task A simulator) and gracefully exit:
```bash
python "/mnt/d/Infotact Internship/dbt_atmosync/scripts/kafka_to_snowflake_consumer.py" \
  --bootstrap-servers "localhost:9092" \
  --topic "atmosync.telemetry.v1" \
  --group-id "atmosync-snowflake-ingestion-v1" \
  --max-messages 27 \
  --batch-size 10
```

### C. Continuous Ingestion Mode
To run the consumer as a standing streaming service:
```bash
python "/mnt/d/Infotact Internship/dbt_atmosync/scripts/kafka_to_snowflake_consumer.py" \
  --bootstrap-servers "localhost:9092" \
  --topic "atmosync.telemetry.v1" \
  --group-id "atmosync-snowflake-ingestion-v1" \
  --max-messages 0 \
  --batch-size 50
```

---

## 6. Snowflake Verification SQL

Execute these verification queries in Snowflake Worksheets to validate the ingested records:

```sql
-- 1. Verify total row count and distinct event IDs in RAW
USE DATABASE ATMOSYNC_DB;
USE SCHEMA RAW;

SELECT 
    COUNT(*) AS total_rows,
    COUNT(DISTINCT RECORD_CONTENT:event_id::VARCHAR) AS unique_event_ids,
    COUNT(DISTINCT RECORD_CONTENT:container_id::VARCHAR) AS distinct_containers,
    MIN(INGESTED_AT) AS first_ingested_at,
    MAX(INGESTED_AT) AS last_ingested_at
FROM ATMOSYNC_DB.RAW.CONTAINER_TELEMETRY_RAW;

-- 2. Inspect ingested metadata and parsed canonical fields
SELECT 
    RECORD_METADATA:topic::VARCHAR          AS kafka_topic,
    RECORD_METADATA:partition::INT          AS kafka_partition,
    RECORD_METADATA:offset::INT             AS kafka_offset,
    RECORD_METADATA:key::VARCHAR            AS kafka_key,
    RECORD_CONTENT:schema_version::INT      AS schema_version,
    RECORD_CONTENT:event_id::VARCHAR        AS event_id,
    RECORD_CONTENT:container_id::VARCHAR    AS container_id,
    RECORD_CONTENT:sensor_type::VARCHAR     AS sensor_type,
    RECORD_CONTENT:observed_at::TIMESTAMP_TZ AS observed_at,
    RECORD_CONTENT:value::FLOAT             AS sensor_value,
    RECORD_CONTENT:unit::VARCHAR            AS sensor_unit,
    INGESTED_AT                             AS ingested_at
FROM ATMOSYNC_DB.RAW.CONTAINER_TELEMETRY_RAW
ORDER BY INGESTED_AT DESC
LIMIT 27;

-- 3. Verify event distribution across sensor modalities
SELECT 
    RECORD_CONTENT:sensor_type::VARCHAR AS sensor_type,
    RECORD_CONTENT:unit::VARCHAR        AS unit,
    COUNT(*)                            AS reading_count,
    ROUND(AVG(RECORD_CONTENT:value::FLOAT), 2) AS avg_value,
    ROUND(MIN(RECORD_CONTENT:value::FLOAT), 2) AS min_value,
    ROUND(MAX(RECORD_CONTENT:value::FLOAT), 2) AS max_value
FROM ATMOSYNC_DB.RAW.CONTAINER_TELEMETRY_RAW
GROUP BY 1, 2
ORDER BY 1;
```

---

## 7. Next dbt Staging Evolution Recommendation

### The Integration Challenge
The existing model (`models/staging/stg_sensor_readings.sql`) was built for an older, flat 10-field telemetry structure (`temperature, humidity, vibration, latitude, longitude, commodity, quantity_kg` in a single row). 

Task A produces normalized 7-field single-sensor events (`sensor_type`, `value`, `unit`, `observed_at`).

### Recommended Architectural Solutions for Task 3:

#### Option 1: Event-Level Modality Staging (`stg_telemetry_events.sql`) - Recommended
Maintain the normalized event grain in staging. This supports independent sensor cadences and future modalities (e.g., $CO_2$, door sensors):

```sql
{{ config(
    materialized = 'view',
    schema = 'STG'
) }}

WITH source_data AS (
    SELECT
        RECORD_CONTENT:event_id::VARCHAR(100)            AS event_id,
        RECORD_CONTENT:observed_at::TIMESTAMP_TZ         AS observed_at,
        RECORD_CONTENT:container_id::VARCHAR(50)         AS container_id,
        LOWER(TRIM(RECORD_CONTENT:sensor_type::VARCHAR)) AS sensor_type,
        RECORD_CONTENT:value::FLOAT                      AS sensor_value,
        LOWER(TRIM(RECORD_CONTENT:unit::VARCHAR))        AS sensor_unit,
        RECORD_METADATA:topic::VARCHAR                   AS kafka_topic,
        RECORD_METADATA:partition::INT                   AS kafka_partition,
        RECORD_METADATA:offset::INT                      AS kafka_offset,
        INGESTED_AT                                      AS ingested_at
    FROM {{ source('raw_kafka', 'CONTAINER_TELEMETRY_RAW') }}
    WHERE RECORD_CONTENT:event_id IS NOT NULL
)

SELECT *
FROM source_data
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_id 
    ORDER BY ingested_at DESC
) = 1;
```

#### Option 2: Time-Windowed Container Pivot (`stg_container_telemetry_wide.sql`)
For backward compatibility with existing `fct_sensor_telemetry.sql`, pivot readings over a 1-minute time window per container:

```sql
{{ config(
    materialized = 'view',
    schema = 'STG'
) }}

WITH base_events AS (
    SELECT
        RECORD_CONTENT:event_id::VARCHAR                 AS event_id,
        RECORD_CONTENT:container_id::VARCHAR             AS container_id,
        RECORD_CONTENT:sensor_type::VARCHAR              AS sensor_type,
        RECORD_CONTENT:value::FLOAT                      AS val,
        RECORD_CONTENT:observed_at::TIMESTAMP_NTZ        AS observed_at,
        DATE_TRUNC('minute', RECORD_CONTENT:observed_at::TIMESTAMP_NTZ) AS observation_window,
        INGESTED_AT                                      AS ingested_at
    FROM {{ source('raw_kafka', 'CONTAINER_TELEMETRY_RAW') }}
)

SELECT
    MD5(CONCAT(container_id, '_', observation_window))  AS window_id,
    container_id,
    observation_window                                  AS event_timestamp,
    AVG(CASE WHEN sensor_type = 'temperature' THEN val END) AS temperature_celsius,
    AVG(CASE WHEN sensor_type = 'humidity' THEN val END)    AS humidity_pct,
    MAX(CASE WHEN sensor_type = 'vibration' THEN val END)   AS vibration_g,
    MAX(ingested_at)                                    AS ingested_at
FROM base_events
GROUP BY container_id, observation_window;
```

