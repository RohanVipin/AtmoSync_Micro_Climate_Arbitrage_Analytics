{{ config(
    materialized = 'view',
    schema = none
) }}

/*
    =============================================================================
    Model: stg_sensor_events_v1
    Layer: Staging (STG)
    Project: AtmoSync – Micro-Climate Arbitrage Analytics (Task 2)
    Destination: ATMOSYNC_DB.STG.STG_SENSOR_EVENTS_V1
    
    Description:
      Staging model for Schema Version 1 canonical IoT sensor telemetry events.
      Pivots independent, single-sensor readings (temperature, humidity, vibration)
      into a wide-format dataset grouped strictly by (container_id, observed_at).
      
    Data Source:
      ATMOSYNC_DB.RAW.CONTAINER_TELEMETRY_RAW (source: 'raw_kafka', 'CONTAINER_TELEMETRY_RAW')
      
    Contract Compatibility & Boundary Safety:
      - STRICT FILTER: RECORD_CONTENT:schema_version::INT = 1.
        Historical 10-field records (377 rows with schema_version IS NULL) are completely
        excluded, leaving historical dbt lineage intact and uncorrupted.
      - EXACT GRAIN: Grouped strictly by container_id and observed_at. Readings from
        different containers or observation timestamps are NEVER combined.
      - DETERMINISTIC DEDUPLICATION: QUALIFY ROW_NUMBER() partitions by
        (container_id, observed_at, sensor_type) ordering by ingested_at DESC.
        This guarantees that even if duplicate events exist for the same modality at the
        same observation moment, the latest ingested record is deterministically selected
        rather than MAX() picking an arbitrary numerical maximum.
      - PRECISION SURROGATE KEY: measurement_id uses full nanosecond precision (.FF9)
        with timezone offset (TZHTZM) to prevent key collisions between timestamps.
      - SCHEMA ISOLATION: schema = none prevents dbt from prefixing target.schema (STG),
        ensuring the destination table resolves to ATMOSYNC_DB.STG.STG_SENSOR_EVENTS_V1
        instead of STG_STG.
      - TIMESTAMP JITTER ADVISORY: If sensor readings within a cycle exhibit clock jitter
        (unequal observed_at timestamps), the model emits separate rows with
        sensor_count < 3 rather than silently misjoining data.
    =============================================================================
*/

WITH source_events AS (
    SELECT
        RECORD_CONTENT:event_id::VARCHAR(100)            AS event_id,
        RECORD_CONTENT:schema_version::INT               AS schema_version,
        RECORD_CONTENT:container_id::VARCHAR(50)         AS container_id,
        LOWER(TRIM(RECORD_CONTENT:sensor_type::VARCHAR)) AS sensor_type,
        RECORD_CONTENT:value::FLOAT                      AS sensor_value,
        LOWER(TRIM(RECORD_CONTENT:unit::VARCHAR))        AS sensor_unit,
        RECORD_CONTENT:observed_at::TIMESTAMP_TZ         AS observed_at,
        INGESTED_AT                                      AS ingested_at
    FROM {{ source('raw_kafka', 'CONTAINER_TELEMETRY_RAW') }}
    WHERE RECORD_CONTENT:schema_version::INT = 1
      AND RECORD_CONTENT:event_id IS NOT NULL
      AND RECORD_CONTENT:container_id IS NOT NULL
      AND RECORD_CONTENT:observed_at IS NOT NULL
),

deduped_events AS (
    SELECT *
    FROM source_events
    -- Deduplicate deterministically at the modality grain, retaining latest ingested reading
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY container_id, observed_at, sensor_type
        ORDER BY ingested_at DESC, event_id DESC
    ) = 1
),

pivoted_observations AS (
    SELECT
        -- Deterministic surrogate key preserving full subsecond precision (nanoseconds) & timezone
        MD5(CONCAT_WS('||', container_id, TO_VARCHAR(observed_at, 'YYYY-MM-DD"T"HH24:MI:SS.FF9TZHTZM'))) AS measurement_id,
        container_id,
        observed_at,
        
        -- Pivoted sensor telemetry values via Snowflake IFF conditional aggregation
        MAX(IFF(sensor_type = 'temperature', sensor_value, NULL)) AS temperature,
        MAX(IFF(sensor_type = 'humidity', sensor_value, NULL))    AS humidity,
        MAX(IFF(sensor_type = 'vibration', sensor_value, NULL))   AS vibration,
        
        -- Corresponding engineering units
        MAX(IFF(sensor_type = 'temperature', sensor_unit, NULL))  AS temperature_unit,
        MAX(IFF(sensor_type = 'humidity', sensor_unit, NULL))     AS humidity_unit,
        MAX(IFF(sensor_type = 'vibration', sensor_unit, NULL))    AS vibration_unit,
        
        -- Distinct sensor types present in this exact observation timestamp
        COUNT(DISTINCT sensor_type)                               AS sensor_count,
        
        -- Traceability and ingestion timestamp
        MAX(ingested_at)                                          AS ingested_at
        
    FROM deduped_events
    GROUP BY
        container_id,
        observed_at
)

SELECT
    measurement_id,
    container_id,
    observed_at,
    temperature,
    humidity,
    vibration,
    temperature_unit,
    humidity_unit,
    vibration_unit,
    sensor_count,
    ingested_at
FROM pivoted_observations
