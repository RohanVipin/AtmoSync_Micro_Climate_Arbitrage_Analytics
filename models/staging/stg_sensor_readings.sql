{{ config(
    materialized = 'view',
    schema = 'STG'
) }}

WITH source_data AS (
    SELECT
        RECORD_CONTENT:event_id::VARCHAR(100)           AS event_id,
        RECORD_CONTENT:event_timestamp::TIMESTAMP_NTZ   AS event_timestamp,
        RECORD_CONTENT:container_id::VARCHAR(50)        AS container_id,
        RECORD_CONTENT:temperature::FLOAT               AS temperature_celsius,
        RECORD_CONTENT:humidity::FLOAT                  AS humidity_pct,
        RECORD_CONTENT:vibration::FLOAT                 AS vibration_g,
        RECORD_CONTENT:latitude::FLOAT                  AS latitude,
        RECORD_CONTENT:longitude::FLOAT                 AS longitude,
        -- Standardize commodity key: lowercase and trimmed for uniform joins
        LOWER(TRIM(RECORD_CONTENT:commodity::VARCHAR))  AS commodity,
        RECORD_CONTENT:quantity_kg::FLOAT               AS quantity_kg,
        INGESTED_AT                                     AS ingested_at
    FROM {{ source('raw_kafka', 'CONTAINER_TELEMETRY_RAW') }}
    WHERE RECORD_CONTENT:event_id IS NOT NULL
)

SELECT
    event_id,
    event_timestamp,
    container_id,
    temperature_celsius,
    humidity_pct,
    vibration_g,
    latitude,
    longitude,
    commodity,
    quantity_kg,
    ingested_at
FROM source_data
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY event_id 
    ORDER BY ingested_at DESC
) = 1

