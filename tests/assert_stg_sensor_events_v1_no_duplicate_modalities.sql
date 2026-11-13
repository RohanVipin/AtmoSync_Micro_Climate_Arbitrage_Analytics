-- Singular Test: assert_stg_sensor_events_v1_no_duplicate_modalities
-- Description: Asserts that no raw v1 ingestion contains duplicate events for the same
--              sensor modality within the same container observation grain (container_id, observed_at).
--              If this query returns rows, multiple distinct events exist for the same sensor_type
--              at the same observation timestamp, which could cause ambiguous aggregation.
-- Reference: Task 2 Architecture & ELT Review Point 3

WITH source_v1_events AS (
    SELECT
        RECORD_CONTENT:event_id::VARCHAR(100)            AS event_id,
        RECORD_CONTENT:container_id::VARCHAR(50)         AS container_id,
        RECORD_CONTENT:observed_at::TIMESTAMP_TZ         AS observed_at,
        LOWER(TRIM(RECORD_CONTENT:sensor_type::VARCHAR)) AS sensor_type,
        INGESTED_AT                                      AS ingested_at
    FROM {{ source('raw_kafka', 'CONTAINER_TELEMETRY_RAW') }}
    WHERE RECORD_CONTENT:schema_version::INT = 1
      AND RECORD_CONTENT:event_id IS NOT NULL
      AND RECORD_CONTENT:container_id IS NOT NULL
      AND RECORD_CONTENT:observed_at IS NOT NULL
),

deduped_by_event_id AS (
    SELECT *
    FROM source_v1_events
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY event_id
        ORDER BY ingested_at DESC
    ) = 1
)

SELECT
    container_id,
    observed_at,
    sensor_type,
    COUNT(*) AS distinct_event_count
FROM deduped_by_event_id
GROUP BY
    container_id,
    observed_at,
    sensor_type
HAVING COUNT(*) > 1

