{{ config(
    materialized = 'table',
    schema = 'MARTS'
) }}

WITH containers_from_telemetry AS (
    SELECT
        container_id,
        MIN(event_timestamp) AS first_seen_at,
        MAX(event_timestamp) AS last_seen_at,
        COUNT(DISTINCT commodity) AS distinct_commodities_transported,
        MAX(quantity_kg) AS peak_payload_kg
    FROM {{ ref('stg_sensor_readings') }}
    GROUP BY container_id
)

SELECT
    container_id,
    first_seen_at,
    last_seen_at,
    distinct_commodities_transported,
    peak_payload_kg
FROM containers_from_telemetry

