-- Singular Test: assert_stg_sensor_events_v1_sensor_count
-- Description: Validates that sensor_count exactly equals the number of non-null
--              pivoted sensor readings (temperature, humidity, vibration) and remains
--              within the valid domain [1, 3].
-- Returning rows from this query indicates a test failure.

SELECT
    measurement_id,
    container_id,
    observed_at,
    sensor_count,
    (IFF(temperature IS NOT NULL, 1, 0) +
     IFF(humidity IS NOT NULL, 1, 0) +
     IFF(vibration IS NOT NULL, 1, 0)) AS calculated_sensor_count
FROM {{ ref('stg_sensor_events_v1') }}
WHERE sensor_count != (
    IFF(temperature IS NOT NULL, 1, 0) +
    IFF(humidity IS NOT NULL, 1, 0) +
    IFF(vibration IS NOT NULL, 1, 0)
)
OR sensor_count < 1
OR sensor_count > 3

