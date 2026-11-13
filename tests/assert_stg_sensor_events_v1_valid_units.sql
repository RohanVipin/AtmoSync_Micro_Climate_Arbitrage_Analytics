-- Singular Test: assert_stg_sensor_events_v1_valid_units
-- Description: Asserts that whenever a sensor value is populated, its corresponding
--              engineering unit strictly matches the Task A canonical contract specifications:
--                - temperature: 'celsius'
--                - humidity: 'percent'
--                - vibration: 'm/s^2'
-- Reference: docs/DATA_CONTRACT.md (Section 5.1 Modality Specification)
-- Returning rows from this query indicates unit inconsistency or contract violation.

SELECT
    measurement_id,
    container_id,
    observed_at,
    temperature,
    temperature_unit,
    humidity,
    humidity_unit,
    vibration,
    vibration_unit
FROM {{ ref('stg_sensor_events_v1') }}
WHERE (temperature IS NOT NULL AND (temperature_unit IS NULL OR LOWER(TRIM(temperature_unit)) != 'celsius'))
   OR (humidity IS NOT NULL AND (humidity_unit IS NULL OR LOWER(TRIM(humidity_unit)) != 'percent'))
   OR (vibration IS NOT NULL AND (vibration_unit IS NULL OR LOWER(TRIM(vibration_unit)) != 'm/s^2'))
