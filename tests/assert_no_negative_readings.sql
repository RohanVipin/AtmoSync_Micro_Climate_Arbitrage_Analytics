-- Singular test: Cargo quantities and vibration readings must be non-negative
SELECT
    event_id,
    container_id,
    quantity_kg,
    vibration_g
FROM {{ ref('stg_sensor_readings') }}
WHERE quantity_kg < 0 
   OR vibration_g < 0

