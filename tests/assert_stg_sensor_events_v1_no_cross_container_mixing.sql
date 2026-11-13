-- Singular Test: assert_stg_sensor_events_v1_no_cross_container_mixing
-- Description: Asserts that each pivoted observation belongs to exactly one container
--              and exactly one observation timestamp.
-- Returning rows from this query indicates that readings from multiple containers or timestamps were improperly merged.

SELECT
    measurement_id,
    COUNT(DISTINCT container_id) AS distinct_containers,
    COUNT(DISTINCT observed_at)  AS distinct_timestamps
FROM {{ ref('stg_sensor_events_v1') }}
GROUP BY measurement_id
HAVING COUNT(DISTINCT container_id) > 1
    OR COUNT(DISTINCT observed_at) > 1

