{{ config(
    materialized = 'table',
    schema = 'MARTS'
) }}

WITH excursion_summary AS (
    SELECT
        container_id,
        commodity,
        commodity_name,
        category,
        MAX(event_timestamp)                                                    AS last_reported_at,
        COUNT(event_id)                                                         AS total_readings,
        COUNT(CASE WHEN temp_status != 'NORMAL' THEN 1 END)                    AS temp_breach_count,
        COUNT(CASE WHEN is_vibration_shock THEN 1 END)                          AS vibration_shock_count,
        AVG(temperature_celsius)                                                AS avg_temperature,
        MAX(temperature_celsius)                                                AS max_temperature,
        MIN(temperature_celsius)                                                AS min_temperature,
        SUM(temp_delta_violation)                                               AS cumulative_degree_strain,
        MAX(cargo_nominal_value_usd)                                            AS cargo_nominal_value_usd,
        MAX(spoilage_rate_per_degree_hour)                                      AS spoilage_rate
    FROM {{ ref('fct_sensor_telemetry') }}
    GROUP BY container_id, commodity, commodity_name, category
)

SELECT
    s.container_id,
    s.commodity,
    s.commodity_name,
    s.category,
    s.last_reported_at,
    s.total_readings,
    s.temp_breach_count,
    s.vibration_shock_count,
    ROUND(s.avg_temperature, 2)                                                 AS avg_temperature,
    s.max_temperature,
    s.min_temperature,
    ROUND(s.cumulative_degree_strain, 2)                                        AS cumulative_degree_strain,
    s.cargo_nominal_value_usd,
    
    -- Estimated degradation capped at 100% (1.0)
    ROUND(LEAST(1.0, s.cumulative_degree_strain * s.spoilage_rate), 4)          AS estimated_degradation_pct,
    
    -- Current Salvage Value
    ROUND(
        s.cargo_nominal_value_usd * (1.0 - LEAST(1.0, s.cumulative_degree_strain * s.spoilage_rate)),
        2
    ) AS estimated_salvage_value_usd,

    -- Arbitrage Reroute Recommendation Flag
    CASE
        WHEN (s.cumulative_degree_strain * s.spoilage_rate) >= 0.60 THEN 'IMMEDIATE_LOCAL_SALVAGE'
        WHEN (s.cumulative_degree_strain * s.spoilage_rate) >= 0.25 THEN 'SECONDARY_MARKET_DISCOUNT'
        ELSE 'CONTINUE_TO_PRIMARY_DESTINATION'
    END AS arbitrage_action_flag

FROM excursion_summary s

