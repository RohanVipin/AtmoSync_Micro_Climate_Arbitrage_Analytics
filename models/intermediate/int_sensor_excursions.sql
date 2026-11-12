{{ config(
    materialized = 'view',
    schema = 'INT'
) }}

WITH telemetry AS (
    SELECT * FROM {{ ref('stg_sensor_readings') }}
),

thresholds AS (
    SELECT * FROM {{ ref('seed_commodity_thresholds') }}
)

SELECT
    t.event_id,
    t.event_timestamp,
    t.container_id,
    t.commodity,
    COALESCE(th.commodity_name, INITCAP(t.commodity))     AS commodity_name,
    COALESCE(th.category, 'General Cargo')                AS category,
    t.quantity_kg,
    t.temperature_celsius,
    t.humidity_pct,
    t.vibration_g,
    t.latitude,
    t.longitude,
    
    -- Safe tolerance boundaries
    th.temp_min_celsius,
    th.temp_max_celsius,
    th.humidity_min_pct,
    th.humidity_max_pct,
    th.max_vibration_g,
    COALESCE(th.base_price_per_kg_usd, 5.00)              AS base_price_per_kg_usd,
    COALESCE(th.critical_excursion_hours, 24.0)           AS critical_excursion_hours,
    COALESCE(th.spoilage_rate_per_degree_hour, 0.0200)    AS spoilage_rate_per_degree_hour,

    -- Excursion Breaches
    CASE 
        WHEN th.temp_min_celsius IS NOT NULL AND t.temperature_celsius < th.temp_min_celsius THEN 'UNDER_TEMP'
        WHEN th.temp_max_celsius IS NOT NULL AND t.temperature_celsius > th.temp_max_celsius THEN 'OVER_TEMP'
        ELSE 'NORMAL'
    END AS temp_status,

    CASE 
        WHEN th.humidity_min_pct IS NOT NULL AND t.humidity_pct < th.humidity_min_pct THEN 'LOW_HUMIDITY'
        WHEN th.humidity_max_pct IS NOT NULL AND t.humidity_pct > th.humidity_max_pct THEN 'HIGH_HUMIDITY'
        ELSE 'NORMAL'
    END AS humidity_status,

    CASE 
        WHEN th.max_vibration_g IS NOT NULL AND t.vibration_g > th.max_vibration_g THEN TRUE 
        ELSE FALSE 
    END AS is_vibration_shock,

    -- Absolute thermal delta beyond threshold
    GREATEST(
        0,
        COALESCE(t.temperature_celsius - th.temp_max_celsius, 0),
        COALESCE(th.temp_min_celsius - t.temperature_celsius, 0)
    ) AS temp_delta_violation,

    -- Total nominal market value
    ROUND(t.quantity_kg * COALESCE(th.base_price_per_kg_usd, 5.00), 2) AS cargo_nominal_value_usd,

    t.ingested_at

FROM telemetry t
LEFT JOIN thresholds th 
    ON t.commodity = th.commodity

