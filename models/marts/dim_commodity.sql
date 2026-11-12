{{ config(
    materialized = 'table',
    schema = 'MARTS'
) }}

SELECT
    commodity,
    commodity_name,
    category,
    temp_min_celsius,
    temp_max_celsius,
    humidity_min_pct,
    humidity_max_pct,
    max_vibration_g,
    base_price_per_kg_usd,
    critical_excursion_hours,
    spoilage_rate_per_degree_hour
FROM {{ ref('seed_commodity_thresholds') }}

