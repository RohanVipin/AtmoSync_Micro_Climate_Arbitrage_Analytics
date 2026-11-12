
{{ config(
    materialized = 'incremental',
    unique_key = 'event_id',
    schema = 'MARTS',
    cluster_by = ['DATE(event_timestamp)', 'container_id']
) }}

SELECT
    i.event_id,
    i.event_timestamp,
    DATE(i.event_timestamp) AS event_date,
    i.container_id,
    i.commodity,
    i.commodity_name,
    i.category,
    i.quantity_kg,
    i.temperature_celsius,
    i.humidity_pct,
    i.vibration_g,
    i.latitude,
    i.longitude,
    i.temp_status,
    i.humidity_status,
    i.is_vibration_shock,
    i.temp_delta_violation,
    i.cargo_nominal_value_usd,
    d.spoilage_rate_per_degree_hour,
    i.ingested_at

FROM {{ ref('int_sensor_excursions') }} i

LEFT JOIN {{ ref('dim_commodity') }} d
    ON LOWER(TRIM(i.commodity)) = LOWER(TRIM(d.commodity))

{% if is_incremental() %}
    WHERE i.event_timestamp > (
        SELECT COALESCE(
            MAX(event_timestamp),
            '1970-01-01'::TIMESTAMP_NTZ
        )
        FROM {{ this }}
    )
{% endif %}


