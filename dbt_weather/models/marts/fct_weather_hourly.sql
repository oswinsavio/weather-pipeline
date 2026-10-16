{{
    config(
        materialized='table'
    )
}}

with staged as (
    select * from {{ ref('stg_weather_observations') }}
)

select 
    city_name,
    country,
    temperature_celsius,
    feels_like_celsius,
    humidity_pct,
    pressure_hpa,
    wind_speed_ms,
    weather_condition,
    observed_at_utc,
    ingested_at,
    record_id,
    current_timestamp as loaded_to_postgres_at
from staged