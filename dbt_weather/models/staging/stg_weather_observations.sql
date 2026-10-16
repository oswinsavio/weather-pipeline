{{ config(materialized='view') }}

with raw_source as (
    -- DuckDB reads directly from your Hive-partitioned Parquet files in the lake!
    select 
        city_name,
        country,
        temperature_celsius,
        feels_like_celsius,
        humidity_pct,
        pressure_hpa,
        wind_speed_ms,
        weather_condition,
        timestamp_utc,
        ingested_at,
        -- Convert UNIX timestamp to a proper SQL timestamp
        to_timestamp(timestamp_utc) as observed_at_utc
    from read_parquet('../data/lake/bronze/weather/dt=*/*.parquet', hive_partitioning=true)
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
    -- Create a unique surrogate key for deduplication
    city_name || '_' || observed_at_utc as record_id
from raw_source
qualify row_number() over (partition by city_name, observed_at_utc order by ingested_at desc) = 1