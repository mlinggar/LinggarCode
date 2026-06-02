{{ config(materialized='table', schema='silver') }}

select
    md5(weather_condition) as weather_condition_key,
    date_trunc('hour', observation_timestamp) as observation_hour,
    weather_condition,
    temperature_celsius,
    wind_speed_mps,
    case 
        when weather_condition ilike '%Rain%' then 'Slippery'
        when weather_condition ilike '%Snow%' then 'Hazardous'
        else 'Normal'
    end as road_surface_implication
from {{ ref('stg_weather') }}
-- Get latest weather per hour to avoid duplicates
qualify row_number() over (partition by date_trunc('hour', observation_timestamp) order by observation_timestamp desc) = 1