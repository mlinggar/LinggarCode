{{ config(materialized='view', tags=['weather']) }}

select 
    md5(upper(trim(city_name))) as city_key,
    city_id,
    city_name,
    temperature as temp_celsius,
    weather_main as weather_condition,
    weather_description,
    wind_speed,
    measure_timestamp as weather_measured_at
from {{ source('openweather', 'silver_openweather') }}
qualify row_number() over (partition by city_name order by measure_timestamp desc) = 1