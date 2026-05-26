{{ config(
    materialized='view',
    tags=['weather']
) }}

with converted_weather as (
    select
        *,
        -- Convert the weather measurement time to Swedish Time zone upfront
        convert_timezone('Europe/Stockholm', measure_timestamp)::timestamp_ntz as measure_timestamp_se
    from {{ source('openweather', 'silver_openweather') }}
)

select 
    md5(upper(trim(city_name))) as city_key,
    city_id,
    city_name,
    temperature as temp_celsius,
    weather_main as weather_condition,
    weather_description,
    wind_speed,
    measure_timestamp_se as weather_measured_at
from converted_weather
-- Ensures we pick the absolute latest Swedish-timestamped record per city
qualify row_number() over (partition by city_name order by measure_timestamp_se desc) = 1