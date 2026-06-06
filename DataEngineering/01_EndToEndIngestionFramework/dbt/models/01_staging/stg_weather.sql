{{ config(materialized='view', schema='bronze') }}

select
    json_data:dt::int as dt_unix,
    convert_timezone('UTC', 'Europe/Stockholm', to_timestamp(json_data:dt::int)) as observation_timestamp,
    coalesce(json_data:weather[0].main::string, 'Unknown') as weather_condition,
    json_data:weather[0].description::string as weather_description,
    json_data:main.temp::float as temperature_celsius,
    json_data:main.humidity::int as humidity_percentage,
    json_data:wind.speed::float as wind_speed_mps,
    ingested_at
from {{ source('traffic_raw', 'raw_weather') }}