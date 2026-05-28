select
    ingested_at as ingested_at_cet,
    file_path,
    json_data:name::string as city_name,
    json_data:coord:lat::float as latitude,
    json_data:coord:lon::float as longitude,
    json_data:main:temp::float as temperature_celsius,
    json_data:wind:speed::float as wind_speed_mps,
    json_data:weather[0]:main::string as weather_condition,
    json_data:weather[0]:description::string as weather_description
from {{ source('azure_bronze', 'raw_weather') }}