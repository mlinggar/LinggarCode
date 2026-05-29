with extracted_json as (
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
)

select
    ingested_at_cet,
    file_path,
    city_name,
    latitude,
    longitude,
    temperature_celsius,
    wind_speed_mps,
    weather_condition,
    weather_description,
    case 
        when weather_condition in ('Snow', 'Rain', 'Drizzle') then 'Wet/Slippery'
        when weather_condition = 'Clear' then 'Optimal'
        else 'Normal'
    end as road_surface_implication
from extracted_json
where city_name = 'Stockholm'