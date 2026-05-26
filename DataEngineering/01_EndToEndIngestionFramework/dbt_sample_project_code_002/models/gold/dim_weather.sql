{{ config(materialized='view') }}

SELECT 
    -- PRIMARY KEY
    MD5(UPPER(TRIM(city_name))) AS city_key,
    city_id,
    city_name,
    
    -- METRICS
    temperature AS temp_celsius,
    weather_main AS weather_condition,
    weather_description,
    wind_speed,
    measure_timestamp AS weather_measured_at

FROM {{ source('openweather', 'silver_openweather') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY city_name ORDER BY measure_timestamp DESC) = 1