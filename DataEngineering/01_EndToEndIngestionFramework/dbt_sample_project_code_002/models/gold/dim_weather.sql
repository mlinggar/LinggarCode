{{ config(materialized='view') }}

WITH ranked_weather AS (
    SELECT 
        city_id,
        UPPER(TRIM(COALESCE(city_name, 'UNKNOWN'))) AS city_name,
        temperature AS temp_celsius,
        weather_main AS weather_condition,
        weather_description,
        wind_speed,
        -- FIXED: Partitioned by city_name instead of third-party city_id to align with backend changes
        ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(COALESCE(city_name, 'UNKNOWN'))) ORDER BY measure_timestamp DESC) as rn
    FROM {{ source('openweather', 'silver_openweather') }}
)

SELECT 
    city_id,
    city_name,
    temp_celsius,
    weather_condition,
    weather_description,
    wind_speed
FROM ranked_weather
WHERE rn = 1