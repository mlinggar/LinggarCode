{{ config(materialized='view') }}

WITH ranked_weather AS (
    SELECT 
        city_id,
        city_name,
        temperature AS temp_celsius,
        weather_main AS weather_condition,
        weather_description,
        wind_speed,
        ROW_NUMBER() OVER (PARTITION BY city_id ORDER BY measure_timestamp DESC) as rn
    FROM DEV_SILVER.OPENWEATHER.SILVER_OPENWEATHER
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