{{ config(materialized='view') }}

WITH fact_traffic AS (
    SELECT * FROM {{ ref('fct_realtime_traffic') }}
),
dim_routes AS (
    SELECT * FROM {{ ref('dim_routes') }}
),
dim_weather AS (
    SELECT * FROM {{ ref('dim_weather') }}
),
deduplicated_traffic AS (
    SELECT 
        *,
        -- Row number assigns '1' to the absolute newest record for each unique route
        ROW_NUMBER() OVER (
            PARTITION BY route_id 
            ORDER BY metrics_measured_at DESC, dbt_loaded_at DESC
        ) AS row_num
    FROM fact_traffic
),
latest_traffic AS (
    -- Strictly filter out everything except the single most recent record per route
    SELECT *
    FROM deduplicated_traffic
    WHERE row_num = 1
)

SELECT 
    lt.traffic_fact_key,
    r.route_geo AS geography_object,
    lt.route_id,
    r.route_name,
    r.regional_division,
    r.country_code,
    lt.live_speed_kmh,
    lt.travel_time_seconds,
    lt.traffic_status,
    lt.status_color,
    lt.metrics_measured_at,
    
    w.weather_condition,
    w.temp_celsius,
    w.wind_speed,
    lt.city_name AS associated_city

FROM latest_traffic lt  
INNER JOIN dim_routes r 
    ON lt.route_key = r.route_key
LEFT JOIN dim_weather w 
    ON lt.city_key = w.city_key