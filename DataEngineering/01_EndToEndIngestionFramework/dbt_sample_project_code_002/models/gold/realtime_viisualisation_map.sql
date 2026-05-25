{{ config(materialized='view') }}

WITH fact_traffic AS (
    SELECT * FROM {{ ref('fct_realtime_traffic') }}
),
dim_routes AS (
    SELECT * FROM {{ ref('dim_routes') }}
),
dim_weather AS (
    SELECT * FROM {{ ref('dim_weather') }}
)

SELECT 
    -- Unique Identifier for Tableau rows
    f.traffic_fact_key,

    -- Spatial Object from Route Dimension
    r.route_geo AS geography_object,
    
    -- Route details from Route Dimension
    f.route_id,
    r.route_name,
    r.regional_division,
    r.country_code,
    
    -- Metrics directly out of the Main Fact Table
    f.live_speed_kmh,
    f.travel_time_seconds,
    f.traffic_status,
    f.status_color,
    f.metrics_measured_at,
    
    -- Atmospheric context from Weather Dimension
    w.weather_condition,
    w.temp_celsius,
    w.wind_speed,
    f.city_name AS associated_city

FROM fact_traffic f
INNER JOIN dim_routes r 
    ON f.route_key = r.route_key
LEFT JOIN dim_weather w 
    ON f.city_key = w.city_key