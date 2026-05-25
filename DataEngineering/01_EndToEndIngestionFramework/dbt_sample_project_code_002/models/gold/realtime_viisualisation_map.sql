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
    -- 1. Spatial Data (The native highway line geometry)
    r.route_geo AS geography_object,
    
    -- 2. Descriptive Identifiers
    t.route_id,
    r.route_name,
    r.regional_division,
    
    -- 3. Core Traffic Performance Metrics (No more nulls!)
    t.speed AS live_speed_kmh,
    t.travel_time AS travel_time_seconds,
    t.traffic_status,
    t.status_color,
    t.last_updated AS metrics_updated_at,
    
    -- 4. Contextual Weather Info
    w.weather_condition,
    w.temp_celsius,
    w.wind_speed,
    t.city_name AS associated_city

FROM fact_traffic t
INNER JOIN dim_routes r 
    ON t.route_id = r.route_id
INNER JOIN dim_weather w 
    ON t.city_name = w.city_name