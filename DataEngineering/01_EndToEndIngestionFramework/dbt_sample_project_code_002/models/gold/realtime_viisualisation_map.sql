{{ config(materialized='view') }}

WITH fact_traffic AS (
    SELECT * FROM {{ ref('fct_realtime_traffic') }} -- Replace with the actual name of your traffic fact model
),
dim_routes AS (
    SELECT * FROM {{ ref('dim_routes') }} -- Replace with the actual name of your routes model
),
dim_weather AS (
    SELECT * FROM {{ ref('dim_weather') }} -- Replace with the actual name of your weather model
),
bridge_assets AS (
    SELECT * FROM {{ ref('bridge_route_assets') }} -- Replace with the actual name of your bridge model
),
dim_assets AS (
    SELECT * FROM {{ ref('dim_map_assets') }} -- Replace with the actual name of your map assets model
)

SELECT 
    -- 1. Spatial Data (For mapping)
    a.latitude,
    a.longitude,
    
    -- 2. Route Information
    t.route_id,
    r.route_name,
    a.asset_label,
    
    -- 3. Live Traffic Metrics
    t.speed AS live_speed_kmh,
    t.travel_time AS travel_time_seconds,
    t.traffic_status,
    t.status_color,
    t.last_updated AS traffic_updated_at,
    
    -- 4. Live Weather Metrics
    w.weather_condition,
    w.temp_celsius,
    w.wind_speed
    
FROM fact_traffic t
-- INNER JOIN ensures ONLY routes with live fact data make it to the map
INNER JOIN dim_routes r 
    ON t.route_id = r.route_id
-- Get the hardware/nodes associated with the active routes
INNER JOIN bridge_assets b 
    ON t.route_id = b.route_id
-- Get the exact coordinates for those nodes
INNER JOIN dim_assets a 
    ON b.node_id = a.node_id
-- Attach the live weather for the city
INNER JOIN dim_weather w 
    ON t.city_id = w.city_id