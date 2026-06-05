{{ config(
    materialized='view',
    schema='gold',
    description='A highly optimized, flattened view specifically tailored for Tableau map layers. Enforces WKT spatial formats and full map coverage.'
) }}

with fact_traffic as (
    select * from {{ ref('fact_travel_times') }}
),
dim_routes as (
    select * from {{ ref('dim_routes') }}
),
dim_weather as (
    select * from {{ ref('dim_weather_conditions') }}
),
dim_osm as (
    select * from {{ ref('dim_osm_assets') }}
),

-- 1. Grab only the absolute newest snapshot for every road
latest_traffic as (
    select 
        f.route_key,
        f.traffic_status,
        f.current_speed_kmh,
        f.observation_timestamp,
        f.weather_condition_key,
        f.temperature_celsius
    from fact_traffic f
    qualify row_number() over (partition by f.route_key order by f.observation_timestamp desc) = 1
)

select 
    -- Route & Asset Identifiers
    coalesce(r.route_key, o.route_key) as route_key,
    coalesce(r.route_name, o.asset_name) as display_name,
    
    -- Live Metrics
    t.traffic_status,
    t.current_speed_kmh as live_speed_kmh,
    
    -- 🛑 CHANGE THIS LINE HERE:
    -- Replace "weather_condition" with your actual column name (e.g., weather_main, description)
    w.weather_condition as current_weather, 
    
    t.temperature_celsius,
    t.observation_timestamp as last_updated_at,
    
    -- Asset details (OpenStreetMap)
    o.asset_name,
    o.asset_type,
    o.asset_maxspeed,
    
    -- PRE-CALCULATED SPATIAL OBJECTS FOR TABLEAU 
    -- Layer 1: The point locations for OSM assets (cameras, tolls, signs)
    st_aswkt(to_geography(st_point(o.longitude, o.latitude))) as asset_map_point,
    
    -- Layer 2: The line shapes for the physical roads and highways from Trafikverket
    st_aswkt(st_transform(st_geomfromwkt(r.route_geometry_wkt, 3006), 4326)) as route_geometry

from dim_routes r

-- FULL OUTER JOIN: Ensures the map draws ALL OSM assets and ALL Trafikverket routes
full outer join dim_osm o 
    on r.route_key = o.route_key

-- Link the live traffic metrics to whichever road/asset ID exists
left join latest_traffic t 
    on coalesce(r.route_key, o.route_key) = t.route_key

-- Link the weather conditions to the traffic snapshot
left join dim_weather w 
    on t.weather_condition_key = w.weather_condition_key