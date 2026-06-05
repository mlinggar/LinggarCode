{{ config(
    materialized='view',
    schema='gold',
    description='A highly optimized, flattened view specifically tailored for Tableau map layers. Updated to enforce WKT spatial formats and full map coverage.'
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
    -- Route details (Safeguarded with COALESCE to capture both Trafikverket AND OpenStreetMap routes)
    coalesce(r.route_key, o.route_key) as route_key,
    coalesce(r.route_name, o.asset_name) as display_name,
    
    -- Live Metrics
    t.traffic_status,
    t.current_speed_kmh as live_speed_kmh,
    w.weather_condition as current_weather,
    t.temperature_celsius,
    t.observation_timestamp as last_updated_at,
    
    -- Asset details
    o.asset_name,
    o.asset_type,
    o.asset_maxspeed,
    
    -- PRE-CALCULATED SPATIAL OBJECTS FOR TABLEAU (Converted to WKT to fix the CSV JSON error)
    -- 1. The point locations for cameras and tolls
    st_aswkt(to_geography(st_point(o.longitude, o.latitude))) as asset_map_point,
    
    -- 2. The line shapes for the actual highways
    st_aswkt(st_transform(st_geomfromwkt(r.route_geometry_wkt, 3006), 4326)) as route_geometry

from dim_routes r
-- FIX 1: Changed from LEFT JOIN to FULL OUTER JOIN. This forces the map to draw ALL OSM streets and assets, even if Trafikverket isn't monitoring them.
full outer join dim_osm o 
    on r.route_key = o.route_key
left join latest_traffic t 
    on coalesce(r.route_key, o.route_key) = t.route_key
left join dim_weather w 
    on t.weather_condition_key = w.weather_condition_key