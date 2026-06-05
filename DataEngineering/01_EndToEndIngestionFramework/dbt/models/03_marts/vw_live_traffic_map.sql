{{ config(
    materialized='view',
    schema='gold',
    description='A highly optimized, flattened view specifically tailored for Tableau map layers. Outputs native GEOGRAPHY types for instant map recognition.'
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
    -- Route details
    r.route_key,
    r.route_name as display_name,
    
    -- Live Metrics (Cleaned for Tableau Legends)
    coalesce(t.traffic_status, 'Unknown') as traffic_status,
    t.current_speed_kmh as live_speed_kmh,
    coalesce(w.weather_condition, 'Unknown') as current_weather, 
    t.temperature_celsius,
    t.observation_timestamp as last_updated_at,
    
    -- Asset details (Cleaned for Tableau Legends)
    coalesce(o.asset_name, 'No Asset Name') as asset_name,
    coalesce(o.asset_type, 'No Asset') as asset_type,
    coalesce(o.asset_maxspeed, 0) as asset_maxspeed,
    
    -- NATIVE SPATIAL OBJECTS FOR TABLEAU (Removed ST_ASWKT)
    -- Layer 1: Native Geography Point for OSM assets
    to_geography(st_point(o.longitude, o.latitude)) as asset_map_point,
    
    -- Layer 2: Native Geography Line for the physical roads
    to_geography(st_transform(st_geomfromwkt(r.route_geometry_wkt, 3006), 4326)) as route_geometry

from dim_routes r

-- LEFT JOIN: Only keeps Trafikverket routes.
left join dim_osm o 
    on r.route_key = o.route_key

-- Link the live traffic metrics
left join latest_traffic t 
    on r.route_key = t.route_key

-- Link the weather conditions
left join dim_weather w 
    on t.weather_condition_key = w.weather_condition_key