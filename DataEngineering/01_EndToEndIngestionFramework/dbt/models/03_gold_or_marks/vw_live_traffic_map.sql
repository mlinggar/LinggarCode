{{ config(
    materialized='view',
    schema='gold',
    description='A live, dual-axis view combining traffic telemetry and OSM map assets, connected by route_key.'
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

-- LAYER 1: Traffic Lines (Roads)
traffic_layer as (
    select 
        f.route_key,
        r.route_name as display_name,
        'Road Segment' as map_layer_type,
        
        -- Traffic-specific metrics
        f.traffic_status as traffic_status,
        f.current_speed_kmh as live_speed_kmh,
        w.weather_condition as current_weather,
        
        -- OSM-specific attributes (Null for roads)
        null::string as asset_type,
        null::string as asset_maxspeed,
        
        r.route_geometry_wkt as geometry_wkt,
        f.observation_timestamp as last_updated_at
    from fact_traffic f
    left join dim_routes r on f.route_key = r.route_key
    left join dim_weather w on f.weather_condition_key = w.weather_condition_key
    qualify row_number() over (partition by r.route_id order by f.observation_timestamp desc) = 1
),

-- LAYER 2: OpenStreetMap Points (Cameras & Tolls)
osm_layer as (
    select 
        route_key,
        asset_name as display_name,
        'OSM Asset' as map_layer_type,
        
        -- Traffic-specific metrics (Null for assets)
        null::string as traffic_status,
        null::numeric as live_speed_kmh,
        null::string as current_weather,
        
        -- OSM-specific attributes
        asset_type as asset_type,
        asset_maxspeed as asset_maxspeed,
        
        asset_geometry_wkt as geometry_wkt,
        current_timestamp() as last_updated_at
    from dim_osm
)

select * from traffic_layer
union all
select * from osm_layer