{{ config(materialized='table', schema='gold') }}

with osm_data as (
    select * from {{ ref('int_osm_cleansed') }}
),

routes_data as (
    select * from {{ ref('dim_routes') }}
),

-- Spatial proximity match using atomic Snowflake WKT constructors with explicit SRIDs
spatial_match as (
    select
        o.osm_id,
        o.targeted_region,
        o.asset_type,
        o.asset_name,
        o.asset_maxspeed,
        o.latitude,
        o.longitude,
        r.route_key,
        
        -- Atomic declaration ensures Snowflake never drops the SRID back to 0
        st_distance(
            st_geomfromwkt(r.route_geometry_wkt, 3006),
            st_transform(st_geomfromwkt('POINT(' || o.longitude || ' ' || o.latitude || ')', 4326), 3006)
        ) as distance_meters
        
    from osm_data o
    left join routes_data r
        on st_distance(
            st_geomfromwkt(r.route_geometry_wkt, 3006),
            st_transform(st_geomfromwkt('POINT(' || o.longitude || ' ' || o.latitude || ')', 4326), 3006)
        ) <= 100 -- Match if within 100 meters
        
    -- Keep only the absolute closest road segment for each camera
    qualify row_number() over (partition by o.osm_id order by distance_meters asc nulls last) = 1
)

select
    md5(osm_id::string) as asset_key,
    coalesce(route_key, md5('Unmapped')) as route_key,
    osm_id,
    targeted_region,
    asset_type,
    asset_name,
    asset_maxspeed,
    latitude,
    longitude,
    'POINT(' || longitude || ' ' || latitude || ')' as asset_geometry_wkt
from spatial_match