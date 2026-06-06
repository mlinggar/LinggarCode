{{ config(materialized='table', schema='gold') }}

with spatial_matching as (
    select
        o.asset_key,
        o.osm_id,
        o.asset_name,
        o.asset_type,
        o.asset_maxspeed,
        o.latitude,
        o.longitude,
        r.route_key
    from {{ ref('int_osm_cleansed') }} o
    left join {{ ref('int_routes_cleansed') }} r
        on st_dwithin(
            st_point(o.longitude, o.latitude), 
            to_geography(st_aswkt(st_transform(st_geomfromwkt(r.route_geometry_wkt, 3006), 4326))), 
            50
        )
    qualify row_number() over (partition by o.asset_key order by r.route_key) = 1
)

select * from spatial_matching

union all

select
    md5('No Asset') as asset_key,
    0 as osm_id,
    'No Associated Asset' as asset_name,
    'None' as asset_type,
    0 as asset_maxspeed,
    0.0 as latitude,
    0.0 as longitude,
    'None' as route_key