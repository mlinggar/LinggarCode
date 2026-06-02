{{ config(materialized='table', schema='silver') }}

select distinct
    md5(osm_id::string) as asset_key,
    osm_id,
    coalesce(highway_tag, barrier_tag) as asset_type,
    coalesce(asset_description, 'Unknown Asset') as asset_name,
    maxspeed as asset_maxspeed,
    latitude,
    longitude
from {{ ref('stg_osm') }}
where highway_tag in ('speed_camera', 'toll_gantry')
   or barrier_tag = 'toll_booth'