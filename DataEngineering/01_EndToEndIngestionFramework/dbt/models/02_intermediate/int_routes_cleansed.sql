{{ config(materialized='table', schema='silver') }}

select distinct
    md5(route_id) as route_key,
    route_id,
    route_name,
    geometry_sweref99tm as route_geometry_wkt
from {{ ref('stg_trafikverket') }}
where route_id is not null