{{ config(materialized='table', schema='gold') }}

select 
    route_key,
    route_id,
    route_name,
    route_geometry_wkt
from {{ ref('int_routes_cleansed') }}