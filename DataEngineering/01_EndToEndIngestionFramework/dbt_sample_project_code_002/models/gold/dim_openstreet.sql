{{ config(materialized='view', tags=['osm']) }}

select distinct
    md5(cast(node_id as varchar)) as osm_asset_key,
    node_id,
    latitude,
    longitude,
    asset_type,
    operator,
    asset_geo
from {{ source('openstreetmap', 'silver_openstreetmap') }}