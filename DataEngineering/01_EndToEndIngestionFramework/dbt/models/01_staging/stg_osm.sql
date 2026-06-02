{{ config(materialized='view', schema='bronze') }}

select
    el.value:id::number as osm_id,
    el.value:lat::float as latitude,
    el.value:lon::float as longitude,
    el.value:tags.highway::string as highway_tag,
    el.value:tags.barrier::string as barrier_tag,
    el.value:tags.description::string as asset_description,
    el.value:tags.maxspeed::int as maxspeed,
    ingested_at
from {{ source('traffic_raw', 'raw_osm') }},
lateral flatten(input => json_data:elements) el
where el.value:type::string = 'node'