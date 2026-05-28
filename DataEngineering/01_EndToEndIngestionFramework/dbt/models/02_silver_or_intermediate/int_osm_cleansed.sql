select
    ingested_at_cet,
    extracted_at_utc,
    targeted_region,
    osm_id,
    latitude,
    longitude,
    coalesce(tags_object:highway::string, 'unknown') as asset_type,
    -- Safely grab the name whether it's a camera or a toll
    coalesce(tags_object:name::string, tags_object:description::string, 'Unnamed Asset') as asset_name,
    tags_object:maxspeed::string as asset_maxspeed
from {{ ref('stg_osm') }}
where osm_id is not null