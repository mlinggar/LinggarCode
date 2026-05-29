with flattened_osm as (
    select
        ingested_at as ingested_at_cet,
        file_path,
        json_data:extracted_at_utc::timestamp as extracted_at_utc,
        json_data:targeted_region::string as targeted_region,
        res.value:id::numeric as osm_id,
        res.value:type::string as element_type,
        res.value:lat::float as latitude,
        res.value:lon::float as longitude,
        res.value:tags as tags_object
    from {{ source('azure_bronze', 'raw_osm') }},
    lateral flatten(input => json_data:elements) res
)

select
    ingested_at_cet,
    file_path,
    extracted_at_utc,
    targeted_region,
    osm_id,
    element_type,
    latitude,
    longitude,
    coalesce(tags_object:highway::string, 'unknown') as asset_type,
    coalesce(tags_object:name::string, tags_object:description::string, 'Unnamed Asset') as asset_name,
    tags_object:maxspeed::string as asset_maxspeed
from flattened_osm
where osm_id is not null