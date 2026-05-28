select
    ingested_at as ingested_at_cet,
    file_path,
    json_data:extracted_at_utc::timestamp as extracted_at_utc,
    json_data:targeted_region::string as targeted_region,
    -- Flattening the array of cameras and toll gantries
    res.value:id::numeric as osm_id,
    res.value:type::string as element_type,
    res.value:lat::float as latitude,
    res.value:lon::float as longitude,
    res.value:tags as tags_object
from {{ source('azure_bronze', 'raw_osm') }},
lateral flatten(input => json_data:elements) res