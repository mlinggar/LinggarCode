select
    ingested_at as ingested_at_cet,
    file_path,
    json_data:extracted_at_utc::timestamp as extracted_at_utc,
    json_data:targeted_region::string as targeted_region,
    json_data:remark::string as api_status_remark
from {{ source('azure_bronze', 'raw_osm') }}