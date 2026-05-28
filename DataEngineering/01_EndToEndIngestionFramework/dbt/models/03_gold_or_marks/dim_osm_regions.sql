select distinct
    md5(targeted_region) as region_key,
    targeted_region,
    provider_health_status,
    api_status_remark as last_known_remark,
    max(ingested_at_cet) over (partition by targeted_region) as last_updated_cet
from {{ ref('int_osm_cleansed') }}