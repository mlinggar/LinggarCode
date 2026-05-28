select
    ingested_at_cet,
    extracted_at_utc,
    targeted_region,
    api_status_remark,
    -- Business Logic: Flag whether the API call was successful or threw a runtime error
    case 
        when api_status_remark like '%error%' then 'Failed'
        else 'Healthy'
    end as provider_health_status
from {{ ref('stg_osm') }}
where targeted_region is not null