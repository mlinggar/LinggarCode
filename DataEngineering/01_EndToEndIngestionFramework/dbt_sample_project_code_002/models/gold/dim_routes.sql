{{ config(materialized='view', tags=['road']) }}

select 
    md5(upper(trim(route_id))) as route_key,
    route_id,
    route_geo,
    route_name,
    country_code,
    case 
        when lower(route_name) like '%göteborg%' or lower(route_name) like '%gothenburg%' then 'Gothenburg Region'
        when lower(route_name) like '%skåne%' or lower(route_name) like '%malmö%' then 'Skåne Region'
        else 'Stockholm Region'
    end as regional_division
from {{ source('trafikverket', 'silver_trafikverket') }}
qualify row_number() over (partition by route_id order by measure_time desc) = 1