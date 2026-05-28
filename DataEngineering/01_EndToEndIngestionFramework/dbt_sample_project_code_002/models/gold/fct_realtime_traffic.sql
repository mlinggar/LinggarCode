{{ config(
    materialized='view',
    tags=['road']
) }}

with converted_traffic as (
    select 
        *,
        -- Convert the API measurement time directly to Swedish Time zone upfront
        convert_timezone('Europe/Stockholm', measure_time)::timestamp_ntz as measure_time_se
    from {{ source('trafikverket', 'silver_trafikverket') }}
)

select 
    -- 1. PRIMARY KEY (Using the Swedish timestamp string to build a deterministic key)
    md5(concat(upper(trim(route_id)), '_', to_varchar(measure_time_se, 'YYYYMMDDHH24MISS'))) as traffic_fact_key,

    -- 2. FOREIGN KEYS
    md5(upper(trim(route_id))) as route_key,
    
    md5(upper(trim(
        case 
            when lower(route_name) like '%göteborg%' or lower(route_name) like '%gothenburg%' then 'GOTHENBURG'
            when lower(route_name) like '%skåne%' or lower(route_name) like '%malmö%' then 'MALMO'
            else 'STOCKHOLM'
        end
    ))) as city_key,
    
    -- 3. DEGENERATE DIMENSIONS
    route_id,
    case 
        when lower(route_name) like '%göteborg%' or lower(route_name) like '%gothenburg%' then 'GOTHENBURG'
        when lower(route_name) like '%skåne%' or lower(route_name) like '%malmö%' then 'MALMO'
        else 'STOCKHOLM'
    end as city_name,
    
    -- 4. FACT METRICS
    speed as live_speed_kmh,
    travel_time as travel_time_seconds,
    traffic_status,
    
    case 
        when lower(traffic_status) like '%free%' or lower(traffic_status) like '%normal%' then 'Green'
        when lower(traffic_status) like '%heavy%' or lower(traffic_status) like '%slow%' then 'Yellow'
        when lower(traffic_status) like '%congested%' or lower(traffic_status) like '%blocked%' then 'Red'
        else 'Gray'
    end as status_color,
    
    -- 5. TIMESTAMPS (Reflecting the true Swedish clock time)
    measure_time_se as measure_time,
    to_varchar(measure_time_se, 'YYYY-MM-DD HH24:MI:SS') as metrics_measured_at,
    load_timestamp as dbt_loaded_at

from converted_traffic