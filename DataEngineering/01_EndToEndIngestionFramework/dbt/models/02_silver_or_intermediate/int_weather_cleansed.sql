select 
    ingested_at_cet,
    city_name,
    temperature_celsius,
    weather_condition,
    case 
        when weather_condition in ('Snow', 'Rain', 'Drizzle') then 'Wet/Slippery'
        when weather_condition = 'Clear' then 'Optimal'
        else 'Normal'
    end as road_surface_implication
from {{ ref('stg_weather') }}
where city_name = 'Stockholm'