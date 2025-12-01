/*
Peak day planning & device mix by country
What are peak viewing day in week per country and device type
*/

--- distinct user with filtering only active user
with distinct_users as (
    select distinct
        user_id,
        country,
        is_active,
        primary_device
    from netflix.public.users
    where is_active = TRUE
),

--- join distinct_users with watch history with filtering device type = primary type
userlogs_history as (
    select
        du.country,
        wh.device_type,
        du.is_active,
        dayname(to_date(wh.watch_date)) as day_name,
        wh.watch_duration_minutes,
        wh.progress_percentage
    from distinct_users as du
    inner join netflix.public.watch_history as wh
        on du.user_id = wh.user_id
    where wh.device_type = du.primary_device
),

--- create new cte with spesific column
new_userlogs_history as (
    select
        country,
        device_type,
        day_name
    from userlogs_history
),


--- ranked total number of each country, device type, and day of week
ranked as (
    select
        country,
        device_type,
        day_name,
        count(*) as total,
        dense_rank() over(partition by country, device_type order by total desc) as rank
    from new_userlogs_history
    group by 
        country,
        device_type,
        day_name
),

--- create new cte that only show number 1 rank of each device type every day of week by grouping with country
final as (
    select
        country,
        listagg(iff(device_type = 'Laptop', day_name, null), ', ') within group (order by day_name) as laptop,
        listagg(iff(device_type = 'Mobile', day_name, null), ', ') within group (order by day_name) as mobile,
        listagg(iff(device_type = 'Tablet', day_name, null), ', ') within group (order by day_name) as tablet,
        listagg(iff(device_type = 'Desktop', day_name, null), ', ') within group (order by day_name) as desktop,
        listagg(iff(device_type = 'Smart TV', day_name, null), ', ') within group (order by day_name) as smart_tv
    from ranked
    where rank = 1
    group by country
    order by country asc
)

--- show the final result
select *
from final;

------------

with watch_hist as(

    select
        session_id,
        watch_date,
        watch_duration_minutes,
        location_country,
        device_type
    from netflix.public.watch_history

),

agg as (

    select
        count(session_id) as n_sessions,
        sum(watch_duration_minutes) as total_watch_duration_minutes,
        dayname(watch_date) as day_in_week,
        location_country,
        device_type
    from watch_hist
    group by
        day_in_week,
        location_country,
        device_type
),

peak_day as (

    select
        *,
        dense_rank() over (
            partition by location_country
            order by n_sessions desc
        ) as peak_rank_sessions,
      /*  dense_rank() over (
            partition by location_country, device_type
            order by total_watch_duration_minutes desc
        ) as peak_rank_duration
    */
    from agg
)

select *
from peak_day
order by
    location_country,
    peak_rank_sessions,
    device_type;


------------

/*
Netflix data analysis
1.	Peak hour planning & device mix by country
	What are peak viewing hours per country and device type
Watch_history contains dates, devices and countries, but NOT times
*/
-- CTE returning distinct records from watch_history containing required fields
WITH DistinctWatchHistories AS (
    SELECT
        DISTINCT session_id,
        device_type,
        watch_date,
        location_country
    FROM
        NETFLIX.public.WATCH_HISTORY
    WHERE
        watch_date IS NOT NULL
),
-- CTE calculating weekday and counting daily totals
-- DAYNAME only returns Mon, Tues etc so used DECODE(EXTRACT) to get fullname Monday, Tuesday etc
DayActivity AS (
    SELECT
        location_country AS country,
        device_type AS device,
        DECODE(
            EXTRACT('dayofweek_iso', watch_date),
            1,'Monday',
            2,'Tuesday',
            3,'Wednesday',
            4,'Thursday',
            5,'Friday',
            6,'Saturday',
            7,'Sunday'
        ) AS weekday,
        COUNT(*) AS total_activity
    FROM
        DistinctWatchHistories AS dwh
    GROUP BY
        location_country,
        device_type,
        DECODE(
            EXTRACT('dayofweek_iso', watch_date),
            1,'Monday',
            2,'Tuesday',
            3,'Wednesday',
            4,'Thursday',
            5,'Friday',
            6,'Saturday',
            7,'Sunday'
        )
),
-- CTE ranking returned records and calculating percentages
RankedDays AS (
    SELECT
        country,
        device,
        weekday,
        total_activity,
        ROW_NUMBER() OVER (
            PARTITION BY country,device
            ORDER BY total_activity DESC
        ) AS ranked_activity,
        ROW_NUMBER() OVER (
            PARTITION BY country
            ORDER BY total_activity DESC
        ) AS country_device_rank,
        SUM(total_activity) OVER (PARTITION BY country) AS country_total,
        100 * SUM(total_activity) OVER (PARTITION BY country) / SUM(total_activity) OVER () AS pct_country_of_overall_total,
        100 * RATIO_TO_REPORT(total_activity) OVER (PARTITION BY country, device) AS pct_of_country_device_day,
        100 * RATIO_TO_REPORT(total_activity) OVER (PARTITION BY country) AS pct_of_country,
        100 * RATIO_TO_REPORT(total_activity) OVER () AS pct_of_total,
        100 * SUM(total_activity) OVER (PARTITION BY country) / SUM(total_activity) OVER () AS country_pct_of_overall_total
    FROM
        DayActivity AS dac
)
-- Ordering and limiting records to desired number
SELECT
    country,
    device,
    weekday,
    ROUND(pct_country_of_overall_total, 2) AS pct_country_share_of_total,
    ROUND(pct_of_country_device_day, 2) AS pct_day_share_of_country_device,
    ROUND(pct_of_country, 2) AS pct_of_country_total,
    ROUND(pct_of_total, 2) AS pct_overall_total
FROM
    RankedDays QUALIFY ranked_activity <= 2 -- a value of 2 here returns 20 records, the total possible records is 70
ORDER BY
    country_total DESC,
    country_device_rank ASC,
    device ASC,
    ranked_activity ASC;