------------
/* first one: deciding the churn rate*/

-- Create CTE only for distinct user to avoid duplicate
with distinct_user as (
    select distinct
        user_id,
        country,
        subscription_plan,
        to_date(created_at) as created_at,
        subscription_start_date,
        is_active
    from netflix.public.users
    -- filtering sign up account that is before or same day as subscription date
    where created_at <= subscription_start_date
),

-- Create CTE to calculate churn rate percentage
churn_rate as (
    select
        country,
        subscription_plan,
        --count total users, total active user, and inactive user
        count(user_id) as total_user,
        count(distinct iff(is_active = true, user_id, null)) as total_active_user,
        count(distinct iff(is_active = false, user_id, null)) as total_inactive_user,
        -- Cast the type of is_active column from bolean to numeric, so it can be aggregrated by average
            -- where 1 = true and 0 false
        round((1-avg(cast(is_active as numeric))),2) as churn_rate_percentage
    from distinct_user
    group by
        country,
        subscription_plan
)
-- run the all cte
select *
from churn_rate
order by 
    churn_rate_percentage desc
;

--------------
/* plans and countries show lower or higher viewing activity/engagement relative to their subscriber base */

-- Create CTE only for distinct user to avoid duplicate
with distinct_user as (
    select distinct
        user_id,
        country,
        subscription_plan,
        to_date(created_at) as created_at,
        subscription_start_date,
        monthly_spend,
        is_active
    from netflix.public.users
    -- filtering sign up account that is before or same day as subscription date
    where to_date(created_at) <= subscription_start_date
),

-- left join watch_history and distinct_user, to see every user_id from the users table, showing their watch status (even if they haven't watched anything)
join_watch_history as (
    select 
        du.user_id,
        du.country,
        du.subscription_plan,
        wh.watch_duration_minutes,
        du.monthly_spend,
        du.is_active
    from distinct_user as du
    left join netflix.public.watch_history as wh
        on du.user_id = wh.user_id
),

churn_rate as (
    select 
        country,
        subscription_plan,
        --count total users, total active user, and inactive user
        count(distinct user_id) as total_user,
        count(distinct iff(is_active = true, user_id, null)) as total_active_user,
        count(distinct iff(is_active = false, user_id, null)) as total_inactive_user,
         -- Cast the type of is_active column from bolean to numeric, so it can be aggregrated by average
            -- where 1 = true and 0 false
        round((1-avg(cast(is_active as numeric))),2) as churn_rate_percentage,
        -- sum watch_duration_minutes to get total of watch duration for each country and subscription plan
        sum(watch_duration_minutes) as tot_watch_duration,
        -- average monthly_spend to get average of monthly spending for each country and subscription plan
        avg(monthly_spend) as avg_monthly_spend
    from join_watch_history
    group by
        country,
        subscription_plan
),

enggage as (
    select 
        country,
        subscription_plan,
        total_user,
        total_active_user,
        total_inactive_user,
        churn_rate_percentage,
        -- formulation of engagement of each user for watch activity
        (tot_watch_duration/total_user) as engagement_minutes_perusers,
        avg_monthly_spend
    from churn_rate
),

round_watch_duration as (
    select 
        country,
        subscription_plan,
        total_user,
        total_active_user,
        total_inactive_user,
        churn_rate_percentage,
        -- round engagement and average monthly spending results
        round((engagement_minutes_perusers),0) as engagement_minutes_perusers,
        round(avg_monthly_spend,0) as avg_monthly_spend
    from enggage
)

select *
from round_watch_duration
order by 
    churn_rate_percentage desc
;


-- correlation
select 
    round(corr(churn_rate_percentage, avg_monthly_spend),1) as corr_churn_spend,
    round(corr(churn_rate_percentage, engagement_minutes_perusers),1) as corr_churn_engage
from round_watch_duration;

----------
/* analysis of churn rate per watch_date */

-- Create CTE only for distinct user to avoid duplicate
with distinct_user as (
    select distinct
        user_id,
        country,
        subscription_plan,
        to_date(created_at) as created_at,
        subscription_start_date,
        monthly_spend,
        is_active
    from netflix.public.users
    -- filtering sign up account that is before or same day as subscription date
    where to_date(created_at) <= subscription_start_date
),

-- left join watch_history and distinct_user 
join_watch_history as (
    select 
        du.user_id,
        du.country,
        du.subscription_plan,
        -- date_trunc to see the data monthly while keeping the year
        date_trunc(month,wh.watch_date) as watch_date,
        du.is_active
    from distinct_user as du
    left join netflix.public.watch_history as wh
        on du.user_id = wh.user_id
    -- Add filter on country to only Canada and subscription plan to only Standard
    where country = 'Canada'
        and subscription_plan = 'Standard'
),

churn_rate as (
    select 
        watch_date,
        -- count total users, total active user, and inactive user
        count(distinct user_id) as total_user,
        count(distinct iff(is_active = true, user_id, null)) as total_active_user,
        count(distinct iff(is_active = false, user_id, null)) as total_inactive_user,
        -- Cast the type of is_active column from bolean to numeric, so it can be aggregrated by average
            -- where 1 = true and 0 false
        round((1-avg(cast(is_active as numeric))),2) as churn_rate_percentage,
    from join_watch_history
    group by
        watch_date   
)

-- run all cte with select only watch_date & churn_Rate percentage
select 
    watch_date, 
    churn_rate_percentage,
from churn_rate 
order by 
    watch_date asc
;
--------
