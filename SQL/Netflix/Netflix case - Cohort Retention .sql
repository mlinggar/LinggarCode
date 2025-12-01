/* Cohort retention (signup cohort analysis)
How well watch activity of sign-up cohorts in 6 months after subscribing for 2024? 
*/

--- distinct user with filtering only active user/user who subscribe
with distinct_user as (
    select distinct
        user_id,
        to_date(created_at) as created_at,
        subscription_start_date,
        subscription_plan
    from netflix.public.users
    where is_active = true
        and created_at < subscription_start_date
    order by user_id
),

--- join the distinct user with watch history table
join_watch_history as (
    select
        du.user_id,
        du.created_at,
        du.subscription_start_date,
        wh.watch_date,
        du.subscription_plan
    from distinct_user as du
    inner join netflix.public.watch_history as wh
        on du.user_id = wh.user_id
),

--- create cohort month by the first signup_date
user_first_signup as (
    select 
        user_id,
        min(created_at) as first_signup_date,
        date_trunc('month',min(created_at)) as cohort_month
    from join_watch_history
    group by
        user_id
),

--- create the month of watch activity 
user_watch_activity as (
    select
        af.user_id,
        af.cohort_month,
        date_trunc('month',jw.watch_date) as watch_month
    from user_first_signup as af
    inner join join_watch_history as jw
        on af.user_id = jw.user_id
    group by af.user_id, af.cohort_month, watch_month
),

--- calculate of cohort time elapsed to determine how many months have passed
cohort_time_elapsed as (
    select
        user_id,
        cohort_month,
        watch_month,
        EXTRACT(YEAR FROM watch_month) * 12 + EXTRACT(MONTH FROM watch_month) -
        (EXTRACT(YEAR FROM cohort_month) * 12 + EXTRACT(MONTH FROM cohort_month)) AS month_number
    from user_watch_activity
),

--- count total cohort user
cohort_summary as (
    select
        cohort_month,
        month_number,
        count(distinct user_id) as distinct_users
    from cohort_time_elapsed
    group by cohort_month, month_number
),

--- filtering the total cohort user by very first month the user was active(month_number=0)
cohort_base as (
    select
        cohort_month,
        distinct_users as total_cohort_users
    from cohort_summary
    where month_number = 0
),

--- aggregate the data and “pivot” it to get the familiar cohort table structure
final as (
    select
        cs.cohort_month,
        cb.total_cohort_users,
        max(case when cs.month_number = 1 then (cs.distinct_users * 1.0 / cb.total_cohort_users) end) as retention_month_1,
        max(case when cs.month_number = 2 then (cs.distinct_users * 1.0 / cb.total_cohort_users) end) as retention_month_2,
        max(case when cs.month_number = 3 then (cs.distinct_users * 1.0 / cb.total_cohort_users) end) as retention_month_3,
        max(case when cs.month_number = 4 then (cs.distinct_users * 1.0 / cb.total_cohort_users) end) as retention_month_4,
        max(case when cs.month_number = 5 then (cs.distinct_users * 1.0 / cb.total_cohort_users) end) as retention_month_5,
        max(case when cs.month_number = 6 then (cs.distinct_users * 1.0 / cb.total_cohort_users) end) as retention_month_6
    from cohort_summary as cs
    inner join cohort_base as cb
        on cs.cohort_month = cb.cohort_month
    where cs.cohort_month between '2024-01-01' and '2024-12-01'
    group by cs.cohort_month, cb.total_cohort_users
    order by cs.cohort_month
)

--- run the final result
select *
from final;