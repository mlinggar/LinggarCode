-- 1st query

-- create age group with distinct user
with user_agegroup as (
        select distinct
            user_id,
            age,
            case
                when age between 1 and 13 then 'child'
                when age between 14 and 18 then 'teen'
                when age between 19 and 34 then 'young adult'
                when age between 35 and 65 then 'adult'
                when age > 65 then 'senior'
                else 'unknown'
            end as agegroup,
        from netflix.public.users
),

agegroup_history as (
        select
            ua.user_id,
            wh.movie_id,
            ua.agegroup,
            wh.watch_duration_minutes,
            wh.progress_percentage
        from user_agegroup as ua
        inner join netflix.public.watch_history as wh
            on ua.user_id = wh.user_id
),
--- join watch_history & movies table
movie_watch as (
        select
            ah.user_id,
            ah.agegroup,
            m.title,
            m.genre_primary,
            m.duration_minutes,
            ah.watch_duration_minutes,
            ah.progress_percentage
        from agegroup_history as ah
        inner join netflix.public.movies as m 
            on ah.movie_id = m.movie_id  
),

--- aggregate watch duration and progress percentage by average
avg_agg as (
        select
            user_id,
            agegroup,
            title,
            genre_primary,
            duration_minutes,
            watch_duration_minutes,
            progress_percentage,
            avg(progress_percentage) over(
                     partition by title 
            ) as avg_progpercentage_title,
            avg(progress_percentage) over(
                     partition by title, agegroup
            ) as avg_progpercentage_title_agegroup,
            avg(progress_percentage) over (
                         partition by genre_primary
            ) as avg_progpercentage_genre,
            avg(progress_percentage) over (
                         partition by genre_primary, agegroup
            ) as avg_progpercentage_genre_agegroup
        from movie_watch
),

agg as (
        select
            count(user_id) as total_userid,
            agegroup,
            title,
            genre_primary,
            duration_minutes,
            progress_percentage,
            avg_progpercentage_title,
            avg_progpercentage_title_agegroup,
            avg_progpercentage_genre,
            avg_progpercentage_genre_agegroup
        from avg_agg
        group by
            agegroup,
            title,
            genre_primary,
            duration_minutes,
            progress_percentage,
            avg_progpercentage_title,
            avg_progpercentage_title_agegroup,
            avg_progpercentage_genre,
            avg_progpercentage_genre_agegroup
),

ranked as (
        select
            total_userid,
            agegroup,
            title,
            genre_primary,
            duration_minutes,
            progress_percentage,
            avg_progpercentage_title,
            avg_progpercentage_title_agegroup,
            avg_progpercentage_genre,
            avg_progpercentage_genre_agegroup,
            dense_rank() over(order by avg_progpercentage_title) as rank_progpercentage_title,
            dense_rank() over(order by avg_progpercentage_genre) as rank_progpercentage_genre,
            dense_rank() over(partition by genre_primary order by avg_progpercentage_title) as rank_progpercentage_genre_title
        from agg
),

ca as (
        select
            total_userid,
            agegroup,
            title,
            genre_primary,
            duration_minutes,
            progress_percentage,
            avg_progpercentage_title,
            avg_progpercentage_title_agegroup,
            avg_progpercentage_genre,
            avg_progpercentage_genre_agegroup,
            round(div0(avg_progpercentage_title_agegroup, avg_progpercentage_title),2) as comple_rate_title,
            round(div0(avg_progpercentage_genre_agegroup, avg_progpercentage_genre),2) as comple_rate_genre,
            rank_progpercentage_genre_title
        from ranked
)

select *
from ca
where progress_percentage is not null
group by all
order by
    rank_progpercentage_genre_title asc,
    total_userid desc;


---------

-- 2nd queries

--- select distinct user from user table

with distinct_users as (
        select distinct
            user_id,
            age
        from users   
    ),

---  create age group by joining distinct_users with watch_history

user_watch_history as (
        select
            wh.user_id,
            wh.movie_id,
            case
                when u.age between 1 and 13 then 'child'
                when u.age between 14 and 18 then 'teen'
                when u.age between 19 and 34 then 'young adult'
                when u.age between 35 and 65 then 'adult'
                when u.age > 65 then 'senior'
                else 'unknown'
            end as agegroup,
            wh.watch_duration_minutes,
            wh.progress_percentage
        from distinct_users as u
        inner join watch_history as wh
            on u.user_id = wh.user_id
),

--- join user_watch_history with movies table

age_group_genre as(
        select
            uw.agegroup,
            m.title,
            m.genre_primary,
            m.duration_minutes,
            uw.watch_duration_minutes,
            uw.progress_percentage
        from user_watch_history as uw
        inner join movies as m 
             on uw.movie_id = m.movie_id
),

--- aggregate watch_duration_minutes with watch duration minutes and progress percentage by genre (window function)

aggwindow_agegroup as (
        select
             agegroup,
             title,
             genre_primary,
             duration_minutes,
             watch_duration_minutes,
             progress_percentage,
             avg(watch_duration_minutes) over(
                                    partition by genre_primary
             ) as avg_watch_genre,
             avg(progress_percentage) over(
                                    partition by genre_primary
             ) as avg_prog_genre
        from age_group_genre

),

--- aggregate average watch duration minutes and progress percentage

avg_all as (
        select
            agegroup,
            title,
            genre_primary,
            duration_minutes,
            avg_watch_genre,
            avg_prog_genre
        from aggwindow_agegroup 
        group by
            agegroup,
            title,
            genre_primary,
            duration_minutes,
            avg_watch_genre,
            avg_prog_genre
),

--- calculate the completion rate
completation_rate as (
        select
            agegroup,
            title,
            genre_primary,
            duration_minutes,
            round(div0(avg_watch_genre, duration_minutes),2) as comple_rate,
            round(avg_prog_genre,2) as avg_prog_genre
        from avg_all
)

select *
from completation_rate
order by comple_rate desc;

--- check duplicate
select count(*),
        agegroup,
        title,
        genre_primary,
        duration_minutes,
        comple_rate,
        avg_prog_genre,
from completation_rate
group by all
having count(*) > 1
;

/*
Title-level completion / engagement (which content keeps viewers watching?)
Which titles and genres have the highest completion rate and are they consistent across age groups?
*/

with distinct_users as (
-- since the user table has duplicates we need to do a seperated cte where we only select distinct users
    select distinct
        user_id,
        age,
        -- age categories
        case
            when age between 1 and 13 
                then 'child'
            when age between 14 and 18 
                then 'teen'
            when age between 19 and 34 
                then 'young adult'
            when age between 35 and 65 
                then 'adult'
            when age > 65 then 'senior'
            else 'unknown'
        end as age_group
    from netflix.public.users
    
),

watch_history as (
    
    select
        wh.user_id,
        u.age_group,
        wh.movie_id,
        m.title,
        m.genre_primary,
        m.duration_minutes,
        wh.watch_date,
        wh.progress_percentage,
        wh.watch_duration_minutes, 
    from netflix.public.watch_history as wh
    inner join distinct_users as u
        on wh.user_id = u.user_id
    inner join netflix.public.movies as m
        on wh.movie_id = m.movie_id
    -- excluding rows where we lack information on the completion percentage
    where progress_percentage is not null

),

averages as (
    select
        user_id,
        age_group,
        title,
        genre_primary,
        duration_minutes,
        watch_date,
        watch_duration_minutes,
        progress_percentage,
        avg(progress_percentage) over (
            partition by title
        ) as avg_progress_percentage_title,
        avg(progress_percentage) over (
            partition by title, age_group
        ) as avg_progress_percentage_title_age_group,
        avg(progress_percentage) over (
            partition by genre_primary
        ) as avg_progress_percentage_genre_primary,
        avg(progress_percentage) over (
            partition by genre_primary
        ) as avg_progress_percentage_genre_primary_age_group
    from watch_history
),

agg as (

    select
        count(user_id) as n_users,
        age_group,
        title,
        genre_primary,
        duration_minutes,
        avg_progress_percentage_title,
        avg_progress_percentage_title_age_group,
        avg_progress_percentage_genre_primary,
        avg_progress_percentage_genre_primary_age_group
    from averages
    group by
        age_group,
        title,
        genre_primary,
        duration_minutes,
        avg_progress_percentage_genre_primary,
        avg_progress_percentage_title_age_group,
        avg_progress_percentage_title,
        avg_progress_percentage_genre_primary_age_group
),

ranking as (
    select
        n_users,
        age_group,
        title,
        genre_primary,
        duration_minutes,
        avg_progress_percentage_title,
        avg_progress_percentage_title_age_group,
        avg_progress_percentage_genre_primary,
        avg_progress_percentage_genre_primary_age_group,
        dense_rank() over (
            order by avg_progress_percentage_title desc
        ) as title_rank,
        dense_rank() over (
            order by avg_progress_percentage_genre_primary desc
        ) as genre_rank,
        dense_rank() over (
            partition by genre_primary
            order by avg_progress_percentage_title desc
        ) as title_rank_within_genre
    from agg
),

age_group_diff as (
    select
        n_users,
        age_group,
        title,
        genre_primary,
        duration_minutes,
        avg_progress_percentage_title,
        avg_progress_percentage_title_age_group,
        div0(avg_progress_percentage_title_age_group, avg_progress_percentage_title) age_group_diff_title,
        avg_progress_percentage_genre_primary,
        avg_progress_percentage_genre_primary_age_group,
        div0(avg_progress_percentage_genre_primary_age_group, avg_progress_percentage_genre_primary) age_group_diff_genre,
        title_rank,
        genre_rank,
        title_rank_within_genre
    from ranking
)
-- checking top title and difference in age group
select *
from age_group_diff
order by
    avg_progress_percentage_title desc,
    avg_progress_percentage_title_age_group desc;


/*
Title-level completion / engagement (which content keeps viewers watching?)
Which titles and genres have the highest completion rate and are they consistent across age groups?
*/

with distinct_users as (
-- since the user table has duplicates we need to do a seperated cte where we only select distinct users
    select distinct
        user_id,
        age,
        -- age categories
        case
            when age between 1 and 13 
                then 'child'
            when age between 14 and 18 
                then 'teen'
            when age between 19 and 34 
                then 'young adult'
            when age between 35 and 65 
                then 'adult'
            when age > 65 then 'senior'
            else 'unknown'
        end as age_group
    from netflix.public.users
    
),

watch_history as (
    
    select
        wh.user_id,
        u.age_group,
        wh.movie_id,
        m.title,
        m.genre_primary,
        m.duration_minutes,
        wh.watch_date,
        wh.progress_percentage,
        wh.watch_duration_minutes, 
    from netflix.public.watch_history as wh
    inner join distinct_users as u
        on wh.user_id = u.user_id
    inner join netflix.public.movies as m
        on wh.movie_id = m.movie_id
    -- excluding rows where we lack information on the completion percentage
    where progress_percentage is not null
),

ranked_total as (    
    select
        age_group,
        title,
        genre_primary,
        progress_percentage,
        count(age_group,title,genre_primary) as total,
        dense_rank() over(
            partition by age_group,genre_primary 
            order by total desc) 
        as rank_total_agegroup_genre    
    from watch_history
    group by
        age_group,
        title,
        genre_primary,
        progress_percentage
),

top_rank_title_genre as (
    select distinct
        age_group,
        title,
        genre_primary,
        progress_percentage,
    from ranked_total
    where rank_total_agegroup_genre = 1
),

avg_progress_percentage as (
    select
        age_group,
        title,
        genre_primary,
        progress_percentage,
        avg(progress_percentage) over(
            partition by genre_primary,age_group) 
        as avg_progpercentag_agegroup_genre,
    from top_rank_title_genre
),

rank_avg_prog as (
    select
        age_group,
        title,
        genre_primary,
        avg_progpercentag_agegroup_genre,
        dense_rank() over(
            partition by age_group,genre_primary 
            order by avg_progpercentag_agegroup_genre desc) 
        as rank_avg
    from avg_progress_percentage
),

top_rank_avg as (
    select distinct
        age_group,
        title,
        genre_primary,
        avg_progpercentag_agegroup_genre
    from rank_avg_prog
    where rank_avg = 1
),

final as (
    select
        age_group,
        listagg(iff(genre_primary = 'Sport', title, null), ', ') within group (order by title) as Sport,
        listagg(iff(genre_primary = 'Romance', title, null), ', ') within group (order by title) as Romance,
        listagg(iff(genre_primary = 'Fantasy', title, null), ', ') within group (order by title) as Fantasy,
        listagg(iff(genre_primary = 'Mystery', title, null), ', ') within group (order by title) as Mystery,
        listagg(iff(genre_primary = 'Thriller', title, null), ', ') within group (order by title) as Thriller,
        listagg(iff(genre_primary = 'Adventure', title, null), ', ') within group (order by title) as Adventure,
        listagg(iff(genre_primary = 'Crime', title, null), ', ') within group (order by title) as Crime,
        listagg(iff(genre_primary = 'Drama', title, null), ', ') within group (order by title) as Drama,
        listagg(iff(genre_primary = 'Sci-Fi', title, null), ', ') within group (order by title) as Scifi,
        listagg(iff(genre_primary = 'Documentary', title, null), ', ') within group (order by title) as Documentary,
        listagg(iff(genre_primary = 'Action', title, null), ', ') within group (order by title) as Action,
        listagg(iff(genre_primary = 'History', title, null), ', ') within group (order by title) as History,
        listagg(iff(genre_primary = 'War', title, null), ', ') within group (order by title) as War,
        listagg(iff(genre_primary = 'Animation', title, null), ', ') within group (order by title) as Animation,
        listagg(iff(genre_primary = 'Family', title, null), ', ') within group (order by title) as Family,
        listagg(iff(genre_primary = 'Horror', title, null), ', ') within group (order by title) as Horror,
        listagg(iff(genre_primary = 'Comedy', title, null), ', ') within group (order by title) as Comedy
    from top_rank_avg
    group by age_group
    order by age_group asc
)

select
    *
from final;
