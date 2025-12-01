with group_table as (
        select 
               passengerid,
               survived,
               sex,
               case
                   when age <16 then 'children'
                   when age >=60 then 'senior'
                   when age is null then 'unknown'
                   else 'adult' 
               end as agegroup,
               sibsp+parch as familysize,
               case 
                   when familysize = 0 then 'solo'
                   else 'family'
               end as traveltype
        from titanic_dataset
),

survival_rate as (
        select
               sex,
               agegroup,
               traveltype,
               count(passengerid) as total_passanger,
               round(avg(survived),2) as avg_suvrate
        from group_table
        group by sex,agegroup,traveltype
),

survival_category as (
        select
                sex,
                agegroup,
                traveltype,
                total_passanger,
                avg_suvrate,
                case
                    when avg_suvrate >= 0.70 then 'high survival'
                    when avg_suvrate <0.40 then 'low survival'
                    else 'medium survival'
                end as surv_category
        from survival_rate
        order by avg_suvrate desc
)

select *
from survival_category;

/*
Questions to discuss in study groups/present: 
Which groups had the highest survival rates? -> senior female and children female either solo or family
Which groups had the lowest survival rates? -> male senior, adult and children who traveled solo
Do the results support the historical idea of “women and children first”? -> yes
*/

WITH age_rng AS (
  SELECT
    age,
    name,
    sex,
    CASE
      WHEN age <= 16 THEN 'child'
      WHEN age BETWEEN 17 AND 59 THEN 'adult'
      WHEN age >= 60 THEN 'senior'
      ELSE 'unknown'
    END AS age_group
  FROM titanic_dataset
),
family_size AS (
  SELECT
    name,
    SUM(SIBSP + PARCH) AS fam_size,
    CASE
      WHEN SUM(SIBSP + PARCH) > 0 THEN 'family' ELSE 'solo'
    END AS travel_type
  FROM titanic_dataset
  GROUP BY name
),
survival_stats AS (
  SELECT
    a.sex,
    a.age_group,
    f.travel_type,
    COUNT(*) AS total_passengers,
    SUM(CASE WHEN s.survived = 1 THEN 1 ELSE 0 END) AS total_survivors
  FROM titanic_dataset s
  LEFT JOIN age_rng a ON s.name = a.name
  LEFT JOIN family_size f ON s.name = f.name
  GROUP BY a.sex, a.age_group, f.travel_type
)
SELECT
  sex,
  age_group,
  travel_type,
  total_survivors,
  total_passengers,
  ROUND(total_survivors * 100.0 / total_passengers, 2) AS survival_rate
FROM survival_stats
ORDER BY age_group, sex, travel_type;