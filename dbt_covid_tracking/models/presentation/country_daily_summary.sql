{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite'
  )
}}

-- Calculating per capita and rolling metrics
with rolling as (
select
    country.* except (geo_id),
    -- Per capita related calcs
    round((cumulative_cases / country_territory_population) * 100000, 2) AS cases_per_100k,
    round((cumulative_deaths / country_territory_population) * 100000, 2) AS deaths_per_100k,
    -- Using new cases/deaths to calculate rolling metrics
    sum(daily_new_cases) over (country_7_days) as new_cases_last_7,
    round(avg(daily_new_cases) over (country_7_days), 2) as avg_daily_new_cases_last_7,
    sum(daily_new_deaths) over (country_7_days) as new_deaths_last_7,
    round(avg(daily_new_deaths) over (country_7_days), 2) as avg_daily_new_deaths_last_7,
    -- To identify most recent date's data
    -- Should prevent need to re-aggregate on front-end
    if(max(date) over (partition by country_territory_name) = date
        , 1, 0) as most_recent_date
from
    {{ ref('country_daily_volume') }} as country
window
    country_7_days as (
        partition by
            country_territory_name
        order by
            date desc
        rows between current row and 6 following
    )
)

-- Using rolling CTE to calculate population adjusted rolling metrics
select
    rolling.*,
    round((new_cases_last_7 / country_territory_population) * 100000
          , 2) AS new_cases_last_7_per_100k,
    round((avg_daily_new_cases_last_7 / country_territory_population) * 100000
          , 2) AS avg_daily_new_cases_last_7_per_100k,
    round((new_deaths_last_7 / country_territory_population) * 100000
          , 2) AS new_deaths_last_7_per_100k,
    round((avg_daily_new_deaths_last_7 / country_territory_population) * 100000
          , 2) AS avg_daily_new_deaths_last_7_per_100k,
from
    rolling
