{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite'
  )
}}

select
    country.* except (geo_id),
    -- Per capita related calcs
    round((cumulative_cases / country_territory_population) * 100000, 2) AS cases_per_100k,
    round((cumulative_deaths / country_territory_population) * 100000, 2) AS deaths_per_100k,
    -- Using new cases/deaths to calculate rolling metrics
    sum(new_cases) over (country_7_days) as new_cases_last_7,
    round(avg(new_cases) over (country_7_days), 2) as avg_daily_new_cases_last_7,
    sum(new_deaths) over (country_7_days) as new_deaths_last_7,
    round(avg(new_deaths) over (country_7_days), 2) as avg_daily_deaths_cases_last_7
from
    {{ ref('ecdc_country_daily_volume') }} as country
window
    country_7_days as (
        partition by
            country_territory_name
        order by
            date desc
        rows between current row and 6 following
    )
