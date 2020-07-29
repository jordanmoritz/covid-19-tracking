
-- Calculating new cases/deaths metrics
with new_calcs as (
select
    date,
    state_name,
    state_population,
    county_geo_id, -- including for distinct counts if needed
    county_name,
    county_population,
    cumulative_cases,
    cumulative_deaths,
    -- Navigation functions to determine daily new cases/deaths
    -- based on diff between today cumulative and yesterday
    cumulative_cases - lead(cumulative_cases, 1, 0) over (county_daily) as daily_new_cases,
    cumulative_deaths - lead(cumulative_deaths, 1, 0) over (county_daily) as daily_new_deaths,
    -- Per capita related calcs
    round((cumulative_cases / county_population) * 100000, 2) AS cases_per_100k,
    round((cumulative_deaths / county_population) * 100000, 2) AS deaths_per_100k,
    -- To identify most recent date's data
    -- Should prevent need to re-aggregate on front-end
    if(max(date) over (county_daily) = date
        , 1, 0) as most_recent_date
from
    {{ ref('us_state_county_daily_volume') }}
window
    county_daily as (partition by state_abbreviation, county_name order by date desc)
),

-- Calculating rolling metrics
rolling as (
select
    new_calcs.* except(most_recent_date),
    -- Using new cases/deaths to calculate rolling metrics
    sum(daily_new_cases) over (county_7_days) as new_cases_last_7,
    round(avg(daily_new_cases) over (county_7_days), 2) as avg_daily_new_cases_last_7,
    sum(daily_new_deaths) over (county_7_days) as new_deaths_last_7,
    round(avg(daily_new_deaths) over (county_7_days), 2) as avg_daily_new_deaths_last_7,
    -- Keeping consistent with other table schemas
    most_recent_date
from
    new_calcs
window
    county_7_days as (
        partition by
            state_name,
            county_name
        order by
            date desc
        rows between current row and 6 following
    )
)

-- Using rolling CTE to calculate population adjusted rolling metrics
select
    rolling.*,
    round((new_cases_last_7 / county_population) * 100000
          , 2) AS new_cases_last_7_per_100k,
    round((avg_daily_new_cases_last_7 / county_population) * 100000
          , 2) AS avg_daily_new_cases_last_7_per_100k,
    round((new_deaths_last_7 / county_population) * 100000
          , 2) AS new_deaths_last_7_per_100k,
    round((avg_daily_new_deaths_last_7 / county_population) * 100000
          , 2) AS avg_daily_new_deaths_last_7_per_100k,
from
    rolling
