
-- Aggregation of US data at state level granularity
with state as (
select
    date,
    state_name,
    state_population,
    sum(cumulative_cases) AS cumulative_cases,
    sum(cumulative_deaths) AS cumulative_deaths,
from
    {{ ref('us_state_county_daily_volume') }}
group by
    date,
    state_name,
    state_population
),

-- Using aggregated to calculate daily new_cases/deaths
new_calcs as (
select
    state.*,
    -- Navigation functions to determine daily new cases/deaths
    -- based on diff between today cumulative and yesterday
    cumulative_cases - lead(cumulative_cases, 1, 0) over (state_daily) as new_cases,
    cumulative_deaths - lead(cumulative_deaths, 1, 0) over (state_daily) as new_deaths,
    -- Per capita related calcs
    round((cumulative_cases / state_population) * 100000, 2) AS cases_per_100k,
    round((cumulative_deaths / state_population) * 100000, 2) AS deaths_per_100k,
    -- To identify most recent date's data
    -- Should prevent need to re-aggregate on front-end
    if(max(date) over (state_daily) = date
        , 1, 0) as most_recent_date
from
    state
window
    state_daily as (partition by state_name order by date desc)
)

select
    new_calcs.* except(most_recent_date),
    -- Using new cases/deaths to calculate rolling metrics
    sum(new_cases) over (state_7_days) as new_cases_last_7,
    round(avg(new_cases) over (state_7_days), 2) as avg_daily_new_cases_last_7,
    sum(new_deaths) over (state_7_days) as new_deaths_last_7,
    round(avg(new_deaths) over (state_7_days), 2) as avg_daily_new_deaths_last_7,
    -- Keeping consistent with other table schemas
    most_recent_date
from
    new_calcs
window
    state_7_days as (
        partition by
            state_name
        order by
            date desc
        rows between current row and 6 following
    )
