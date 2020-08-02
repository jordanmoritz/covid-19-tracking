
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
    {{ calculate_daily_new_metrics('state_daily') }}

    {{ calculate_per_capita_metrics('state_population') }}

    -- To identify most recent date's data
    -- Should prevent need to re-aggregate on front-end
    if(max(date) over (partition by state_name) = date
        , 1, 0) as most_recent_date

from
    state
window
    state_daily as (partition by state_name order by date desc)
),

-- Calculating rolling metrics
rolling as (
select
    new_calcs.* except(most_recent_date),

    {{ calculate_rolling_metrics('state_7_days') }}

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
)

-- Using rolling CTE to calculate population adjusted rolling metrics
select
    rolling.*,
    {{ calculate_pop_rolling_metrics('state_population') }}
from
    rolling
