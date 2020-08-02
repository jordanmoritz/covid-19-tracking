
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

    {{ calculate_daily_new_metrics('county_daily') }}

    {{ calculate_per_capita_metrics('county_population') }}

    -- To identify most recent date's data
    -- Should prevent need to re-aggregate on front-end
    if(max(date) over (partition by state_abbreviation, county_name) = date
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

    {{ calculate_rolling_metrics('county_7_days') }}

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
    {{ calculate_pop_rolling_metrics('county_population') }}
from
    rolling
