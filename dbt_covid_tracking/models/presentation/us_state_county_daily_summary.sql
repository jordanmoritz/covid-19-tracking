
select
    date,
    state_name,
    county_geo_id, -- including for distinct counts if needed
    county_name,
    cumulative_cases,
    cumulative_deaths,
    -- Navigation functions to determine daily new cases/usafacts_deaths
    -- based on diff between today cumulative and yesterday
    cumulative_cases - lead(cumulative_cases, 1, 0) over (county_daily) as new_cases,
    cumulative_deaths - lead(cumulative_deaths, 1, 0) over (county_daily) as new_deaths,
from
    {{ ref('us_state_county_daily_volume') }}
window
    county_daily as (partition by state_abbreviation, county_name order by date desc)
