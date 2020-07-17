
select
    state,
    -- Navigation functions to determine daily new cases/usafacts_deaths
    -- based on diff between today cumulative and yesterday
    cases - lead(cases, 1, 0) over (county_daily) as new_cases,
    deaths - lead(deaths, 1, 0) over (county_daily) as new_deaths,
from
    {{ ref('us_state_county_daily_volume') }}
window
    county_daily as (partition by state_name, county_name order by date desc)
