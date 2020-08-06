
-- Grabs max date across the three primary models for case volume
-- to ensure no daily data is missing in the joined tables e.g.
-- due to one-sided refreshes of deaths w/ no cases, or malformed id/code etc.
with model_max_dates as (
    select
        (
        select
            distinct max(date)
        from
            {{ ref('us_state_county_daily_volume') }}
        ) as joined,

        (
        select
            distinct max(date)
        from
            {{ ref('usafacts_us_state_county_daily_cases') }}
        ) as cases,

        (
        select
            distinct max(date)
        from
            {{ ref('usafacts_us_state_county_daily_deaths') }}
        ) as deaths
)

select
    joined,
    cases,
    deaths
from
    model_max_dates
where
    joined < cases
    or
    joined < deaths
