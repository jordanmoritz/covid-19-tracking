{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite'
  )
}}

select
    cases.state_fips_code,
    cases.state_abbreviation,
    state_fips.state_name,
    cases.county_fips_code,
    cases.county_name,
    cases.date,
    cases.cumulative_cases,
    deaths.cumulative_deaths
from
    {{ ref('us_state_county_daily_cases') }} as cases
inner join
    {{ ref('us_state_county_daily_deaths') }} as deaths
on
    cases.state_fips_code = deaths.state_fips_code
    and cases.county_fips_code = deaths.county_fips_code
    and cases.date = deaths.date
left join
    {{ source('mapping', 'us_states_fips_codes') }} as state_fips
on
    state_fips.state_fips_code = cases.state_fips_code
