select
    cases.state_fips_code,
    cases.state_name,
    cases.county_fips_code,
    cases.county_name,
    cases.date,
    cases.cases,
    deaths.deaths
from
    {{ ref('us_state_county_daily_cases') }} as cases
left join
    {{ ref('us_state_county_daily_deaths') }} as deaths
on
    cases.state_fips_code = deaths.state_fips_code
    and cases.county_fips_code = deaths.county_fips_code
    and cases.date = deaths.date
