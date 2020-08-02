
{#
Would use {{ this }} rather than manually defining max date but finding issue
in BigQuery where processing associated with dbt generated sub-query is
expontentially more expensive than providing the string value as var
#}
{%- set max_partition_date = max_date_value(
    'big-query-horse-play',
    'dbt_covid_dev_joined',
    'country_daily_volume',
    'date') -%}

select
    country.*
from
    {{ ref('ecdc_country_daily_volume') }} as country
where
    -- Excluded because replacing with more frequently updated
    -- dataset based on common US timezones
    country_territory_code != 'USA'

union all

-- Tacking on aggregate US data from state level dataset
select
    date,
    'US' as geo_id,
    'USA' as country_territory_code,
    'United States' as country_territory_name,
    -- Using state census pop to get country level pop
    (select sum(total_pop) from {{ source('mapping', 'us_state_pop_2018_5yr_census') }})
        as country_territory_population,
    sum(cumulative_cases) as cumulative_cases,
    sum(cumulative_deaths) as cumulative_deaths,
    sum(daily_new_cases) as daily_new_cases,
    sum(daily_new_deaths) as daily_new_deaths,
from
    {{ ref('us_state_daily_summary') }}
group by
    date,
    geo_id,
    country_territory_code,
    country_territory_name,
    country_territory_population
