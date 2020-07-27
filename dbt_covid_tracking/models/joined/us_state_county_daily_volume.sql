{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite'
  )
}}

{#
Would use {{ this }} rather than manually defining max date but finding issue
in BigQuery where processing associated with dbt generated sub-query is
expontentially more expensive than providing the string value as var
#}
{%- set max_partition_date = max_date_value(
    'big-query-horse-play',
    'dbt_covid_dev_joined',
    'us_state_county_daily_volume',
    'date') -%}

select
    cases.date,
    cases.state_fips_code,
    cases.state_abbreviation,
    state.state_name,
    cases.county_geo_id,
    county.county_name,
    county_pop.total_pop AS county_population,
    cases.cumulative_cases,
    deaths.cumulative_deaths
from
    {{ ref('usafacts_us_state_county_daily_cases') }} as cases

inner join
    {{ ref('usafacts_us_state_county_daily_deaths') }} as deaths
on
    cases.state_fips_code = deaths.state_fips_code
    and cases.county_geo_id = deaths.county_geo_id
    and cases.date = deaths.date

left join
    {{ source('mapping', 'us_states_fips_codes') }} as state
on
    state.state_fips_code = cases.state_fips_code

left join
    {{ source('mapping', 'us_county_fips_codes') }} as county
on
    county.geo_id = cases.county_geo_id

left join
    {{ source('mapping', 'us_county_pop_2018_5yr_census') }} as county_pop
on
    county_pop.geo_id = cases.county_geo_id

{% if is_incremental() and max_partition_date is not none %}
where
    cases.date >= '{{ max_partition_date }}'
{% endif %}
