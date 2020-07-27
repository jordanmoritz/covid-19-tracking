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
    'dbt_covid_dev_cleansed',
    'ecdc_daily_country_volume',
    'date') -%}

select
    date,
    geo_id,
    country_territory_code,
    countries_and_territories,
    pop_data_2019 as country_population,
    daily_confirmed_cases as new_cases,
    daily_deaths as new_deaths,
    confirmed_cases as cumulative_cases,
    deaths as cumulative_deaths,
from
    {{ source('covid_sources', 'ecdc_daily_country_volume')}}

{% if is_incremental() and max_partition_date is not none %}
where
    date >= '{{ max_partition_date }}'
{% endif %}
