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
    country_data.date,
    country_data.geo_id,
    country_data.country_territory_code,

    case
        when country_map.country is not null then country_map.country
        -- Plug for values lacking matched codes
        when country_data.country_territory_code in ('AIA', 'BES', 'GGY', 'JEY', 'MSF', 'CNG1925')
            then country_data.countries_and_territories
        when country_data.country_territory_code = 'FLK' then 'Falkland Islands'
        when country_data.country_territory_code = 'VAT' then 'Holy See'
        when country_data.country_territory_code = 'ESH' then 'Western Sahara'
        when country_data.countries_and_territories = 'Cases_on_an_international_conveyance_Japan'
            then 'Japan International Conveyance'
        else 'Undefined'
        end
        as country_territory_name,

    country_data.pop_data_2019 as country_territory_population,
    country_data.confirmed_cases as cumulative_cases,
    country_data.deaths as cumulative_deaths,
    country_data.daily_confirmed_cases as daily_new_cases,
    country_data.daily_deaths as daily_new_deaths
from
    {{ source('covid_sources', 'ecdc_daily_country_volume')}} as country_data

left join
    {{ source('mapping', 'country_codes') }} as country_map
on
    country_map.country_code = country_data.country_territory_code

{% if is_incremental() and max_partition_date is not none %}
where
    date >= '{{ max_partition_date }}'
{% endif %}
