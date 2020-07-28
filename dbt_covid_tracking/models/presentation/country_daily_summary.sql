{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite'
  )
}}

select
    country.* except (geo_id),
    round((cumulative_cases / country_territory_population) * 100000, 2) AS cases_per_100k,
    round((cumulative_deaths / country_territory_population) * 100000, 2) AS deaths_per_100k
from
    {{ ref('ecdc_country_daily_volume') }} as country
