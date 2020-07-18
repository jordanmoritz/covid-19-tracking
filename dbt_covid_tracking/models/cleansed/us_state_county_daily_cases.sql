{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'date',
                    'data_type': 'date'},
  )
}}

{%- set max_partition_date = run_query('select 50 as id') -%}
{{ log('testtttt:' ~ max_partition_date) }}

{%- set columns_to_unpivot = columns_to_list(
    'covid_sources',
    'usafacts_confirmed_cases',
    ignore=['state', 'state_fips_code', 'county_name', 'county_fips_code']) -%}

{% for column in columns_to_unpivot -%}
select
    state_fips_code,
    state as state_abbreviation,
    county_fips_code,
    county_name,
    parse_date('%m/%d/%y',
              '{{ column.split('_')[1] }}/{{ column.split('_')[2] }}/{{ column.split('_')[3] }}')
              as date,
    {{ column }} as cumulative_cases
from
    {{ source('covid_sources', 'usafacts_confirmed_cases') }}

{%- if not loop.last %}
union all
{% endif %}

{%- endfor %}
