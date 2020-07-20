{%- set max_partition_date = max_date_value(
    'big-query-horse-play',
    'dbt_covid_dev_cleansed',
    'us_state_county_daily_cases',
    'date') -%}

{%- set columns_to_unpivot = columns_to_list(
    'big-query-horse-play',
    'covid_sources',
    'usafacts_confirmed_cases',
    ignore=['state', 'state_fips_code', 'county_name', 'county_fips_code']) -%}

{%- set max_source_date = columns_to_unpivot | last() -%}
{# {%- set max_source_date_formatted = } #}

{{ log('dates:' ~ columns_to_unpivot | last() ) }}




{%- set cols_to_unpivot = get_dates_to_unpivot(columns_to_unpivot, max_partition_date) -%}
{{ log('macro_results:' ~ cols_to_unpivot) }}


with source_dates as (
{% for column in columns_to_unpivot -%}
select
  parse_date('%m/%d/%y',
          '{{ column.split('_')[1] }}/{{ column.split('_')[2] }}/{{ column.split('_')[3] }}')
          as source_date
{%- if not loop.last %}
union all
{% endif %}

{%- endfor %}
)

-- this works, but now you need to retrieve these results
-- and use them to build the unpivot/union
-- and account for cases when the table is empty (hoping it returns none results)
select
  source_date
from
  source_dates
{% if max_partition_date is not none -%}
where
  -- Greater and equal incase previous day was re-stated
  source_date >= date '{{ max_partition_date }}'
{%- endif %}
