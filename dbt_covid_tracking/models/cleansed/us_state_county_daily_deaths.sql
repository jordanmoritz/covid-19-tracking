{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite'
  )
}}

{%- set max_partition_date = max_date_value(
    'big-query-horse-play',
    'dbt_covid_dev_cleansed',
    'us_state_county_daily_deaths',
    'date') -%}

{%- set columns_to_unpivot = columns_to_list(
    'big-query-horse-play',
    'covid_sources',
    'usafacts_deaths',
    ignore=['state', 'state_fips_code', 'county_name', 'county_fips_code']) -%}

{# In case where cleansed table is not currently built #}
{%- if max_partition_date is none -%}

    {# Then construct this model using entire column list from source table #}
    {% for column in columns_to_unpivot -%}
    select
        -- Some states have malformed codes for whatever reason
        if(length(state_fips_code) = 3
          , substr(state_fips_code, 2)
          , state_fips_code) as state_fips_code,
        state as state_abbreviation,
        county_fips_code,
        county_name,

        {%- set date_parts = column.split('_') %}
        parse_date('%m/%d/%y',
                  '{{ date_parts[1] }}/{{ date_parts[2] }}/{{ date_parts[3] }}')
                  as date,
        {{ column }} as cumulative_deaths
    from
        {{ source('covid_sources', 'usafacts_deaths') }}

    {%- if not loop.last %}
    union all
    {% endif %}

    {%- endfor %}

{# Else use the max date in cleansed table to determine which fields to unpivot
so we dont rebuild the entire table for no reason #}
{%- else -%}

    {%- set dates_to_unpivot = get_dates_to_unpivot(columns_to_unpivot, max_partition_date) -%}

    {% for date in dates_to_unpivot -%}
    select
        state_fips_code,
        state as state_abbreviation,
        county_fips_code,
        county_name,
        date '{{ date }}' as date,
        {# Have to reconstruct date column name from date value #}
        {{ format_date_column(date) }} as cumulative_deaths
    from
        {{ source('covid_sources', 'usafacts_deaths') }}

    {%- if not loop.last %}
    union all
    {% endif %}

    {%- endfor %}

{%- endif -%}
