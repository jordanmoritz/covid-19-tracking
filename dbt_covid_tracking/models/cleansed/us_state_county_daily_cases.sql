{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite'
  )
}}

{#
This model references source data that has unique column values for each day
formatted as '_m_dd_yy'. Every day the source table has a new column added for
the previous day.

To melt this down into a single date field it uses a macro to grab all the
column values. Those are used to union selects together for each value,
while formatting into a date value, and using the original text value for
column selection.

Ideally, this would entirely leverage the native dbt incremental strategy, but
given the structure of this source the incremental behavior seems to be
recreating the entire table from scratch, and using that to determine which
additional dates partitions to append to the existing table from the model
({{ this }}). This results in unnecessary compute/processing.

To work around this, it uses a macro to determine which dates (and by virtue of
the source structure, which columns) should be processed based on the current
max date in the destination table, and the dates present in the source. Using
this, the model conditionally builds the table only for those dates which are
not already present in the destination table, which significantly reduces
compute/processing.

From here, the dbt incremental strategy proceeds as expected, only appending
new data to the destination table. However, this wrestles with some of the
native features of the framework, specifically the --full_refresh
flag on model run. On full refresh, dbt switches from its merge strategy to
create and replace of existing table, but because the model is intervening and
conditionally building the table only for new data that should be appended to
the destination table, this effectively truncates the destination table and
recreates it only with the new data that was intended to be appended.
#}

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

{# In case where cleansed table is not currently built #}
{%- if max_partition_date is none -%}

    {# Then construct this model using entire column list from source table #}
    {% for column in columns_to_unpivot -%}
    select
        state_fips_code,
        state as state_abbreviation,
        county_fips_code,
        county_name,

        {%- set date_parts = column.split('_') %}
        parse_date('%m/%d/%y',
                  '{{ date_parts[1] }}/{{ date_parts[2] }}/{{ date_parts[3] }}')
                  as date,
        {{ column }} as cumulative_cases
    from
        {{ source('covid_sources', 'usafacts_confirmed_cases') }}

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
        {{ format_date_column(date) }} as cumulative_cases
    from
        {{ source('covid_sources', 'usafacts_confirmed_cases') }}

    {%- if not loop.last %}
    union all
    {% endif %}

    {%- endfor %}

{%- endif -%}
