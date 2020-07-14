select * from {{ source('dbt_dev_jm', 'county_cases_date_columns') }}

{% set columns = [{{ columns_to_list() }}] %}

select
  columns

{{ columns_to_list() }}


-- this was another approch using the adapter macro
{% set dest_columns = adapter.get_columns_in_table('county_cases_date_columns', identifier) %}

select
  dest_columns



-- Can also consider the return macro?
{% macro get_data() %}

  {{ return([1,2,3]) }}

{% endmacro %}


select
  -- getdata() returns a list
  {% for i in getdata() %}
    {{ i }}
    {% if not loop.last %},{% endif %}
  {% endfor %}


-- Trying to use namespace to park the column list
{% macro columns_to_list() %}

{% set column_list = namespace(column_names=[]) %}

{% set column_list.column_names = [] %}
    (
      select
        column_name
      from
        `bigquery-public-data.covid19_usafacts`.INFORMATION_SCHEMA.COLUMNS
      where
        table_name = 'confirmed_cases'
        and starts_with(column_name, '_')
    )
{% endmacro %}

-- Using statement istead of run_query
{% call statement(name='get_columns', fetch_result=True, auto_begin=False) %}

    select
      column_name
    from
      `bigquery-public-data.covid19_usafacts`.INFORMATION_SCHEMA.COLUMNS
    where
      table_name = 'confirmed_cases'
      and starts_with(column_name, '_')
{% endcall %}

{% set results = load_result('get_columns') %}
{% set column_list = results['data'] %}
