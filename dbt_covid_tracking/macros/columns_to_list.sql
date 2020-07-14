{% macro columns_to_list() %}

{% call statement(name='get_columns', fetch_result=True, auto_begin=False) %}

    select
      column_name
    from
      `bigquery-public-data.covid19_usafacts`.INFORMATION_SCHEMA.COLUMNS
    where
      table_name = 'confirmed_cases'
      and starts_with(column_name, '_')

{% endcall %}

{% set columns = load_result('get_columns') %}
{% set column_list = columns['data'] %}

{{ log(column_list) }}

{% endmacro %}
