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
