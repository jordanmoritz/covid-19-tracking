{% macro columns_to_list() %}

{% set get_columns %}
select
  column_name
from
  `bigquery-public-data.covid19_usafacts`.INFORMATION_SCHEMA.COLUMNS
where
  table_name = 'confirmed_cases'
  and starts_with(column_name, '_')
{% endset %}

{% set results = run_query(get_columns) %}

{{ log("results:" ~ results) }}

{% set column_list = results %}

{{ log("column_list:" ~ column_list) }}

{{ return(column_list) }}

{% endmacro %}
