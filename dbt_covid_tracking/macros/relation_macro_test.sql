{% macro relation_test() %}

{% set relation = api.Relation.create(
      schema='dbt_dev_jm',
      identifier='county_cases_date_columns') %}

{% set relation2 = source('dbt_dev_jm', 'county_cases_date_columns') %}

{{ log("schema:" ~ relation.schema)}}
{{ log("database:" ~ relation.database)}}

{% set column_list = adapter.get_columns_in_relation(relation) %}

{{ log("column_list:" ~ column_list)}}

{% for column in column_list %}
  {{ log("column:" ~ column.name) }}
{% endfor %}

{% endmacro %}
