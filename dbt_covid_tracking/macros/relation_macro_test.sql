{% macro relation_test() %}

{% set relation = api.Relation.create(schema='my_first_dbt_model') %}

{% set relation2 = source('dbt_dev_jm', 'county_cases_date_columns') %}

{{ log("schema:" ~ relation2.schema)}}
{{ log("database:" ~ relation2.database)}}

{% endmacro %}
