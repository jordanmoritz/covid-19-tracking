{#
Returns the max date value from specified date column.

Args:
    dataset_name: BigQuery dataset name
    table_name: BigQuery table name
    column: Column name
#}

{% macro max_date_value(dataset_name, table_name, column) %}

{# Uses input to construct relation object and retrieve columns #}
{%- set relation = api.Relation.create(
      schema=dataset_name,
      identifier=table_name) -%}

{%- call statement('max_date_value', fetch_result=true) -%}
    select max(date) as date_field from {{ relation }}
{%- endcall -%}

{%- set results = load_result('max_date_value') -%}

{%- set max_date_value = results['data'] | map(attribute=0) | first() -%}

{{ return(max_date_value) }}

{% endmacro %}
