{#
Returns the max date value from specified date column.

Args:
    project_name: GCP project identifier
    dataset_name: BigQuery dataset name
    table_name: BigQuery table name
    column: Column name
#}

{% macro max_date_value(project_name, dataset_name, table_name, column) %}

{# Uses input to construct relation object and retrieve columns #}
{%- set relation = adapter.get_relation(
      database=project_name,
      schema=dataset_name,
      identifier=table_name) -%}

{%- if relation -%}

    {%- call statement('max_date_value', fetch_result=true) -%}
        select max(date) as date_field from {{ relation }}
    {%- endcall -%}

    {%- set results = load_result('max_date_value') -%}

    {%- set max_date_value = results['data'] | map(attribute=0) | first() -%}

    {{ return(max_date_value) }}

{%- else -%}

    {{ return('not_avail')}}

{%- endif -%}

{% endmacro %}
