{#
Returns list of columns from a table.

Args:
    dataset_name: BigQuery dataset name
    table_name: BigQuery table name
    ignore: List of columnns to ignore from list
#}

{% macro columns_to_list(dataset_name, table_name, ignore=none) %}

{# Uses input to construct relation object and retrieve columns #}
{% set relation = api.Relation.create(
      schema=dataset_name,
      identifier=table_name) %}

{% set table_column_list = adapter.get_columns_in_relation(relation) %}
{% set ignore_columns = [] if ignore is none else ignore %}
{% set column_list = [] %}

{# Loop through columns in table, check against ignore list #}
{% for column in table_column_list %}
    {% if column.name.lower() not in ignore_columns|map('lower') %}
        {% do column_list.append(column.name) %}
    {% endif %}
{% endfor %}

{# {{ log("column_list:" ~ column_list) }} #}

{{ return(column_list) }}

{% endmacro %}
