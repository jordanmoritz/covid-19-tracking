{% macro columns_to_list(dataset_name, table_name) %}

{% set relation = api.Relation.create(
      schema=dataset_name,
      identifier=table_name) %}

{% set column_list = adapter.get_columns_in_relation(relation) %}

{% for column in column_list %}
  {{ log("column_updated:" ~ column.name) }}
{% endfor %}

{% endmacro %}
