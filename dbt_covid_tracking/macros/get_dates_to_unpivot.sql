{#
Returns date columns to unpivot into single date field.
Unique to structure of usafacts cases/deaths dataset.

Args:
    column_list: All date field columns in source dataset
    max_partition_date: Current max date in cleansed table
#}

{% macro get_dates_to_unpivot(column_list, max_partition_date) %}

{%- call statement('columns_to_unpivot', fetch_result=true) -%}

with source_dates_formatted as (

{% for column in columns_to_unpivot -%}
  select
      parse_date('%m/%d/%y',
              '{{ column.split('_')[1] }}/{{ column.split('_')[2] }}/{{ column.split('_')[3] }}')
              as source_date

  {%- if not loop.last %}
  union all
  {% endif %}

{%- endfor %}
)

select
    source_date
from
    source_dates_formatted

{% if max_partition_date is not none -%}
where
  -- Greater and equal incase previous day was re-stated
    source_date >= date '{{ max_partition_date }}'
{%- endif %}

{%- endcall -%}

{%- set results = load_result('columns_to_unpivot') -%}

{{ return(results) }}

-- handle for no results in the table (maybe this is done elsewhere?)

{% endmacro %}
