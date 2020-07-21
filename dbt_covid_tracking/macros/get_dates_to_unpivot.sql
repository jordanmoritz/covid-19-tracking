{#
Returns date columns to unpivot into single date field.
Unique to structure of usafacts cases/deaths dataset.

Args:
    column_list: All date field columns in source dataset
    max_partition_date: Current max date in cleansed table
#}

{% macro get_dates_to_unpivot(column_list, max_partition_date) %}

{# Uses column list to create unioned table of all date values
and compares that against the current max date #}
{%- call statement('columns_to_unpivot', fetch_result=true) -%}
    -- Start get_dates_to_unpivot sql
    with source_dates_formatted as (

    {% for column in column_list -%}
      select
          {%- set date_parts = column.split('_') -%}
          parse_date('%m/%d/%y',
                  '{{ date_parts[1] }}/{{ date_parts[2] }}/{{ date_parts[3] }}')
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
        source_date >= '{{ max_partition_date }}'
    {%- endif %}

{%- endcall -%}

{%- set results = load_result('columns_to_unpivot') -%}

{%- set dates_to_unpivot = results['data'] | map(attribute=0) | list() -%}

{# In case where there are no dates to unpivot #}
{%- if dates_to_unpivot | length == 0 -%}
    {%- set dates_to_unpivot = none -%}
{%- endif -%}

{{ return(dates_to_unpivot) }}

{% endmacro %}
