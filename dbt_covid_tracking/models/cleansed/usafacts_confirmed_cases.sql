{%- set columns_to_unpivot = columns_to_list(
    'dbt_dev_jm',
    'county_cases_date_columns',
    ignore=['state', 'county_name']) -%}

{% for column in columns_to_unpivot -%}
select
    state,
    county_name,
    parse_date('%m/%d/%y',
              '{{ column.split('_')[1] }}/{{ column.split('_')[2] }}/{{ column.split('_')[3] }}')
              as date,
    {{ column }} as cases
from
    {{ source('dbt_dev_jm', 'county_cases_date_columns') }}

{%- if not loop.last %}
union all
{% endif %}

{%- endfor %}
