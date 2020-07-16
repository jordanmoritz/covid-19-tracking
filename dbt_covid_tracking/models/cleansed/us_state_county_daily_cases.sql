{%- set columns_to_unpivot = columns_to_list(
    'covid_sources',
    'usafacts_confirmed_cases',
    ignore=['state', 'state_fips_code', 'county_name', 'county_fips_code']) -%}

{% for column in columns_to_unpivot -%}
select
    state_fips_code,
    state,
    county_fips_code,
    county_name,
    parse_date('%m/%d/%y',
              '{{ column.split('_')[1] }}/{{ column.split('_')[2] }}/{{ column.split('_')[3] }}')
              as date,
    {{ column }} as cases
from
    {{ source('covid_sources', 'usafacts_confirmed_cases') }}

{%- if not loop.last %}
union all
{% endif %}

{%- endfor %}
