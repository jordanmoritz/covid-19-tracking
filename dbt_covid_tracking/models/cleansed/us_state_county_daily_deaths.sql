{%- set columns_to_unpivot = columns_to_list(
    'covid_sources',
    'usafacts_deaths',
    ignore=['state', 'state_fips_code', 'county_name', 'county_fips_code']) -%}

{% for column in columns_to_unpivot -%}
select
    -- Some states have malformed codes for whatever reason
    if(length(state_fips_code) = 3
      , substr(state_fips_code, 2)
      , state_fips_code) as state_fips_code,
    state as state_name,
    county_fips_code,
    county_name,
    parse_date('%m/%d/%y',
              '{{ column.split('_')[1] }}/{{ column.split('_')[2] }}/{{ column.split('_')[3] }}')
              as date,
    {{ column }} as deaths
from
    {{ source('covid_sources', 'usafacts_deaths') }}

{%- if not loop.last %}
union all
{% endif %}

{%- endfor %}
