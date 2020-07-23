{%- set max_partition_date = max_date_value(
    'big-query-horse-play',
    'dbt_covid_dev_cleansed',
    'us_state_county_daily_cases',
    'date') -%}

{%- set columns_to_unpivot = columns_to_list(
    'big-query-horse-play',
    'covid_sources',
    'usafacts_confirmed_cases',
    ignore=['state', 'state_fips_code', 'county_name', 'county_fips_code']) -%}

{%- set cols = get_dates_to_unpivot(columns_to_unpivot, max_partition_date) -%}

{% if cols is none %}
{{ log('its none') }}
{% else %}
{{ log('its not' ~ cols) }}
{% for date in cols %}
{{ log(date) }}
{% endfor %}
{% endif %}

{#
{{ log('cols test' ~ cols|length() )}}
#}

select 1 as num
