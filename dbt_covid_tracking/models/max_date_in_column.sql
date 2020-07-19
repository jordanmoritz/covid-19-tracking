{%- set max_partition_date = max_date_value(
    'dbt_covid_dev_cleansed',
    'us_state_county_daily_cases',
    'date') -%}

select '{{ max_partition_date }}' as test
