{% set columns_to_unpivot %}
    {{ columns_to_list(
          'dbt_dev_jm',
          'county_cases_date_columns',
          ignore=['state', 'county_name']) }}
{% endset %}

select 1 as field
