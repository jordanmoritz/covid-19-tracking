{% set dest_columns = adapter.get_columns_in_table('county_cases_date_columns', identifier) %}

select
  dest_columns
