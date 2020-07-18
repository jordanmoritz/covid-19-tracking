{%- call statement('max_partition_date', fetch_result=true) -%}
    select '1' as id
{%- endcall -%}

{%- set results = load_result('max_partition_date') -%}
{{ log('results:' ~ results['data'] | list()) }}

select max(date) as max_date from {{ ref('us_state_county_daily_cases') }}
