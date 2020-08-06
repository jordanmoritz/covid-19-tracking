{% macro test_expected_min_date(model, column_name) %}

with min_date as (
    select
        min({{ column_name }}) as date
    from
        {{ model }}
)

select count(*) from min_date where date != '2020-01-22'

{% endmacro %}
