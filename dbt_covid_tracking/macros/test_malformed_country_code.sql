{% macro test_malformed_country_code(model, column_name) %}

with malformed_country_code as (
    select
        {{ column_name }} as codes
    from
        {{ model }}
    where
        {{ column_name }} != 'CNG1925'
        and length({{ column_name }}) != 3
    group by
        {{ column_name }}
    having
        count(codes) > 1
)

select count(*) from malformed_country_code

{% endmacro %}
