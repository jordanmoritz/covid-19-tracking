{% macro test_malformed_state_code(model, column_name) %}

with malformed_state_codes as (
    select
        {{ column_name }} as codes
    from
        {{ model }}
    where
        length({{ column_name }}) > 2
    group by
        {{ column_name }}
    having
        count(codes) > 1
)

select count(*) from malformed_state_codes

{% endmacro %}
