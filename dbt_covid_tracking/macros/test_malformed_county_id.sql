{% macro test_malformed_county_id(model, column_name) %}

with malformed_county_id as (
    select
        {{ column_name }} as ids
    from
        {{ model }}
    where
        length({{ column_name }}) != 5
    group by
        {{ column_name }}
    having
        count(ids) > 1
)

select count(*) from malformed_county_id

{% endmacro %}
