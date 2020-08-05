{% macro test_unique_county_id(model, column_name) %}

with non_unique_ids as (
    select
        {{ column_name }} as ids
    from
        {{ model }}
    where
        {{ column_name }} != '00000'
    group by
        {{ column_name }}
    having
        count(ids) > 1
)

select count(*) from non_unique_ids

{% endmacro %}
