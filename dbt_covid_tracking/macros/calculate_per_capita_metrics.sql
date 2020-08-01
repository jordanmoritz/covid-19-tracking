{#
Returns SQL block for calculating per capita metrics. Same block used by multiple
presentation models, except different population field name.

Args:
    population_field: Model specific population field (country, state, county)
#}

{% macro calculate_per_capita_metrics(population_field) %}

    -- Per capita related calcs
    round((cumulative_cases / {{ population_field }}) * 100000
        , 2) AS cases_per_100k,
    round((cumulative_deaths / {{ population_field }}) * 100000
        , 2) AS deaths_per_100k,

{% endmacro %}
