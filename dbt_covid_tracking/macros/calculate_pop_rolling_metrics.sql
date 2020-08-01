{#
Returns SQL block for calculating rolling population metrics. Same block used by
multiple presentation models, except different population field names.

Args:
    population_field: Model specific population field (country, state, county)
#}

{% macro calculate_pop_rolling_metrics(population_field) %}

    -- Calculate population adjusted rolling metrics
    round((new_cases_last_7 / {{ population_field }}) * 100000
          , 2) AS new_cases_last_7_per_100k,
    round((avg_daily_new_cases_last_7 / {{ population_field }}) * 100000
          , 2) AS avg_daily_new_cases_last_7_per_100k,
    round((new_deaths_last_7 / {{ population_field }}) * 100000
          , 2) AS new_deaths_last_7_per_100k,
    round((avg_daily_new_deaths_last_7 / {{ population_field }}) * 100000
          , 2) AS avg_daily_new_deaths_last_7_per_100k

{% endmacro %}
