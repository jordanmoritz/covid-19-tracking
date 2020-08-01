{#
Returns SQL block for calculating new daily metrics. Same block used by multiple
presentation models, except different window name.

Args:
    window: Model specific named window
#}

{% macro calculate_daily_new_metrics(window) %}

    -- Navigation functions to determine daily new cases/deaths
    -- based on diff between today cumulative and yesterday
    cumulative_cases - lead(cumulative_cases, 1, 0) over ({{ window }}) as daily_new_cases,
    cumulative_deaths - lead(cumulative_deaths, 1, 0) over ({{ window }}) as daily_new_deaths,

{% endmacro %}
