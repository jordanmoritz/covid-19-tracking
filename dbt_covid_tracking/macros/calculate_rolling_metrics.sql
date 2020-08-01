{#
Returns SQL block for calculating rolling metrics. Same block used by multiple
presentation models, except different window name.

Args:
    window: Model specific named window
#}

{% macro calculate_rolling_metrics(window) %}

    -- Using new cases/deaths to calculate rolling metrics
    sum(daily_new_cases) over ({{ window }}) as new_cases_last_7,
    round(avg(daily_new_cases) over ({{ window }}), 2) as avg_daily_new_cases_last_7,
    sum(daily_new_deaths) over ({{ window }}) as new_deaths_last_7,
    round(avg(daily_new_deaths) over ({{ window }}), 2) as avg_daily_new_deaths_last_7,

{% endmacro %}
