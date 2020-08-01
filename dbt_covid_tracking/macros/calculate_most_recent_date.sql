{#
Returns SQL block for calculating most recent date in model. Same block used by
multiple presentation models, except different window name.

Args:
    window: Model specific named window
#}

{% macro calculate_most_recent_date(window) %}

    -- To identify most recent date's data
    -- Should prevent need to re-aggregate on front-end
    if(max(date) over ({{ window }}) = date
        , 1, 0) as most_recent_date

{% endmacro %}
