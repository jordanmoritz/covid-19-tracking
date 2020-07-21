{#
Returns formatted date column string value.
Unique to structure of usafacts cases/deaths dataset which has columns
for date values values structured as: _m_dd_yy

Args:
    date_value: Date value (not string)
#}


{% macro format_date_column(date_value) %}

{%- set date_string = date_value | string -%}
{%- set date_split = date_string.split('-') -%}

{# Year #}
{%- set year = date_split[0] -%}
{%- set year_chars = year | list() -%}
{%- set year_formatted = year_chars[2] ~ year_chars[3] -%}

{# Month #}
{%- set month = date_split[1] -%}
{%- set month_chars = month | list() -%}
{%- if month_chars[0] == '0' -%}
    {%- set month_formatted = month_chars[1] -%}
{%- else -%}
    {%- set month_formatted = month -%}
{%- endif -%}

{# Day #}
{%- set day_formatted = date_split[2] -%}

{%- set formatted_date_column = '_' ~ month_formatted ~ '_' ~ day_formatted ~ '_' ~ year_formatted -%}

{{ return(formatted_date_column) }}

{% endmacro %}
