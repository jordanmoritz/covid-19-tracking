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

{# {{ log("date_split:" ~ date_split) }} #}

{# Year #}
{%- set year_formatted = date_split[0] -%}

{# Q1 2021 format of source data changed making this unnecessary #}
{# %- set year_chars = year | list() -% #}
{# %- set year_formatted = year_chars[2] ~ year_chars[3] -% #}

{# Month #}
{%- set month_formatted = date_split[1] -%}

{# Q1 2021 format of source data changed making this unnecessary #}
{# %- set month_chars = month | list() -% #}
{# %- if month_chars[0] == '0' -% #}
    {# %- set month_formatted = month_chars[1] -% #}
{# %- else -% #}
    {# %- set month_formatted = month -% #}
{# %- endif -% #}

{# Day #}
{%- set day_formatted = date_split[2] -%}

{# Q1 2021 format of source data changed making this unnecessary #}
{# %- set day_chars = day | list() -% #}
{# %- if day_chars[0] == '0' -% #}
    {# %- set day_formatted = day_chars[1] -% #}
{# %- else -% #}
    {# %- set day_formatted = day -% #}
{# %- endif -% #}

{%- set formatted_date_column = '_' ~ year_formatted ~ '_' ~ month_formatted ~ '_' ~ day_formatted -%}

{# {{ log("formatted_date_column:" ~ formatted_date_column) }} #}

{{ return(formatted_date_column) }}

{% endmacro %}
