{#
Same group of columns requried across multiple models for state_county_daily.
Naming and transformations are identical except metric.
#}

{%- macro county_daily_column_select() -%}

    -- Some states have malformed codes for whatever reason
    if(length(state_fips_code) = 3
      , substr(state_fips_code, 2)
      , state_fips_code) as state_fips_code,
    state as state_abbreviation,
    -- These are technically geo_id values which are
    -- concatenated state and county fips codes
    county_fips_code as county_geo_id,
    county_name,

{%- endmacro -%}
