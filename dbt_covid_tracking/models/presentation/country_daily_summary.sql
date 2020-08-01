
-- Calculating per capita and rolling metrics
with rolling as (
select
    country.* except (geo_id),

    {{ calculate_per_capita_metrics('country_territory_population') }}

    {{ calculate_rolling_metrics('country_7_days') }}

    -- To identify most recent date's data
    -- Should prevent need to re-aggregate on front-end
    if(max(date) over (partition by country_territory_name) = date
        , 1, 0) as most_recent_date

from
    {{ ref('ecdc_country_daily_volume') }} as country
window
    country_7_days as (
        partition by
            country_territory_name
        order by
            date desc
        rows between current row and 6 following
    )
)

-- Using rolling CTE to calculate population adjusted rolling metrics
select
    rolling.*,
    {{ calculate_pop_rolling_metrics('country_territory_population') }}
from
    rolling
