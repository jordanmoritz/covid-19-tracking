
-- CTE with max year for dynamic calculation below
with vehicle_fatalities as (
    select
        year,
        total_deaths,
        max(cast(year as int64)) over () as max_year
    from
        {{ source('united_states_comparisons', 'us_motor_vehicle_fatalities') }}
    group by
      year,
      total_deaths)

-- Motor vehicle fatalities
select
  'Motor Vehicle Death' as cause_of_death,
  cast(avg(total_deaths) as int64) as total_deaths
from
  vehicle_fatalities
where
  cast(year as int64) > (max_year - 7)

union all

-- CDC leading cause of death
select
    -- Mapping for cleaner front-end values
    case
        when cause_of_death like '%respiratory%' then 'Respiratory Disease'
        when cause_of_death like '%Accidents%' then 'Unintentional Accidents'
        when cause_of_death like '%Alzheimer%' then 'Alzheimer’s'
        when cause_of_death like '%Heart%' then 'Heart Disease'
        when cause_of_death like '%suicide%' then 'Suicide'
        when cause_of_death like '%nephrosis%' then 'Nephrosis Related'
        when cause_of_death like '%Stroke%' then 'Stroke'
        else cause_of_death
        end
        as cause_of_death,
    total_deaths
from
    {{ source('united_states_comparisons', 'cdc_us_leading_causes_death') }}
where
    -- Excluding because will have entire section dedicated to influenza
    cause_of_death != 'Influenza and pneumonia'

union all

-- Get value for COVID to use alongside
select
    'COVID-19' as cause_of_death,
    sum(cumulative_deaths) as total_deaths
from
    {{ ref('us_state_daily_summary') }}
where
    most_recent_date = 1
group by
    cause_of_death
