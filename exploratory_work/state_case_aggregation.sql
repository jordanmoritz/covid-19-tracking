SELECT
  DISTINCT
  date,
  state_name,
  SUM(DISTINCT county_population) OVER (state) AS state_pop,
  SUM(cumulative_cases) OVER (PARTITION BY state_name, date) AS total_cases
FROM
  `big-query-horse-play.dbt_covid_dev_joined.us_state_county_daily_volume`
WHERE
  state_name = 'Oregon'
WINDOW
  state AS (PARTITION BY state_name)
ORDER BY
  date DESC
