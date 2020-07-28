SELECT
  date,
  state_name,
  SUM(county_population) AS state_population,
  SUM(cumulative_cases) AS cumulative_cases,
  SUM(cumulative_deaths) AS cumulative_deaths,
  SUM(new_cases) AS new_cases,
  SUM(new_deaths) AS new_deaths
FROM
  `big-query-horse-play.dbt_covid_dev_presentation.us_state_county_daily_summary`
WHERE
  state_name = 'Oregon'
GROUP BY
  date,
  state_name
ORDER BY
  date DESC
