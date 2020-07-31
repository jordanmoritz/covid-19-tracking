-- SELECT
--   date,
--   country_territory_population,
--   country_territory_name,
--   cumulative_cases,
--   cumulative_deaths,
--   daily_new_cases,
--   daily_new_deaths
-- FROM
--   `big-query-horse-play.dbt_covid_dev_presentation.country_daily_summary`
-- WHERE
--   date > '2020-07-20'
--   AND country_territory_name = 'United States'
-- ORDER BY
--   date DESC



SELECT
  state,
  SUM(_7_24_20) _7_24_20,
  SUM(_7_25_20) AS _7_25_20,
  SUM(_7_26_20) _7_26_20,
  SUM(_7_27_20) _7_27_20,
  SUM(_7_28_20) _7_28_20,
  SUM(_7_29_20) _7_29_20
FROM
  `big-query-horse-play.covid_sources.usafacts_deaths`
  -- `big-query-horse-play.dbt_covid_dev_cleansed.ecdc_country_daily_volume`
WHERE
  state = 'OR'
GROUP BY
  1


  SELECT
    date,
    confirmed_cases,
    deaths
  FROM
    `bigquery-public-data.covid19_nyt.us_states`
  WHERE
    state_name = 'Oregon'
  ORDER BY
    date DESC

SELECT
  date,
  confirmed_cases,
  deaths
FROM
  `bigquery-public-data.covid19_nyt.us_states`
WHERE
  state_name = 'Oregon'
ORDER BY
  date DESC

SELECT
  date,
  SUM(confirmed_cases),
  SUM(deaths)
FROM
  `bigquery-public-data.covid19_nyt.us_counties`
WHERE
  state_name = 'Oregon'
GROUP BY
  1
ORDER BY
  date DESC

SELECT
  date,
  state_name,
  SUM(cumulative_cases),
  SUM(daily_new_cases),
  SUM(cumulative_deaths),
  SUM(daily_new_deaths)
FROM
  `big-query-horse-play.dbt_covid_dev_presentation.us_state_daily_summary`
WHERE
  date > '2020-07-20'
  AND state_name = 'Oregon'
GROUP BY
  1,
  2
ORDER BY
  date DESC

  SELECT
    date,
    cumulative_cases,
    cumulative_deaths
  FROM
    `big-query-horse-play.dbt_covid_dev_cleansed.ecdc_country_daily_volume`
  WHERE
    country_territory_name = 'United States'
  ORDER BY
    date DESC
