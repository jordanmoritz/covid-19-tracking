-- SELECT
--   *
-- FROM
--   `big-query-horse-play.dbt_covid_dev_joined.us_state_county_daily_volume`
-- WHERE
--   state_name = 'California'
--   AND date = '2020-07-26'
-- ORDER BY
--   date DESC

-- SELECT
--   *
-- FROM
--   --   `big-query-horse-play.dbt_covid_dev_cleansed.usafacts_us_state_county_daily_cases`
--   `big-query-horse-play.dbt_covid_dev_cleansed.usafacts_us_state_county_daily_deaths`
-- WHERE
--   date = "2020-07-26"
--   AND state_abbreviation = 'CA'

SELECT
  DISTINCT LENGTH(state_fips_code) AS code_len, --have instances of len 7...
  COUNT(DISTINCT state_fips_code)
FROM
--   `big-query-horse-play.dbt_covid_dev_cleansed.usafacts_us_state_county_daily_deaths`
  `big-query-horse-play.dbt_covid_dev_cleansed.usafacts_us_state_county_daily_cases`
GROUP BY
  1

SELECT
  DISTINCT LENGTH(state_fips_code) AS length,
  state_fips_code
FROM
  `big-query-horse-play.covid_sources.usafacts_deaths`
WHERE
  LENGTH(state_fips_code) != 2
ORDER BY
  length DESC
