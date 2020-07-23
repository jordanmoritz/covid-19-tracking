-- SELECT
--   state_name,
--   county_name,
--   date,
--   deaths
-- FROM
--   `big-query-horse-play.dbt_covid_dev_cleansed.us_state_county_daily_deaths`
-- WHERE
--   -- deaths IS NULL
--   deaths = 0

-- SELECT
--   state_name,
--   county_name,
--   date,
--   cases
-- FROM
--   `big-query-horse-play.dbt_covid_dev_cleansed.us_state_county_daily_cases`
-- WHERE
--   cases = 0

SELECT
  *
FROM
  `big-query-horse-play.dbt_covid_dev_cleansed.us_state_county_daily_deaths`
WHERE
  date = "2020-06-22"
  AND state_name = 'CO'
  AND county_name IN ('Garfield County', 'Larimer County', 'Routt County')


SELECT
  cases.state_fips_code,
  cases.state_name,
  cases.county_fips_code,
  cases.county_name,
  cases.date,
  cases.cases,
  deaths.deaths
FROM
  `big-query-horse-play.dbt_covid_dev_cleansed.us_state_county_daily_cases` AS cases
LEFT JOIN
  `big-query-horse-play.dbt_covid_dev_cleansed.us_state_county_daily_deaths` AS deaths
ON
  cases.state_fips_code = deaths.state_fips_code
  AND cases.county_fips_code = deaths.county_fips_code
  AND cases.date = deaths.date
WHERE
  cases.date = "2020-06-22"
  AND cases.state_name = 'CO'
  AND cases.county_name IN ('Garfield County', 'Larimer County', 'Routt County')


-- Ah! Some fips codes have inconsistent length 008 vs 08
-- write test for this

SELECT
  DISTINCT length(state_fips_code) AS len,
  COUNT(state_fips_code) AS count
FROM
  `big-query-horse-play.dbt_covid_dev_cleansed.us_state_county_daily_deaths`
GROUP BY
  len

SELECT
  state_fips_code,
  state_name,
  date
FROM
  `big-query-horse-play.dbt_covid_dev_cleansed.us_state_county_daily_deaths`
WHERE
  length(state_fips_code) = 3
  AND state_name = 'CO'


SELECT
  DISTINCT
  state_fips_code,
  state_name
FROM
  `big-query-horse-play.dbt_covid_dev_cleansed.us_state_county_daily_deaths`
WHERE
--   length(state_fips_code) = 3
  state_name IN ('CO', 'CA', 'AK', 'AZ', 'CT')


SELECT
  DISTINCT
  state_fips_code,
  state_name,
FROM
  `big-query-horse-play.dbt_covid_dev_cleansed.us_state_county_daily_cases`
WHERE
  state_name IN ('CO', 'CA', 'AK', 'AZ', 'CT')

SELECT
  DISTINCT
  state_fips_code,
  if(length(state_fips_code) = 3, substr(state_fips_code, 2), state_fips_code) AS cleaned_code,
  state_name,
FROM
  `big-query-horse-play.dbt_covid_dev_cleansed.us_state_county_daily_deaths`
WHERE
  state_name IN ('CO', 'CA', 'AK', 'AZ', 'CT')
