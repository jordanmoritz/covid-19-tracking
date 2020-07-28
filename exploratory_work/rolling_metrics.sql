CREATE OR REPLACE TABLE `big-query-horse-play.sandbox_jm.rolling_metrics` AS
SELECT
  *
FROM
  `big-query-horse-play.dbt_covid_dev_presentation.country_daily_summary`
WHERE
  country_territory_name = 'United States'
--   AND cumulative_cases > 0
  AND date BETWEEN '2020-01-13' AND '2020-02-28'
ORDER BY
  date DESC


SELECT
  date,
  country_territory_name,
  cumulative_cases,
  new_cases,
  SUM(new_cases) OVER (country_7_days) AS new_cases_last_7,
  ROUND(AVG(new_cases) OVER (country_7_days), 2) AS avg_daily_new_cases_last_7,
FROM
  `big-query-horse-play.sandbox_jm.rolling_metrics`
WINDOW
  country_7_days AS (
  PARTITION BY
    country_territory_name
  ORDER BY
    date DESC ROWS BETWEEN CURRENT ROW
    AND 6 FOLLOWING)
