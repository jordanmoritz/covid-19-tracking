CREATE OR REPLACE TABLE `big-query-horse-play.sandbox_jm.negative_new_metrics` AS
SELECT
  *
FROM
  `big-query-horse-play.dbt_covid_dev_presentation.country_daily_summary`
WHERE
--   (new_cases < 0 OR new_deaths < 0)
  country_territory_code = 'ESP'
ORDER BY
  date DESC
