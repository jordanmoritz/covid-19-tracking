SELECT
  *,
  IF(MAX(date) OVER (PARTITION BY country_territory_name) = date, 1, 0) AS most_recent_date
FROM
  `big-query-horse-play.dbt_covid_dev_presentation.country_daily_summary`
WHERE
  country_territory_name = 'United States'
ORDER BY
  date DESC
