-- Things to account for:
-- first day where there is no previous day for comparison
-- day where same value as previous day

SELECT
  *,
  cases - LEAD(cases, 1, 0) OVER (county_daily) AS new_cases
FROM
  `big-query-horse-play.sandbox_jm.county_state_daily_cases`
WINDOW county_daily AS (PARTITION BY state_name, county_name ORDER BY date DESC)
ORDER BY county_name, date DESC
