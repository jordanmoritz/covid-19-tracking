WITH a AS (
SELECT
  state,
  county_name,
  DATE '2020-03-18' AS date,
  _3_18_20 AS cases
FROM
  `big-query-horse-play.dbt_dev_jm.county_cases_date_columns`
),

b AS (
SELECT
  state,
  county_name,
  DATE '2020-03-19' AS date,
  _3_19_20 AS cases
FROM
  `big-query-horse-play.dbt_dev_jm.county_cases_date_columns`
),

c AS (
SELECT
  state,
  county_name,
  DATE '2020-03-20' AS date,
  _3_20_20 AS cases
FROM
  `big-query-horse-play.dbt_dev_jm.county_cases_date_columns`
)

SELECT * FROM a
UNION ALL
SELECT * FROM b
UNION ALL
SELECT * FROM c
