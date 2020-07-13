SELECT
  column_name
FROM
  `bigquery-public-data.covid19_usafacts`.INFORMATION_SCHEMA.COLUMNS
WHERE
  table_name = 'confirmed_cases'
  AND STARTS_WITH(column_name, '_')
