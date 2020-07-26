SELECT
  DISTINCT a.countries_and_territories,
  b.country,
  b.country_code,
  a.geo_id,
  a.pop_data_2019,
  b.year_2018
FROM
  `bigquery-public-data.covid19_ecdc.covid_19_geographic_distribution_worldwide` AS a
LEFT JOIN
  `bigquery-public-data.world_bank_global_population.population_by_country` AS b
ON
  b.country_code = a.country_territory_code
WHERE
-- I can factor in for this
--   b.country IS NULL
ORDER BY
  a.countries_and_territories ASC
