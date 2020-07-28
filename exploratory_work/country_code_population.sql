SELECT
  DISTINCT a.countries_and_territories AS source_country_value,
  b.country AS mapping_country,
--   c.country_name AS mapping_country_name,
--   a.geo_id AS source_geo_id,
  a.country_territory_code AS source_country_territory_code,
  b.country_code AS mapping_country_code,
--   c.stanag_code AS mapping_stanag_code,
--   c.fips_code AS mapping_fips_code
--   a.pop_data_2019,
--   b.year_2018,
--   a.pop_data_2019 - b.year_2018 AS pop_diff --we want pop data from ecdc (commonly eurostat it seems?)
FROM
  `bigquery-public-data.covid19_ecdc.covid_19_geographic_distribution_worldwide` AS a

LEFT JOIN
  `bigquery-public-data.world_bank_global_population.population_by_country` AS b
ON
  b.country_code = a.country_territory_code

-- This is out, way too many mismatches
-- LEFT JOIN
--   `bigquery-public-data.utility_us.country_code_iso` AS c
-- ON
--   c.fips_code = a.geo_id

WHERE
-- I can factor in for this
  b.country IS NULL
-- c.country_name IS NULL
ORDER BY
  a.countries_and_territories ASC
--   pop_diff DESC
--   a.pop_data_2019 DESC



-- these aren't in mapping dataset with another code
SELECT
country, country_code
FROM
  `bigquery-public-data.world_bank_global_population.population_by_country`
WHERE
LOWER(country) LIKE '%anguilla%'


-- And not getting any inflation. all good
SELECT
  DISTINCT countries_and_territories
FROM
  `bigquery-public-data.covid19_ecdc.covid_19_geographic_distribution_worldwide`


-- Looks good
SELECT
  DISTINCT country_territory_name
FROM
  `big-query-horse-play.dbt_covid_dev_cleansed.ecdc_country_daily_volume`
ORDER BY
  country_territory_name DESC
