-- SELECT SUM(confirmed_cases) FROM `bigquery-public-data.covid19_ecdc.covid_19_geographic_distribution_worldwide` WHERE date = "2020-07-29" AND country_territory_code = 'USA'

SELECT SUM(confirmed_cases) FROM `bigquery-public-data.covid19_usafacts.summary` WHERE DATE = "2020-07-29"

-- SELECT SUM(cumulative_cases) FROM `big-query-horse-play.dbt_covid_dev_presentation.us_state_daily_summary`  WHERE DATE = "2020-07-29"

-- SELECT SUM(confirmed) FROM `bigquery-public-data.covid19_jhu_csse.summary`  WHERE DATE = "2020-07-29" AND country_region = 'US'

-- SELECT SUM(confirmed_cases) FROM `bigquery-public-data.covid19_nyt.us_states`  WHERE DATE = "2020-07-29" GROUP BY date

-- SELECT country_code, country_name, subregion1_name FROM `bigquery-public-data.covid19_open_data.covid19_open_data`  WHERE DATE = "2020-07-29" AND country_code = 'US'
