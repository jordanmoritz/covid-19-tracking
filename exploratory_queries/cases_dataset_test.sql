-- SELECT
--   date,
--   state,
--   county_name,
--   confirmed_cases
-- FROM
--   `bigquery-public-data.covid19_usafacts.summary`
-- WHERE
--   DATE(_PARTITIONTIME) = "2020-07-13"
--   AND date BETWEEN '2020-03-18' AND '2020-03-20'
--   AND state = 'OR'
--   AND county_name = 'Linn County'

SELECT
  state,
  county_name,
  _3_18_20, --15
  _3_19_20, --18
  _3_20_20 --19
FROM
  `bigquery-public-data.covid19_usafacts.confirmed_cases`
WHERE
  DATE(_PARTITIONTIME) = "2020-07-13"
--   AND date = '2020-03-18'
  AND state = 'OR'
  AND county_name = 'Linn County'
