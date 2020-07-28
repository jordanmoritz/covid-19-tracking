
-- Aggregation of us data at state level granularity
select
    date,
    state_name,
    sum(county_population) AS state_population,
    sum(cumulative_cases) AS cumulative_cases,
    sum(cumulative_deaths) AS cumulative_deaths,
