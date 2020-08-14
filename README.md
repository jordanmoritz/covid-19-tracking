All relevant resources related to this [COVID-19 Case Tracker dashboard](https://datastudio.google.com/s/tReESJ2kyTM).

Built using dbt for orchestrating data transformation and managing pipelines. Various GCP tools (BigQuery, Cloud Functions, Pub/Sub, etc.). BeautifulSoup, requests, and pandas for web scraping. Data Studio for dashboard front-end.

**Consists of the following elements:**
- [Data loader](# data-loader)
- [Web scraping](# web-scraping)
- [Production run trigger](# production-run-trigger)
- [dbt](# dbt)
- [Exploratory work](# exploratory-work)

### Data Loader
Uses Cloud Scheduler to regularly invoke Cloud Function that checks for updates to underlying data sources. As refreshed data is available, loads updated data into destination tables in BigQuery. On success, posts message to Pub/Sub topic.

### Web scraping
Scrapes relevant sites for data. Applies simple transformation. Loads data into BigQuery.

### Production run trigger
Cloud Function invoked by Pub/Sub message from Data Loader. Responds by hitting dbt Cloud job run endpoint for related dbt production run.

### dbt
All data transformation required to build data models that meet front-end needs for Dashboard. Also handles incremental loading, testing, and documentation.

### Exploratory work
Just a parking lot for related exploratory work, testing, rough dev files for reference purposes. Will be removed once project is complete.
