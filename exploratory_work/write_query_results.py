# Create job config obj
job_config = bigquery.QueryJobConfig(
                        destination = 'big-query-horse-play.covid_test.test_results',
                        allow_large_results=True,
                        write_disposition='WRITE_TRUNCATE')

# Construct query job, make call
query_job = cases.destination_client.query(query='SELECT 2 AS num', job_config=job_config, job_id='test_id_123')

# Wait for completion
query_job.result()
