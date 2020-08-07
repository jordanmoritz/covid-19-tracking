import os, requests

token = os.environ.get('DBT_API_TOKEN')
account_id = os.environ.get('DBT_ACCT_ID')
job_id = os.environ.get('DBT_JOB_ID')

headers = {'Authorization': f'Token {token}'}

request_data = {'cause': 'API Test Trigger'}

job_endpoint = f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/'

response = requests.post(job_endpoint, headers=headers, params={}, data=request_data)
