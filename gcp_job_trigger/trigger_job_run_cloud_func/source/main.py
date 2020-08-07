import os, base64, requests

dbt_api_token = os.environ.get('DBT_API_TOKEN')
dbt_account_id = os.environ.get('DBT_ACCT_ID')
dbt_job_id = os.environ.get('DBT_JOB_ID')

headers = {'Authorization': f'Token {dbt_api_token}'}

request_data = {'cause': 'API Triggered Update'}

dbt_job_endpoint = f'https://cloud.getdbt.com/api/v2/accounts/{dbt_account_id}/jobs/{dbt_job_id}/run/'

def main(event, context):

    message = base64.b64decode(event['data']).decode('utf-8')

    if 'data sources updated' in message:
        trigger_job_response = requests.post(dbt_job_endpoint,
                                            headers=headers,
                                            params={},
                                            data=request_data)

        status_code = trigger_job_response.status_code

        if status_code == 200:
            trigger_id = trigger_job_response.json().get('data').get('trigger_id')
            print(f'Status: {status_code}\nTrigger ID: {trigger_id}')
        else:
            print(f'Trigger request failed with status: {status_code}')
            print(f'Requested URL: {trigger_job_response.url}')
            print(f'Response text: {trigger_job_response.text}')
