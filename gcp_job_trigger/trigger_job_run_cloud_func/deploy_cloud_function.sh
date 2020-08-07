
gcloud functions deploy trigger-job-run \
        --entry-point='main' \
        --memory='128MB' \
        --runtime='python37' \
        --service-account=$SERVICE_ACCOUNT_EMAIL \
        --source=$SOURCE_PATH \
        --set-env-vars DBT_API_TOKEN=$DBT_API_TOKEN,DBT_ACCT_ID=$DBT_ACCT_ID,DBT_JOB_ID=$DBT_JOB_ID \
        --trigger-topic='data-source-update-status'
