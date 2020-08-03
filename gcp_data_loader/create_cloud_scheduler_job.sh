
gcloud scheduler jobs create http cloud-func-check-update-data-source \
        --schedule="0 */6 * * *" \
        --time-zone="America/Vancouver" \
        --description="Responsible for invoking Cloud Function to check for data source updates" \
        --uri=$CLOUD_FUNC_TRIGGER_URL \
        --http-method="get" \
        --oidc-service-account-email=$SERVICE_ACCOUNT_EMAIL
