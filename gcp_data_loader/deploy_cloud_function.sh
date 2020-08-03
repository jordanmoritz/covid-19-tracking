
gcloud functions deploy update-data-source \
        --entry-point='main' \
        --memory='128MB' \
        --runtime='python37' \
        --service-account=$SERVICE_ACCOUNT_EMAIL \
        --source=$SOURCE_PATH \
        --trigger-http
