# set the project
gcloud config set project ba882-inclass-project

echo "======================================================"
echo "deploying the ml dataset: netflix_data"
echo "======================================================"

gcloud functions deploy ml-netflix-data \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source /home/sekka/BA882-Team05-Project/prefect/functions/netflix-api-ml \
    --stage-bucket ba882-team05-functions \
    --service-account id-82-group-project@ba882-inclass-project.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB  \
    --timeout 300s