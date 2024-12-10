gcloud config set project ba882-inclass-project

echo "======================================================"
echo "build (no cache)"
echo "======================================================"

docker build --no-cache -t gcr.io/ba882-inclass-project/streamlit-poc:v1 .

echo "======================================================"
echo "push"
echo "======================================================"

docker push gcr.io/ba882-inclass-project/streamlit-poc:v1

echo "======================================================"
echo "deploy run"
echo "======================================================"


gcloud run deploy streamlit-poc \
    --image gcr.io/ba882-inclass-project/streamlit-poc:v1 \
    --platform managed \
    --region us-central1 \
    --allow-unauthenticated \
    --service-account id-82-group-project@ba882-inclass-project.iam.gserviceaccount.com \
    --memory 1Gi