# set the project
gcloud config set project ba882-inclass-project

echo "======================================================"
echo "deploying the movies training function"
echo "======================================================"

gcloud functions deploy ml-movies-train \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point main \
    --source /home/sekka/BA882-Team05-Project/ml/functions/movies-knn-train \
    --stage-bucket ba882-team05-functions \
    --service-account id-82-group-project@ba882-inclass-project.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1GB  \
    --timeout 540s 

echo "======================================================"
echo "deploying the movies inference function"
echo "======================================================"

gcloud functions deploy ml-movies-serve \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point main \
    --source /home/sekka/BA882-Team05-Project/ml/functions/movies-knn-serve \
    --stage-bucket ba882-team05-functions \
    --service-account id-82-group-project@ba882-inclass-project.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1GB  \
    --timeout 540s

echo "======================================================"
echo "deploying the shows training function"
echo "======================================================"

gcloud functions deploy ml-shows-train \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point main \
    --source /home/sekka/BA882-Team05-Project/ml/functions/shows-knn-train \
    --stage-bucket ba882-team05-functions \
    --service-account id-82-group-project@ba882-inclass-project.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1GB  \
    --timeout 540s 

echo "======================================================"
echo "deploying the shows inference function"
echo "======================================================"

gcloud functions deploy ml-shows-serve \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point main \
    --source /home/sekka/BA882-Team05-Project/ml/functions/shows-knn-serve \
    --stage-bucket ba882-team05-functions \
    --service-account id-82-group-project@ba882-inclass-project.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 1GB  \
    --timeout 540s