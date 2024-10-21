########################################
## Script for now to deploy functions ##
########################################

# setup the project
gcloud config set project ba882-inclass-project

# schema setup
echo "======================================================"
echo "deploying the schema setup"
echo "======================================================"

gcloud functions deploy schema-netflix \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source /home/sekka/BA882-Team05-Project/functions/schema-netflix \
    --stage-bucket ba882-team05 \
    --service-account id-82-group-project@ba882-inclass-project.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB \
	--timeout 540s
	
gcloud functions deploy schema-top10 \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source /home/sekka/BA882-Team05-Project/functions/schema-top10 \
    --stage-bucket ba882-team05 \
    --service-account id-82-group-project@ba882-inclass-project.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB \
	--timeout 540s
	
gcloud functions deploy schema-youtube \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point task \
    --source /home/sekka/BA882-Team05-Project/functions/schema-youtube \
    --stage-bucket ba882-team05 \
    --service-account id-82-group-project@ba882-inclass-project.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB \
	--timeout 540s

# extract data
echo "======================================================"
echo "deploying the extractor"
echo "======================================================"

gcloud functions deploy extract-netflix \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point main \
    --source /home/sekka/BA882-Team05-Project/functions/extract-netflix \
    --stage-bucket ba882-team05 \
    --service-account id-82-group-project@ba882-inclass-project.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB \
	--timeout 540s

gcloud functions deploy extract-top10 \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point main \
    --source /home/sekka/BA882-Team05-Project/functions/extract-top10 \
    --stage-bucket ba882-team05 \
    --service-account id-82-group-project@ba882-inclass-project.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB \
	--timeout 540s 
	
gcloud functions deploy extract-youtube \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point main \
    --source /home/sekka/BA882-Team05-Project/functions/extract-youtube \
    --stage-bucket ba882-team05 \
    --service-account id-82-group-project@ba882-inclass-project.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
    --memory 512MB \
	--timeout 540s


# load data into raw and changes into stage
echo "======================================================"
echo "deploying the loader"
echo "======================================================"

gcloud functions deploy load-all-data \
    --gen2 \
    --runtime python311 \
    --trigger-http \
    --entry-point main \
    --source /home/sekka/BA882-Team05-Project/functions/load-all-data \
    --stage-bucket ba882-team05 \
    --service-account id-82-group-project@ba882-inclass-project.iam.gserviceaccount.com \
    --region us-central1 \
    --allow-unauthenticated \
	--timeout 540s