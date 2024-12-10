# imports
import functions_framework
import joblib
import json
from gcsfs import GCSFileSystem
import pandas as pd
from google.cloud import secretmanager
from google.cloud import storage
import duckdb

# db setup
db = 'ba882_project'
schema = "mlops"
db_schema = f"{db}.{schema}"

# settings
project_id = 'ba882-inclass-project'
secret_id = 'duckdb-token'
version_id = 'latest'
gcp_region = 'us-central1'

# Instantiate services
sm = secretmanager.SecretManagerServiceClient()
storage_client = storage.Client()

# Build the resource name of the secret version
name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

# Access the secret version
response = sm.access_secret_version(request={"name": name})
md_token = response.payload.data.decode("UTF-8")

# Initiate the MotherDuck connection through an access token
md = duckdb.connect(f'md:?motherduck_token={md_token}')

@functions_framework.http
def task(request):
    """
    Fetch model information and make predictions based on the title provided.
    """

    # Parse the request data
    request_json = request.get_json(silent=True)
    print(request_json)

    # Extract the title from the input data
    title = request_json.get('title')
    if not title:
        return {"error": "Title is required for prediction."}, 400

    # Query to determine if the title is a movie or a show
    query = f"""
    SELECT showType FROM ba882_project.stage.netflix_api WHERE LOWER(title) = LOWER('{title}')
    """
    result_df = md.sql(query).df()

    if result_df.empty:
        return {"error": f"Title '{title}' not found in the database."}, 404

    show_type = result_df['showType'][0]
    print(f"Title '{title}' is identified as a '{show_type}'.")

    # Determine the appropriate table and fetch the best model based on showType
    if show_type == "movie":
        metrics_table = f"{db_schema}.movies_job_metrics"
        runs_table = f"{db_schema}.movies_model_runs"
        # model_name_filter = "%movie%"
    elif show_type == "series":
        metrics_table = f"{db_schema}.shows_job_metrics"
        runs_table = f"{db_schema}.shows_model_runs"
        # model_name_filter = "%show%"
    else:
        return {"error": f"Invalid showType '{show_type}' for title '{title}'."}, 400

    # Query to fetch the best model for the given type (movie or show)
    sql_query = f"""
    SELECT m.*, r.model_path 
    FROM {metrics_table} m 
    INNER JOIN {runs_table} r 
    ON m.job_id = r.job_id 
    WHERE m.metric_name = 'MAP@K'
    ORDER BY m.created_at DESC 
    LIMIT 1
    """
    
    results = md.sql(sql_query).df()

    if results.empty:
        return {"error": f"No trained model found for {show_type}."}, 404

    model_path = results['model_path'][0]
    print(f"Fetching model from: {model_path}")

    # Load the model pipeline from GCS
    with GCSFileSystem().open(model_path, 'rb') as f:
        model_pipeline = joblib.load(f)

    # Flatten for inclusion in the result
    json_output = results.iloc[0].to_dict()
    json_output['created_at'] = json_output['created_at'].isoformat()

    # Prepare data for prediction (based on input data key)
    data_list = request_json.get('data')
    
    if not data_list or not isinstance(data_list, list):
        return {"error": "A valid 'data' list is required for prediction."}, 400

    print(f"Data for prediction: {data_list}")

    # Run predictions using the loaded model pipeline
    preds = model_pipeline.predict(data_list)

    # Convert predictions to a list for returning in JSON format
    preds_list = preds.tolist()

    return {
        'predictions': preds_list,
        'model_info': json_output,
        'title': title,
        'showType': show_type,
        'data': data_list
    }, 200
