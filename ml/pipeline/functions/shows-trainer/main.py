# imports
import functions_framework
import os
import pandas as pd
import numpy as np
import joblib
import json
import uuid
import datetime
from gcsfs import GCSFileSystem
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import warnings
from google.cloud import storage, secretmanager
import duckdb

warnings.filterwarnings("ignore", message="X does not have valid feature names")

# settings
project_id = 'ba882-inclass-project'
secret_id = 'duckdb-token'
version_id = 'latest'
project_region = 'us-central1'

# db setup
db = 'ba882_project'
schema = "mlops"
db_schema = f"{db}.{schema}"

@functions_framework.http
def task(request):
    # Unique job ID for tracking
    job_id = datetime.datetime.now().strftime("%Y%m%d%H%M") + "-" + str(uuid.uuid4())

    # Instantiate Secret Manager to retrieve tokens
    sm = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # Establish a MotherDuck database connection
    md = duckdb.connect(f'md:?motherduck_token={md_token}')

    # Define the GCS path for the dataset
    GCS_PATH = "gs://ba882-team05-vertex-models/training-data/netflix-api-recommendation/netflix_data.csv"
    df = pd.read_csv(GCS_PATH)
    print(df.head())

    # Filter to only "series" shows and select relevant columns
    df_show = df[df["showType"] == "series"]
    columns_of_interest = ['genres', 'cast', 'directors', 'overview', 'title', 'episodeCount', 'seasonCount']
    df_show = df_show[columns_of_interest]

    # Preprocessing function for string fields
    def preprocess_field(field):
        if isinstance(field, str):
            field = field.strip('[]')
            items = field.split(',')
            return ' '.join([item.split(':')[-1].strip().strip("'\"") for item in items])
        return ''

    # Apply preprocessing
    for col in ['genres', 'cast', 'directors']:
        df_show[col] = df_show[col].apply(preprocess_field)

    # Combine text features and clean dataset
    df_show['text_features'] = df_show['genres'] + ' ' + df_show['cast'] + ' ' + df_show['directors'] + ' ' + df_show['overview']
    df_show = df_show.dropna(subset=['text_features', 'title', 'episodeCount', 'seasonCount'])

    # Convert numeric fields
    df_show['episodeCount'] = pd.to_numeric(df_show['episodeCount'], errors='coerce')
    df_show['seasonCount'] = pd.to_numeric(df_show['seasonCount'], errors='coerce')

    # Split into training and testing sets
    train_set, test_set = train_test_split(df_show, test_size=0.3, random_state=42)

    # Text feature vectorization
    vectorizer = TfidfVectorizer(stop_words='english')
    tfidf_matrix_train = vectorizer.fit_transform(train_set['text_features'])

    # Scale numeric features
    scaler = StandardScaler()
    count_features_train = scaler.fit_transform(train_set[['episodeCount', 'seasonCount']])
    features_train = np.hstack((tfidf_matrix_train.toarray(), count_features_train))

    # KNN model
    knn = NearestNeighbors(n_neighbors=10, metric='cosine')
    knn.fit(features_train)

    # Save models to GCS
    GCS_BUCKET = "ba882-team05-vertex-models"
    GCS_PATH = "models/netflix-shows/"
    with GCSFileSystem().open(f"gs://{GCS_BUCKET}/{GCS_PATH}knn_model.joblib", 'wb') as f:
        joblib.dump(knn, f)
    with GCSFileSystem().open(f"gs://{GCS_BUCKET}/{GCS_PATH}vectorizer.joblib", 'wb') as f:
        joblib.dump(vectorizer, f)
    with GCSFileSystem().open(f"gs://{GCS_BUCKET}/{GCS_PATH}scaler.joblib", 'wb') as f:
        joblib.dump(scaler, f)

    # Save show metadata
    show_metadata = df_show[['genres', 'cast', 'directors', 'overview', 'title', 'episodeCount', 'seasonCount']].to_dict(orient='records')
    with GCSFileSystem().open(f"gs://{GCS_BUCKET}/{GCS_PATH}show_metadata.json", 'w') as f:
        json.dump(show_metadata, f)

    # Logging metrics and parameters
    ingest_timestamp = pd.Timestamp.now()
    metrics = {'job_id': job_id, 'created_at': ingest_timestamp}
    params = {"model": 'NearestNeighbors', 'created_at': ingest_timestamp, 'job_id': job_id}
    metrics_df = pd.DataFrame([metrics])
    params_df = pd.DataFrame([params])

    # Insert to DB
    md.sql(f"INSERT INTO {db_schema}.shows_model_runs (job_id, gcs_path) VALUES ('{job_id}', '{GCS_BUCKET + GCS_PATH}')")
    md.sql(f"INSERT INTO {db_schema}.shows_job_metrics SELECT * from metrics_df")
    md.sql(f"INSERT INTO {db_schema}.shows_job_parameters SELECT * from params_df")

    # Return job metadata
    return_data = {
        'job_id': job_id,
        "model_path": f"gs://{GCS_BUCKET}/{GCS_PATH}knn_model.joblib",
        "parameters": {"model": "NearestNeighbors"}
    }
    return return_data, 200