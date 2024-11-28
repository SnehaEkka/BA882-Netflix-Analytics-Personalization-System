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
from sklearn.model_selection import train_test_split
from google.cloud import storage, secretmanager
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

@functions_framework.http
def task(request):
    # Job ID for tracking
    job_id = datetime.datetime.now().strftime("%Y%m%d%H%M") + "-" + str(uuid.uuid4())

    # Instantiate services
    sm = secretmanager.SecretManagerServiceClient()
    storage_client = storage.Client()

    # Secret token retrieval
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # Database connection
    md = duckdb.connect(f'md:?motherduck_token={md_token}')

    # Dataset loading
    GCS_PATH = "gs://ba882-team05-vertex-models/training-data/netflix-api-recommendation/netflix_data.csv"
    df = pd.read_csv(GCS_PATH)

    # Model preprocessing
    df_movie = df[df["showType"] == "movie"]
    columns_of_interest = ['genres', 'cast', 'directors', 'overview', 'title']
    df_movie = df_movie[columns_of_interest]

    # Preprocess fields
    def preprocess_field(field):
        if isinstance(field, str):
            return ' '.join([item.strip().strip("'\"") for item in field.strip('[]').split(',')])
        return ''
    for col in ['genres', 'cast', 'directors']:
        df_movie[col] = df_movie[col].apply(preprocess_field)
    df_movie['text_features'] = df_movie['genres'] + ' ' + df_movie['cast'] + ' ' + df_movie['directors'] + ' ' + df_movie['overview']
    df_movie = df_movie.dropna(subset=['text_features', 'title'])

    # Train/Test Split and Model Training
    train_set, test_set = train_test_split(df_movie, test_size=0.3, random_state=42)
    vectorizer = TfidfVectorizer(stop_words='english')
    tfidf_matrix_train = vectorizer.fit_transform(train_set['text_features'])
    knn = NearestNeighbors(n_neighbors=10, metric='cosine')
    knn.fit(tfidf_matrix_train)

    # Save model and vectorizer
    GCS_BUCKET = "ba882-team05-vertex-models"
    GCS_PATH = "models/netflix-movies/"
    with GCSFileSystem().open(f"gs://{GCS_BUCKET}/{GCS_PATH}knn_model.joblib", 'wb') as f:
        joblib.dump(knn, f)
    with GCSFileSystem().open(f"gs://{GCS_BUCKET}/{GCS_PATH}vectorizer.joblib", 'wb') as f:
        joblib.dump(vectorizer, f)

    # Metrics and Parameters
    ingest_timestamp = pd.Timestamp.now()
    metrics = {'job_id': job_id, 'created_at': ingest_timestamp}
    params = {"model": 'NearestNeighbors', 'created_at': ingest_timestamp, 'job_id': job_id}
    metrics_df = pd.DataFrame([metrics])
    params_df = pd.DataFrame([params])

    # Insert to DB
    md.sql(f"INSERT INTO {db_schema}.movies_model_runs (job_id, gcs_path) VALUES ('{job_id}', '{GCS_BUCKET + GCS_PATH}')")
    md.sql(f"INSERT INTO {db_schema}.movies_job_metrics SELECT * from metrics_df")
    md.sql(f"INSERT INTO {db_schema}.movies_job_parameters SELECT * from params_df")

    # Return data
    return_data = {
        'job_id': job_id,
        "model_path": f"gs://{GCS_BUCKET}/{GCS_PATH}knn_model.joblib",
        "parameters": {"model": "NearestNeighbors"}
    }
    return return_data, 200