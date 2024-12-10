# Final movies trainer

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
from sklearn.metrics import average_precision_score
from sklearn.metrics.pairwise import cosine_similarity
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

##################################################### helpers

def load_sql(p):
    with open(p, "r") as f:
        sql = f.read()
    return sql
	
def preprocess_field(field):
    if isinstance(field, str):
        return ' '.join([item.strip().strip("'\"") for item in field.strip('[]').split(',')])
    return ''

##################################################### task

@functions_framework.http
def task(request):
    # Job ID for tracking
    job_id = datetime.datetime.now().strftime("%Y%m%d%H%M") + "-" + str(uuid.uuid4())

    # Instantiate services
    sm = secretmanager.SecretManagerServiceClient()
    storage_client = storage.Client()

    # Secret token retrieval for DuckDB connection
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # Database connection
    md = duckdb.connect(f'md:?motherduck_token={md_token}')

    # Dataset loading from GCS or SQL query
    sql = load_sql("dataset.sql")
    df = md.sql(sql).df()

    # Model preprocessing: Filter movies and preprocess fields of interest
    df_movie = df[df["showType"] == "movie"]
    columns_of_interest = ['genres', 'cast', 'directors', 'overview', 'title']
    df_movie = df_movie[columns_of_interest]

    # Preprocess text fields and combine features into a single column																  
    for col in ['genres', 'cast', 'directors']:
        df_movie[col] = df_movie[col].apply(preprocess_field)

    df_movie['text_features'] = df_movie['genres'] + ' ' + df_movie['cast'] + ' ' + df_movie['directors'] + ' ' + df_movie['overview']
    
    # Drop rows with missing values in required fields
    df_movie = df_movie.dropna(subset=['text_features', 'title'])

    # Parse incoming request to get parameters (if any)
    try:
        request_json = request.get_json()
    except Exception:
        request_json = {}

    # Model parameters with default values
    n_neighbors = request_json.get('n_neighbors', 10)
    metric = request_json.get('metric', 'cosine')
    model_name = 'KNearestNeighbors'

    # Train/Test Split and Model Training
    train_set = df_movie.copy()
    # Vectorize text features using TfidfVectorizer											   
    vectorizer = TfidfVectorizer(stop_words='english')
    tfidf_matrix_train = vectorizer.fit_transform(train_set['text_features'])

    # KNN model training					
    knn = NearestNeighbors(n_neighbors=n_neighbors, metric=metric)
    knn.fit(tfidf_matrix_train)

    # Save model and vectorizer to GCS
    GCS_BUCKET = "ba882-team05-vertex-models"
    GCS_PATH_MODEL = f"models/netflix-movies/{job_id}"
    
    M_GCS = f"gs://{GCS_BUCKET}/{GCS_PATH_MODEL}/model/knn_model.joblib"
    V_GCS = f"gs://{GCS_BUCKET}/{GCS_PATH_MODEL}/model/vectorizer.joblib"

    with GCSFileSystem().open(M_GCS, 'wb') as f:
        joblib.dump(knn, f)

    with GCSFileSystem().open(V_GCS, 'wb') as f:
        joblib.dump(vectorizer, f)

    ###################################################### Metrics Calculation
	
    # Metric 1: MAP@K (Mean Average Precision at K)
    def calculate_map_at_k(knn, vectorizer, train_set, k=n_neighbors):
        total_ap = 0
        n_samples = len(train_set)

        for idx, row in train_set.iterrows():
            input_features = vectorizer.transform([row['text_features']])
            distances, indices = knn.kneighbors(input_features)

            current_genres = set(row['genres'].split())
            recommended_genres = [set(train_set.iloc[i]['genres'].split()) for i in indices.flatten()[1:]]

            relevance = [1 if current_genres.intersection(rec_genres) else 0 for rec_genres in recommended_genres]
            ap = average_precision_score(relevance, [1/(i+1) for i in range(k-1)])
            total_ap += ap

        return total_ap / n_samples

    map_at_k = calculate_map_at_k(knn, vectorizer, train_set)

    # Metric 2: Coverage
    def calculate_coverage(knn, vectorizer, train_set, k=n_neighbors):
        all_recommended_items = set()
        total_items = set(train_set.index)

        for idx, row in train_set.iterrows():
            input_features = vectorizer.transform([row['text_features']])
            distances, indices = knn.kneighbors(input_features)
            recommended_items = set(indices.flatten()[1:])
            all_recommended_items.update(recommended_items)

        return len(all_recommended_items) / len(total_items)

    coverage = calculate_coverage(knn, vectorizer, train_set)

    # Metric 3: Intra-list Similarity
    def calculate_intra_list_similarity(knn, vectorizer, train_set, k=n_neighbors):
        total_similarity = 0
        count = 0

        for idx, row in train_set.iterrows():
            input_features = vectorizer.transform([row['text_features']])
            distances, indices = knn.kneighbors(input_features)

            recommended_items_indices = indices.flatten()[1:]
            if len(recommended_items_indices) > 1:
                recommended_features = vectorizer.transform(train_set.iloc[recommended_items_indices]['text_features'])
                similarity_matrix = cosine_similarity(recommended_features)

                n_recommendations = similarity_matrix.shape[0]
                similarity_sum = (similarity_matrix.sum() - n_recommendations) / 2  # Exclude self-similarities

                avg_similarity_within_list = similarity_sum / (n_recommendations * (n_recommendations - 1) / 2)
                total_similarity += avg_similarity_within_list
                count += 1

        return total_similarity / count if count > 0 else 0

    intra_list_similarity = calculate_intra_list_similarity(knn, vectorizer, train_set)

    insert_query = f"""
    INSERT INTO {db_schema}.movies_model_runs (job_id, name, gcs_path, model_path, vectorizer_path)
    VALUES ('{job_id}', '{model_name}', '{GCS_BUCKET + "/" + GCS_PATH_MODEL}', '{M_GCS}', '{V_GCS}');
    """
    print(f"mlops to warehouse: {insert_query}")
    md.sql(insert_query)
	
    # Store metrics in database (metrics table)
    ingest_timestamp = pd.Timestamp.now()
    
    metrics_df_data = [
        {'job_id': job_id, 'metric_name': 'MAP@K', 'metric_value': map_at_k, 'created_at': ingest_timestamp},
        {'job_id': job_id, 'metric_name': 'Coverage', 'metric_value': coverage, 'created_at': ingest_timestamp},
        {'job_id': job_id, 'metric_name': 'Intra-list Similarity', 'metric_value': intra_list_similarity, 'created_at': ingest_timestamp}
    ]
    
    metrics_df = pd.DataFrame(metrics_df_data)
    
    md.sql(f"INSERT INTO {db_schema}.movies_job_metrics SELECT * FROM metrics_df")

    # Record parameters (parameters table)
    params_df_data = [
        {"job_id": job_id, "parameter_name": "k", "parameter_value": n_neighbors, "created_at": ingest_timestamp},
        {"job_id": job_id, "parameter_name": "metric", "parameter_value": metric, "created_at": ingest_timestamp}
    ]
    
    params_df = pd.DataFrame(params_df_data)

    md.sql(f"INSERT INTO {db_schema}.movies_job_parameters SELECT * FROM params_df")

    return {
        "job_id": job_id,
        "map_at_k": map_at_k,
        "coverage": coverage,
        "intra_list_similarity": intra_list_similarity,
        "model_path": M_GCS,
        "vectorizer_path": V_GCS,
        "parameters": {
            "k": n_neighbors,
            "metric": metric
        }
    }, 200