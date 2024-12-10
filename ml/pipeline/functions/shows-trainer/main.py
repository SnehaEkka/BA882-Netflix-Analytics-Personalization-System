# Final shows trainer

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

###################################################### helpers

def load_sql(p):
    with open(p, "r") as f:
        sql = f.read()
    return sql

def preprocess_field(field):
    if isinstance(field, str):
        return ' '.join([item.strip().strip("'\"") for item in field.strip('[]').split(',')])
    return ''

###################################################### task

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

    # Model preprocessing: Filter shows and preprocess fields of interest
    df_show = df[df["showType"] == "series"]
    columns_of_interest = ['genres', 'cast', 'directors', 'overview', 'title', 'episodeCount', 'seasonCount']
    df_show = df_show[columns_of_interest]

    # Preprocess text fields and combine features into a single column
    for col in ['genres', 'cast', 'directors']:
        df_show[col] = df_show[col].apply(preprocess_field)

    df_show['text_features'] = (df_show['genres'] + ' ' + df_show['cast'] + ' ' + df_show['directors'] + ' ' + df_show['overview'])

    # Drop rows with missing values in required fields
    df_show = df_show.dropna(subset=['text_features', 'title', 'episodeCount', 'seasonCount'])

    # Convert numeric fields to float type for scaling
    df_show['episodeCount'] = pd.to_numeric(df_show['episodeCount'], errors='coerce')
    df_show['seasonCount'] = pd.to_numeric(df_show['seasonCount'], errors='coerce')

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
    train_set = df_show.copy()

    # Split the dataset into training (70%) and testing (30%) sets
    train_set, test_set = train_test_split(train_set, test_size=0.5, random_state=42)

    # Vectorize text features using TfidfVectorizer
    vectorizer = TfidfVectorizer(stop_words='english')
    tfidf_matrix_train = vectorizer.fit_transform(train_set['text_features'])

    # Scale numeric features
    scaler = StandardScaler()
    count_features_train = scaler.fit_transform(train_set[['episodeCount', 'seasonCount']])
    count_features_train_df = pd.DataFrame(count_features_train, columns=['episodeCount', 'seasonCount'])

    features_train = np.hstack((tfidf_matrix_train.toarray(), count_features_train_df.values))

    # KNN model training
    knn = NearestNeighbors(n_neighbors=n_neighbors, metric=metric)
    knn.fit(features_train)

    # Save models to GCS
    GCS_BUCKET = "ba882-team05-vertex-models"
    GCS_PATH_MODEL = f"models/netflix-shows/{job_id}/"
    M_GCS = f"gs://{GCS_BUCKET}/{GCS_PATH_MODEL}/model/knn_model.joblib"
    V_GCS = f"gs://{GCS_BUCKET}/{GCS_PATH_MODEL}/model/vectorizer.joblib"
    S_GCS = f"gs://{GCS_BUCKET}/{GCS_PATH_MODEL}/model/scaler.joblib"

    with GCSFileSystem().open(M_GCS, 'wb') as f:
        joblib.dump(knn, f)
    with GCSFileSystem().open(V_GCS, 'wb') as f:
        joblib.dump(vectorizer, f)
    with GCSFileSystem().open(S_GCS, 'wb') as f:
        joblib.dump(scaler, f)

    ###################################################### Metrics Calculation

    # Metric 1: MAP@K (Mean Average Precision at K)
    def calculate_map_at_k(knn, vectorizer, scaler, train_set, k=n_neighbors):
        total_ap = 0
        n_samples = len(train_set)
        for idx, row in train_set.iterrows():
            # Combine text and numeric features
            text_features = vectorizer.transform([row['text_features']])
            count_features = scaler.transform(pd.DataFrame([[row['episodeCount'], row['seasonCount']]], columns=['episodeCount', 'seasonCount']))
            input_features = np.hstack((text_features.toarray(), count_features))

            # Get the distances and indices of the k-neighbors
            distances, indices = knn.kneighbors(input_features)

            # Get the genres of the current show and recommended shows
            current_genres = set(row['genres'].split())
            recommended_genres = [set(train_set.iloc[i]['genres'].split()) for i in indices.flatten()[1:k+1]]

            # Calculate relevance scores (1 if genres overlap, 0 otherwise)
            relevance = [1 if current_genres.intersection(rec_genres) else 0 for rec_genres in recommended_genres]

            # Skip if there are no positive labels
            if sum(relevance) == 0:
                continue

            # Calculate average precision for this sample
            ap = average_precision_score(relevance, [1/(i+1) for i in range(len(relevance))])
            total_ap += ap

        # Avoid division by zero
        map_at_k = total_ap / n_samples if n_samples > 0 else 0
        return map_at_k

    map_at_k = calculate_map_at_k(knn, vectorizer, scaler, train_set)

    # Metric 2: Coverage Calculation
    def calculate_coverage(knn, vectorizer, scaler, train_set, k=10):
        all_recommended_items = set()
        total_items = set(train_set.index)
        for idx, row in train_set.iterrows():
            # Combine text and numeric features
            text_features = vectorizer.transform([row['text_features']])
            count_features = scaler.transform([[row['episodeCount'], row['seasonCount']]])
            input_features = np.hstack((text_features.toarray(), count_features))

            # Get nearest neighbors
            distances, indices = knn.kneighbors(input_features)
            recommended_items = set(indices.flatten()[1:k+1])  # Exclude the item itself and limit to k items
            all_recommended_items.update(recommended_items)

        # Calculate coverage
        coverage = len(all_recommended_items) / len(total_items)
        return coverage

    coverage = calculate_coverage(knn, vectorizer, scaler, train_set)

    # Metric 3: Intra-list Similarity Calculation
    def calculate_intra_list_similarity(knn, vectorizer, scaler, train_set, k=n_neighbors):
        total_similarity = 0
        count = 0
        for idx, row in train_set.iterrows():
            # Get features of the current show
            input_features_text = vectorizer.transform([row['text_features']])
            input_features_numeric = scaler.transform(pd.DataFrame([[row['episodeCount'], row['seasonCount']]], columns=['episodeCount', 'seasonCount']))
            input_features_combined = np.hstack((input_features_text.toarray(), input_features_numeric))

            distances, indices = knn.kneighbors(input_features_combined)
            recommended_items_indices = indices.flatten()[1:k+1]  # Exclude the item itself and limit to k items

            if len(recommended_items_indices) > 1:
                # Get text features of recommended shows
                recommended_features_text = vectorizer.transform(train_set.iloc[recommended_items_indices]['text_features'])
                # Get count features of recommended shows
                recommended_features_numeric = scaler.transform(train_set.iloc[recommended_items_indices][['episodeCount', 'seasonCount']])
                # Combine text and count features
                recommended_features_combined = np.hstack((recommended_features_text.toarray(), recommended_features_numeric))

                similarity_matrix = cosine_similarity(recommended_features_combined)

                # Calculate average similarity within the list
                n_recommendations = similarity_matrix.shape[0]
                similarity_sum = (similarity_matrix.sum() - n_recommendations) / 2  # Exclude self-similarities
                avg_similarity_within_list = similarity_sum / (n_recommendations * (n_recommendations - 1) / 2)

                total_similarity += avg_similarity_within_list
                count += 1

        # Calculate intra-list similarity
        intra_list_similarity = total_similarity / count if count > 0 else 0
        return intra_list_similarity

    intra_list_similarity = calculate_intra_list_similarity(knn, vectorizer, scaler, train_set)

    ###################################################### Store Model Runs
    insert_query = f"""
    INSERT INTO {db_schema}.shows_model_runs (job_id, name, gcs_path, model_path, vectorizer_path, scaler_path)
    VALUES ('{job_id}', '{model_name}', '{GCS_BUCKET + "/" + GCS_PATH_MODEL}', '{M_GCS}', '{V_GCS}', '{S_GCS}');
    """
    print(f"mlops to warehouse: {insert_query}")
    md.sql(insert_query)

    ###################################################### Store Metrics in Database
    ingest_timestamp = pd.Timestamp.now()

    # Prepare metrics data for insertion into database
    metrics_df_data = [
        {'job_id': job_id, 'metric_name': 'MAP@K', 'metric_value': map_at_k, 'created_at': ingest_timestamp},
        {'job_id': job_id, 'metric_name': 'Coverage', 'metric_value': coverage, 'created_at': ingest_timestamp},
        {'job_id': job_id, 'metric_name': 'Intra-list Similarity', 'metric_value': intra_list_similarity, 'created_at': ingest_timestamp}
    ]
    metrics_df = pd.DataFrame(metrics_df_data)
    md.sql(f"INSERT INTO {db_schema}.shows_job_metrics SELECT * FROM metrics_df")

    ###################################################### Record Parameters in Database
    params_df_data = [
        {"job_id": job_id, "parameter_name": "k", "parameter_value": n_neighbors, "created_at": ingest_timestamp},
        {"job_id": job_id, "parameter_name": "metric", "parameter_value": metric, "created_at": ingest_timestamp}
    ]
    params_df = pd.DataFrame(params_df_data)
    md.sql(f"INSERT INTO {db_schema}.shows_job_parameters SELECT * FROM params_df")

    ###################################################### Return Response
    return {
        "job_id": job_id,
        "map_at_k": map_at_k,
        "coverage": coverage,
        "intra_list_similarity": intra_list_similarity,
        "model_path": M_GCS,
        "vectorizer_path": V_GCS,
        "scaler_path": S_GCS,
        "parameters": {
            "k": n_neighbors,
            "metric": metric
        }
    }, 200