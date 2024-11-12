##################################################### imports

import functions_framework
import os
import pandas as pd
import numpy as np
import joblib
import json
from gcsfs import GCSFileSystem
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
import warnings
warnings.filterwarnings("ignore", message="X does not have valid feature names")

from google.cloud import storage
from google.cloud import aiplatform

##################################################### settings

# settings
project_id = 'ba882-inclass-project'  # <------- change this to your value
project_region = 'us-central1'  #

##################################################### task

@functions_framework.http
def main(request):
    "Fit the model using a cloud function"

    # we are hardcoding the dataset
    GCS_PATH = "gs://ba882-team05-vertex-models/training-data/netflix-api-recommendation/netflix_data.csv"

    # get the dataset
    df = pd.read_csv(GCS_PATH)
    print(df.head())

    # Shows
    df_show = df[df["showType"] == "series"]

    # Extract relevant columns
    columns_of_interest = ['genres', 'cast', 'directors', 'overview', 'title', 'episodeCount', 'seasonCount']
    df_show = df_show[columns_of_interest]

    def preprocess_field(field):
        if isinstance(field, str):
            field = field.strip('[]')
            items = field.split(',')
            return ' '.join([item.split(':')[-1].strip().strip("'\"") for item in items])
        return ''

    # Preprocess the genres, cast, and directors
    df_show['genres'] = df_show['genres'].apply(preprocess_field)
    df_show['cast'] = df_show['cast'].apply(preprocess_field)
    df_show['directors'] = df_show['directors'].apply(preprocess_field)

    # Combine text features
    df_show['text_features'] = df_show['genres'] + ' ' + df_show['cast'] + ' ' + df_show['directors'] + ' ' + df_show['overview']

    # Remove rows with empty text features, title, episodeCount, or seasonCount
    df_show = df_show.dropna(subset=['text_features', 'title', 'episodeCount', 'seasonCount'])

    # Convert episodeCount and seasonCount to numeric type
    df_show['episodeCount'] = pd.to_numeric(df_show['episodeCount'], errors='coerce')
    df_show['seasonCount'] = pd.to_numeric(df_show['seasonCount'], errors='coerce')

    # Split the dataset into training (70%) and testing (30%) sets
    train_set, test_set = train_test_split(df_show, test_size=0.3, random_state=42)

    # Create TF-IDF vectorizer for text features
    vectorizer = TfidfVectorizer(stop_words='english')
    tfidf_matrix_train = vectorizer.fit_transform(train_set['text_features'])

    # Scale episodeCount and seasonCount
    scaler = StandardScaler()
    count_features_train = scaler.fit_transform(train_set[['episodeCount', 'seasonCount']])
    
    # Combine TF-IDF matrix with scaled count features
    features_train = np.hstack((tfidf_matrix_train.toarray(), count_features_train))

    # Implement KNN model
    k = 10  # number of neighbors
    knn = NearestNeighbors(n_neighbors=k, metric='cosine')
    knn.fit(features_train)

    # Save the KNN model and vectorizer to GCS
    GCS_BUCKET = "ba882-team05-vertex-models"
    GCS_PATH = "models/netflix-shows/"

    # Save KNN model
    knn_model_fname = "knn_model.joblib"
    knn_model_gcs_path = f"gs://{GCS_BUCKET}/{GCS_PATH}{knn_model_fname}"
    with GCSFileSystem().open(knn_model_gcs_path, 'wb') as f:
        joblib.dump(knn, f)  # Save the KNN model

    # Save Vectorizer model
    vectorizer_fname = "vectorizer.joblib"
    vectorizer_gcs_path = f"gs://{GCS_BUCKET}/{GCS_PATH}{vectorizer_fname}"
    with GCSFileSystem().open(vectorizer_gcs_path, 'wb') as f:
        joblib.dump(vectorizer, f)  # Save the vectorizer

    # Save Scaler model
    scaler_fname = "scaler.joblib"
    scaler_gcs_path = f"gs://{GCS_BUCKET}/{GCS_PATH}{scaler_fname}"
    with GCSFileSystem().open(scaler_gcs_path, 'wb') as f:
        joblib.dump(scaler, f)  # Save the scaler

    # Save the complete show metadata
    show_metadata = df_show[['genres', 'cast', 'directors', 'overview', 'title', 'episodeCount', 'seasonCount']].to_dict(orient='records')
    
    # Save as a JSON file in GCS
    show_metadata_fname = "show_metadata.json"
    show_metadata_gcs_path = f"gs://{GCS_BUCKET}/{GCS_PATH}{show_metadata_fname}"
    
    with GCSFileSystem().open(show_metadata_gcs_path, 'w') as f:
        json.dump(show_metadata, f)

    return 'KNN model, vectorizer, scaler, and show metadata saved successfully to GCS', 200