# imports
import functions_framework
import os
import pandas as pd
import numpy as np
import joblib
import json

from gcsfs import GCSFileSystem
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors
from sklearn.model_selection import train_test_split
from google.cloud import storage
from google.cloud import aiplatform

# settings
project_id = 'ba882-inclass-project'  # <------- change this to your value
project_region = 'us-central1' #

@functions_framework.http
def main(request):
    """Fit the model and save it to GCS"""

    # we are hardcoding the dataset
    GCS_PATH = "gs://ba882-team05-vertex-models/training-data/netflix-api-recommendation/netflix_data.csv"

    # Load the dataset
    df = pd.read_csv(GCS_PATH)
    print(df.head())

    # Movies
    df_movie = df[df["showType"] == "movie"]

    # Extract relevant columns
    columns_of_interest = ['genres', 'cast', 'directors', 'overview', 'title']
    df_movie = df_movie[columns_of_interest]

    # Preprocessing function
    def preprocess_field(field):
        if isinstance(field, str):
            field = field.strip('[]')
            items = field.split(',')
            return ' '.join([item.split(':')[-1].strip().strip("'\"") for item in items])
        return ''

    # Preprocess the genres, cast, and directors
    df_movie['genres'] = df_movie['genres'].apply(preprocess_field)
    df_movie['cast'] = df_movie['cast'].apply(preprocess_field)
    df_movie['directors'] = df_movie['directors'].apply(preprocess_field)

    # Combine text features
    df_movie['text_features'] = df_movie['genres'] + ' ' + df_movie['cast'] + ' ' + df_movie['directors'] + ' ' + df_movie['overview']

    # Remove rows with empty text features and title
    df_movie = df_movie.dropna(subset=['text_features', 'title'])

    # Split the dataset into training (70%) and testing (30%) sets
    train_set, test_set = train_test_split(df_movie, test_size=0.3, random_state=42)

    # Create TF-IDF vectorizer for text features
    vectorizer = TfidfVectorizer(stop_words='english')
    tfidf_matrix_train = vectorizer.fit_transform(train_set['text_features'])

    # Implement KNN model
    k = 10  # number of neighbors
    knn = NearestNeighbors(n_neighbors=k, metric='cosine')
    knn.fit(tfidf_matrix_train)

    # Save the KNN model and vectorizer to GCS
    GCS_BUCKET = "ba882-team05-vertex-models"
    GCS_PATH = "models/netflix-movies/"
    
    # Save KNN Model
    knn_model_fname = "knn_model.joblib"
    knn_model_gcs_path = f"gs://{GCS_BUCKET}/{GCS_PATH}{knn_model_fname}"
    with GCSFileSystem().open(knn_model_gcs_path, 'wb') as f:
        joblib.dump(knn, f)  # Save the KNN model

    # Save Vectorizer Model
    vectorizer_fname = "vectorizer.joblib"
    vectorizer_gcs_path = f"gs://{GCS_BUCKET}/{GCS_PATH}{vectorizer_fname}"
    with GCSFileSystem().open(vectorizer_gcs_path, 'wb') as f:
        joblib.dump(vectorizer, f)  # Save the vectorizer

    # Save the complete movie metadata (including 'genres', 'cast', 'directors', 'overview', 'title')
    movie_metadata = df_movie[['genres', 'cast', 'directors', 'overview', 'title']].to_dict(orient='records')
    
    # Save as a JSON file in GCS
    movie_metadata_fname = "movie_metadata.json"
    movie_metadata_gcs_path = f"gs://{GCS_BUCKET}/{GCS_PATH}{movie_metadata_fname}"
    
    with GCSFileSystem().open(movie_metadata_gcs_path, 'w') as f:
        json.dump(movie_metadata, f)

    return 'KNN model, vectorizer, and movie metadata saved successfully to GCS', 200