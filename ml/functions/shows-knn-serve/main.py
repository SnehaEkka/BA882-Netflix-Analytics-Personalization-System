import functions_framework
import os
import pandas as pd
import numpy as np
import joblib
import json
import io
from flask import Response
from gcsfs import GCSFileSystem
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import NearestNeighbors
import warnings
warnings.filterwarnings("ignore", message="X does not have valid feature names")

from google.cloud import storage
from google.cloud import aiplatform

# GCS paths for KNN, vectorizer, and movie metadata
GCS_BUCKET = "ba882-team05-vertex-models"
GCS_PATH = "models/netflix-shows/"
KNN_FNAME = "knn_model.joblib"
VECTORIZER_FNAME = "vectorizer.joblib"
SCALER_FNAME = "scaler.joblib"
SHOW_METADATA_FNAME = "show_metadata.json"

GCS_KNN_PATH = f"gs://{GCS_BUCKET}/{GCS_PATH}{KNN_FNAME}"
GCS_VECTORIZER_PATH = f"gs://{GCS_BUCKET}/{GCS_PATH}{VECTORIZER_FNAME}"
GCS_SCALER_PATH = f"gs://{GCS_BUCKET}/{GCS_PATH}{SCALER_FNAME}"
GCS_SHOW_METADATA_PATH = f"gs://{GCS_BUCKET}/{GCS_PATH}{SHOW_METADATA_FNAME}"

# Load the trained KNN model from GCS
with GCSFileSystem().open(GCS_KNN_PATH, 'rb') as f:
    knn_model = joblib.load(f)
print("Loaded the KNN model from GCS")

# Load the vectorizer from GCS
with GCSFileSystem().open(GCS_VECTORIZER_PATH, 'rb') as f:
    vectorizer = joblib.load(f)
print("Loaded the vectorizer from GCS")

# Load the scaler from GCS
with GCSFileSystem().open(GCS_SCALER_PATH, 'rb') as f:
    scaler = joblib.load(f)
print("Loaded the scaler from GCS")

# Load the show metadata from GCS
with GCSFileSystem().open(GCS_SHOW_METADATA_PATH, 'r') as f:
    show_metadata = json.load(f)
print("Loaded movie metadata from GCS")

# Convert movie metadata to a dictionary for easier lookup by title
show_metadata_dict = {show['title']: show for show in show_metadata}

# Preprocessing function for fields like genres, cast, directors
def preprocess_field(field):
    if isinstance(field, str):
        field = field.strip('[]')
        items = field.split(',')
        return ' '.join([item.split(':')[-1].strip().strip("'\"") for item in items])
    return ''

# Preprocess the input data (similar to how we preprocess training data)
def preprocess_input_data(data_list):
    """Preprocess the input data (similar to how we preprocess training data)"""
    processed_data = []
    episode_count_data = []

    for data in data_list:
        # Preprocess the genres, cast, and directors fields using preprocess_field
        genres = preprocess_field(data.get('genres', ''))
        cast = preprocess_field(data.get('cast', ''))
        directors = preprocess_field(data.get('directors', ''))
        overview = data.get('overview', '')
        
        # Combine text features for the TF-IDF
        text_features = f"{genres} {cast} {directors} {overview}"

        # Add the text features to the processed data list
        processed_data.append(text_features)

        # Convert episodeCount and seasonCount to numeric type
        episodeCount = pd.to_numeric(data.get('episodeCount', ''), errors='coerce')
        seasonCount = pd.to_numeric(data.get('seasonCount', ''), errors='coerce')

        episode_count_data.append([episodeCount, seasonCount])

    # Step 1: Apply TF-IDF vectorizer on the processed text data
    tfidf_matrix = vectorizer.transform(processed_data)

    # Step 2: Scale the episodeCount and seasonCount using the scaler
    scaled_count_features = scaler.transform(episode_count_data)

    # Step 3: Combine TF-IDF matrix and scaled numeric features
    features = np.hstack((tfidf_matrix.toarray(), scaled_count_features))

    return features

@functions_framework.http
def main(request):
    """Make predictions for the model based on the input data"""

    # Parse the request data (assumes JSON format)
    request_json = request.get_json(silent=True)
    
    if not request_json or 'data' not in request_json:
        return {'error': 'No input data provided'}, 400

    # Load the data (a list of movie data dictionaries with 'genres', 'cast', 'directors', 'overview')
    data_list = request_json.get('data')
    print(f"Received data: {data_list}")

    # Step 1: Preprocess input data (just like we did for training data)
    preprocessed_data = preprocess_input_data(data_list)

    # Step 3: Get predictions using the KNN model (which will return indices and distances of the neighbors)
    distances, indices = knn_model.kneighbors(preprocessed_data)

    # Step 4: Collect recommendations for each show
    recommendations = []
    for i, show in enumerate(data_list):
        show_recs = []
        for j in range(1, len(distances[i])):  # Start from 1 to exclude the show itself
            # Get the index of the recommended show
            show_idx = indices[i][j]
            # Retrieve the show title using the index from show_metadata_dict
            recommended_show_title = show_metadata[show_idx]['title']
            
            # Retrieve additional details about the recommended show
            recommended_show_info = show_metadata_dict.get(recommended_show_title, {})

            # Create the recommendation info
            show_info = {
                'title': recommended_show_title,
                'similarity': 1 - distances[i][j],  # Convert distance to similarity
                'distance': distances[i][j],
                'overview': recommended_show_info.get('overview', 'N/A'),
                'genres': recommended_show_info.get('genres', 'N/A'),
                'cast': recommended_show_info.get('cast', 'N/A'),
                'episodeCount': recommended_show_info.get('episodeCount', 'N/A'),
                'seasonCount': recommended_show_info.get('seasonCount', 'N/A')
            }
            show_recs.append(show_info)
        recommendations.append({show['title']: show_recs})

    # Return the recommendations as a JSON response
    return json.dumps({'recommendations': recommendations}), 200