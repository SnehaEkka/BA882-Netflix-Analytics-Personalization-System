# imports
import functions_framework
import joblib
import json
from gcsfs import GCSFileSystem
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.neighbors import NearestNeighbors

# the hardcoded model on GCS
GCS_BUCKET = "ba882-team05-vertex-models"
GCS_PATH = "models/netflix-movies/"
KNN_FNAME = "knn_model.joblib"
VECTORIZER_FNAME = "vectorizer.joblib"
MOVIE_METADATA_FNAME = "movie_metadata.json"

# GCS paths for KNN, vectorizer, and movie metadata
GCS_KNN_PATH = f"gs://{GCS_BUCKET}/{GCS_PATH}{KNN_FNAME}"
GCS_VECTORIZER_PATH = f"gs://{GCS_BUCKET}/{GCS_PATH}{VECTORIZER_FNAME}"
GCS_MOVIE_METADATA_PATH = f"gs://{GCS_BUCKET}/{GCS_PATH}{MOVIE_METADATA_FNAME}"

# Load the trained KNN model from GCS
with GCSFileSystem().open(GCS_KNN_PATH, 'rb') as f:
    knn_model = joblib.load(f)
print("Loaded the KNN model from GCS")

# Load the vectorizer from GCS
with GCSFileSystem().open(GCS_VECTORIZER_PATH, 'rb') as f:
    vectorizer = joblib.load(f)
print("Loaded the vectorizer from GCS")

# Load the movie metadata from GCS
with GCSFileSystem().open(GCS_MOVIE_METADATA_PATH, 'r') as f:
    movie_metadata = json.load(f)
print("Loaded movie metadata from GCS")

# Convert movie metadata to a dictionary for easier lookup by title
movie_metadata_dict = {movie['title']: movie for movie in movie_metadata}

# Preprocessing function for fields like genres, cast, directors
def preprocess_field(field):
    if isinstance(field, str):
        field = field.strip('[]')
        items = field.split(',')
        return ' '.join([item.split(':')[-1].strip().strip("'\"") for item in items])
    return ''

# Preprocess input data by applying similar steps used during training
def preprocess_input_data(data_list):
    """Preprocess the input data (similar to how we preprocess training data)"""
    processed_data = []

    for data in data_list:
        # Preprocess the genres, cast, and directors fields using preprocess_field
        genres = preprocess_field(data.get('genres', ''))
        cast = preprocess_field(data.get('cast', ''))
        directors = preprocess_field(data.get('directors', ''))
        overview = data.get('overview', '')

        # Combine all text features (genres, cast, directors, and overview)
        text_features = f"{genres} {cast} {directors} {overview}"

        processed_data.append(text_features)  # Append the combined text features

    return processed_data

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

    # Step 2: Transform the preprocessed data using the vectorizer
    input_features = vectorizer.transform(preprocessed_data)

    # Step 3: Get predictions using the KNN model (which will return indices and distances of the neighbors)
    distances, indices = knn_model.kneighbors(input_features)

    # Step 4: Collect recommendations for each movie
    recommendations = []
    for i, movie in enumerate(data_list):
        movie_recs = []
        for j in range(1, len(distances[i])):  # Start from 1 to exclude the movie itself
            # Get the index of the recommended movie
            movie_idx = indices[i][j]
            # Retrieve the movie title using the index from movie_metadata_dict
            recommended_movie_title = movie_metadata[movie_idx]['title']
            
            # Retrieve additional details about the recommended movie
            recommended_movie_info = movie_metadata_dict.get(recommended_movie_title, {})

            # Create the recommendation info
            movie_info = {
                'title': recommended_movie_title,
                'similarity': 1 - distances[i][j],  # Convert distance to similarity
                'distance': distances[i][j],
                'overview': recommended_movie_info.get('overview', 'N/A'),
                'genres': recommended_movie_info.get('genres', 'N/A'),
                'cast': recommended_movie_info.get('cast', 'N/A'),
                'directors': recommended_movie_info.get('directors', 'N/A')
            }
            movie_recs.append(movie_info)
        recommendations.append({movie['title']: movie_recs})

    # Return the recommendations as a JSON response
    return json.dumps({'recommendations': recommendations}), 200