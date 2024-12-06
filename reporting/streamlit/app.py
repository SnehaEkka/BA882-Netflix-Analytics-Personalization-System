# NEW STREAMLIT SCRIPT

import streamlit as st
import duckdb
import pandas as pd
from google.cloud import secretmanager
import requests
import base64
from streamlit_lottie import st_lottie
import json

# Secret manager and database connection setup
project_id = 'ba882-inclass-project'
secret_id = 'duckdb-token'
version_id = 'latest'
db = 'ba882_project'
schema = "stage"
db_schema = f"{db}.{schema}"

# Secret manager setup
sm = secretmanager.SecretManagerServiceClient()
name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
response = sm.access_secret_version(request={"name": name})
md_token = response.payload.data.decode("UTF-8")
md = duckdb.connect(f'md:?motherduck_token={md_token}')

# Define Cloud Run URLs
MOVIE_CLOUD_RUN_URL = "https://us-central1-ba882-inclass-project.cloudfunctions.net/ml-movies-serve"
TV_SHOW_CLOUD_RUN_URL = "https://us-central1-ba882-inclass-project.cloudfunctions.net/ml-shows-serve"

# Load Lottie animation
def load_lottiefile(filepath: str):
    with open(filepath, "r") as f:
        return json.load(f)

# Fetch movies from DuckDB
def get_movies():
    query = "SELECT title FROM ba882_project.stage.netflix_api WHERE showType = 'movie'"
    movies_df = md.execute(query).df()
    return movies_df['title'].tolist()

# Fetch TV shows from DuckDB
def get_tv_shows():
    query = "SELECT title FROM ba882_project.stage.netflix_api WHERE showType = 'series'"
    shows_df = md.execute(query).df()
    return shows_df['title'].tolist()

# Fetch additional data (poster, releaseYear, genres) from Motherduck
def fetch_additional_data_from_motherduck(titles):
	# Check if titles list is empty							   
    if not titles:
        st.warning("No titles provided for fetching additional data.")
        return pd.DataFrame()

	# Convert list of titles into a format suitable for SQL query															 
    title_list_str = ", ".join([f"'{title}'" for title in titles])

    # Query Motherduck to fetch metadata for these titles
    query = f"""
    SELECT title, releaseYear, genres
    FROM {db_schema}.netflix_api 
    WHERE title IN ({title_list_str})
    """

    try:
        # Execute query and return results as a DataFrame
        result_df = md.execute(query).df()
        # st.write("Query Result DataFrame:", result_df.head())
        return result_df

    except Exception as e:
        st.error(f"Error executing SQL query: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of error

# Recommendation function
def recommend(title, content_type):
	# Determine which Cloud Run service to call based on content type
    if content_type == 'movie':
        cloud_run_url = MOVIE_CLOUD_RUN_URL
    else:
        cloud_run_url = TV_SHOW_CLOUD_RUN_URL										 

    payload = {
        "data": [
            {"title": title}
        ]
    }

    try:
		# Make a POST request to the appropriate Cloud Run service (no authentication needed)
        response = requests.post(cloud_run_url, json=payload)
        
        # Check if the request was successful
        response.raise_for_status()
        
        # Parse the JSON response
        recommendations = response.json()

        # Extract the list of recommendations for the given title
        recommended_items = recommendations["recommendations"][0][title]

        # Extract only the top 10 titles from the recommended items
        top_10_titles = [item["title"] for item in recommended_items]
		
        # Fetch additional metadata from Motherduck for these recommended titles																		
        metadata_df = fetch_additional_data_from_motherduck(top_10_titles)
		
        # Check if metadata_df is empty
        if metadata_df.empty:
            st.warning("No recommendations found.")
            return None

        # print("Metadata DataFrame:", metadata_df.head())

        
        return metadata_df

    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching recommendations: {e}")
        return None
        
    except KeyError as e:
        st.error(f"Unexpected response format: {e}")
        return None 

# Set page config
st.set_page_config(page_title="Netflix Recommendation System", layout="wide")

# Add custom CSS for Netflix-style theming
st.markdown("""
<style>
body {
    color: white;
    background-color: black;
}
.stApp {
    background-color: black;
}
.stSelectbox {
    color: white;
}
.stButton>button {
    color: white;
    background-color: #E50914;
    border-color: #E50914;
}
.stButton>button:hover {
    color: white;
    background-color: #B20710;
    border-color: #B20710;
}
</style>
""", unsafe_allow_html=True)

# Set page config and add Netflix logo
# st.set_page_config(page_title="Movie Recommendation System", layout="wide")
st.markdown("""<img src="https://upload.wikimedia.org/wikipedia/commons/0/08/Netflix_2015_logo.svg" width="100">""", unsafe_allow_html=True)

# Title and introduction
st.write("""<h1 style="color:white;">Netflix Recommendation System</h1>""", unsafe_allow_html=True)
st.write("""<p style="color:white;">Discover new movies and shows based on your interests.</p>""", unsafe_allow_html=True)

# Load Lottie animation (optional)
lottie_coding = load_lottiefile("netflix-logo.json")
st_lottie(lottie_coding, speed=1, reverse=False, loop=True, quality="low", height=220)

# Create tabs for Movies and TV Shows
tab1, tab2 = st.tabs(["Movies 🎬", "TV Shows 📺"])

# Movie selection with tab1
with tab1:
    movie_expander = st.expander("Select a Movie")
    movies_list = get_movies()
    selected_movie = movie_expander.selectbox("", movies_list, key="movie_select")
    
    if movie_expander.button("Get Movie Recommendations"):
        movie_recommendations_df = recommend(selected_movie, content_type='movie')
        
        if movie_recommendations_df is not None:
            st.subheader("Top 5 Recommendations")
            st.dataframe(movie_recommendations_df[['title', 'releaseYear', 'genres']])

# TV Show selection with tab2
with tab2:
    show_expander = st.expander("Select a TV Show")
    shows_list = get_tv_shows()
    selected_show = show_expander.selectbox("", shows_list, key="show_select")
    
    if show_expander.button("Get TV Show Recommendations"):
        show_recommendations_df = recommend(selected_show, content_type='show')
        
        if show_recommendations_df is not None:
            st.subheader("Top 5 Recommended TV Shows")
            st.dataframe(show_recommendations_df[['title', 'releaseYear', 'genres']])