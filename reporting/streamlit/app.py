# NEW STREAMLIT SCRIPT - movie and show lists (latest working)

import streamlit as st
import duckdb
import pandas as pd
from google.cloud import secretmanager
import numpy as np
from itertools import combinations
import networkx as nx
from collections import Counter
import matplotlib.pyplot as plt
import base64
import requests

# Secret manager and database connection setup (keep as is)
project_id = 'ba882-inclass-project'
secret_id = 'duckdb-token'
version_id = 'latest'
db = 'ba882_project'
schema = "stage"
db_schema = f"{db}.{schema}"

# Secret manager and database connection setup (keep as is)														   
sm = secretmanager.SecretManagerServiceClient()
name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
response = sm.access_secret_version(request={"name": name})
md_token = response.payload.data.decode("UTF-8")
md = duckdb.connect(f'md:?motherduck_token={md_token}')

# Define separate URLs for movies and TV shows Cloud Run functions
MOVIE_CLOUD_RUN_URL = "https://us-central1-ba882-inclass-project.cloudfunctions.net/ml-movies-serve"
TV_SHOW_CLOUD_RUN_URL = "https://us-central1-ba882-inclass-project.cloudfunctions.net/ml-shows-serve"																								 
# Fetch movies from DuckDB
def get_movies():
    query = "SELECT title FROM ba882_project.stage.netflix_api where showType = 'movie'"
    movies_df = md.execute(query).df()
    return movies_df['title'].tolist()

# Fetch TV shows from DuckDB
def get_tv_shows():
    query = "SELECT title FROM ba882_project.stage.netflix_api where showType = 'series'"
    shows_df = md.execute(query).df()
    return shows_df['title'].tolist()

# Function to fetch additional data (poster, releaseYear, genres) from Motherduck
def fetch_additional_data_from_motherduck(titles):
    # Convert list of titles into a format suitable for SQL query
    title_list_str = ", ".join([f"'{title}'" for title in titles])
    
    # Query Motherduck to fetch metadata for these titles
    query = f"""
    SELECT title, poster_url, releaseYear, genres 
    FROM {db_schema}.netflix_api 
    WHERE title IN ({title_list_str})
    """
    
    # Execute query and return results as a DataFrame
    result_df = md.execute(query).df()
    
    return result_df
# Recommendation function that calls the appropriate Cloud Run service and fetches metadata from Motherduck
def recommend(title, content_type='movie'):
    # Determine which Cloud Run service to call based on content type
    if content_type == 'movie':
        cloud_run_url = MOVIE_CLOUD_RUN_URL
    else:
        cloud_run_url = TV_SHOW_CLOUD_RUN_URL
    
    # Prepare data payload (you might need to adjust this based on your API's expected input)
    payload = {
        "title": title,
        "content_type": content_type  # Either 'movie' or 'show'
    }
    
    try:
        # Make a POST request to the appropriate Cloud Run service (no authentication needed)
        response = requests.post(cloud_run_url, json=payload)
        
        # Check if the request was successful
        response.raise_for_status()
        
        # Parse and return recommendations from the response (assuming it's a list of titles)
        recommended_titles = response.json()  # Assuming the API returns a list of recommended titles
        
        # Fetch additional metadata from Motherduck for these recommended titles
        metadata_df = fetch_additional_data_from_motherduck(recommended_titles)
        
        return metadata_df
    
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching recommendations: {e}")
        return None
# Function to add custom CSS
def add_bg_from_local(image_file):
    with open(image_file, "rb") as image_file:
        encoded_string = base64.b64encode(image_file.read())
    st.markdown(
    f"""
    <style>
    .stApp {{
        background-image: url(data:image/{"png"};base64,{encoded_string.decode()});
        background-size: cover;
    }}
    </style>
    """,
    unsafe_allow_html=True
    )

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

# Add Netflix logo
st.markdown("""
    <img src="https://upload.wikimedia.org/wikipedia/commons/0/08/Netflix_2015_logo.svg" width="100">
    """, unsafe_allow_html=True)

# Title and introduction
st.write(""" <h1 style="color:white;">Netflix Recommendation System</h1>""", unsafe_allow_html=True)
st.write(""" <p style="color:white;">Discover new movies and shows based on your interests.</p>""", unsafe_allow_html=True)
st.write("##")

# Create tabs for Movies and Shows
tab1, tab2 = st.tabs(["Movies 🎬", "TV Shows 📺"])

# Movie selection with tab1
with tab1:
    movie_expander = st.expander("Select a Movie")
    movies_list = get_movies()  # Fetch movies dynamically
    selected_movie = movie_expander.selectbox("", movies_list, key="movie_select")
    
    if movie_expander.button("Get Movie Recommendations"):
        st.text("Top 5 movie recommendations")
        st.write("#")
        # Call your recommendation function for movies
        movie_recommendations = recommend(selected_movie)
        
        # Display movie recommendations
        cols = st.columns(5)
        for i, rec in enumerate(movie_recommendations[:5]):
            with cols[i]:
                st.write(f'<p style="color:white;"><b>{rec["title"]}</b></p>', unsafe_allow_html=True)
                st.image(rec["poster"])
                st.write("________")
                st.write(f'<p style="color:#E50914;">Rating: <b>{rec["rating"]}</b></p>', unsafe_allow_html=True)
                st.write(f'<p style="color:#E50914;">Votes: <b>{rec["votes"]}</b></p>', unsafe_allow_html=True)

# TV Show selection with tab2
with tab2:
    show_expander = st.expander("Select a TV Show")
    shows_list = get_tv_shows()  # Fetch TV shows dynamically
    selected_show = show_expander.selectbox("", shows_list, key="show_select")
    
    if show_expander.button("Get TV Show Recommendations"):
        st.text("Top 5 TV show recommendations")
        st.write("#")
        # Call your recommendation function for TV shows
        show_recommendations = recommend(selected_show)
        
        # Display TV show recommendations
        cols = st.columns(5)
        for i, rec in enumerate(show_recommendations[:5]):
            with cols[i]:
                st.write(f'<p style="color:white;"><b>{rec["title"]}</b></p>', unsafe_allow_html=True)
                st.image(rec["poster"])
                st.write("________")
                st.write(f'<p style="color:#E50914;">Rating: <b>{rec["rating"]}</b></p>', unsafe_allow_html=True)
                st.write(f'<p style="color:#E50914;">Votes: <b>{rec["votes"]}</b></p>', unsafe_allow_html=True)

# Recommendation function (you'll need to implement this)
def recommend(title):
    # Your recommendation logic here
    # Return list of recommended movies/shows, their posters, and any additional info
    # For now, we'll return a dummy list
    return [
        {"title": f"Recommendation {i}", "poster": "https://via.placeholder.com/150", "rating": "8.5", "votes": "10000"}
        for i in range(1, 6)
    ]

# Additional features (you can keep or modify these as needed)
st.markdown("---")

# Data visualization (modify as needed)
st.markdown("<h3 style='color:white;'>Data Visualization</h3>", unsafe_allow_html=True)
# Your existing data visualization code here