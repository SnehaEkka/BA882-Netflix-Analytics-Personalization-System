# import streamlit as st
# import duckdb
# import pandas as pd
# from google.cloud import secretmanager
# import numpy as np
# from itertools import combinations
# import networkx as nx
# from collections import Counter
# import matplotlib.pyplot as plt
# import base64

# # Secret manager and database connection setup (keep as is)
# # Secret manager and database connection setup (keep as is)
# project_id = 'ba882-inclass-project'
# secret_id = 'duckdb-token'
# version_id = 'latest'
# db = 'awsblogs'
# schema = "stage"
# db_schema = f"{db}.{schema}"

# sm = secretmanager.SecretManagerServiceClient()
# name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
# response = sm.access_secret_version(request={"name": name})
# md_token = response.payload.data.decode("UTF-8")
# md = duckdb.connect(f'md:?motherduck_token={md_token}')

# # Function to add custom CSS
# def add_bg_from_local(image_file):
#     with open(image_file, "rb") as image_file:
#         encoded_string = base64.b64encode(image_file.read())
#     st.markdown(
#     f"""
#     <style>
#     .stApp {{
#         background-image: url(data:image/{"png"};base64,{encoded_string.decode()});
#         background-size: cover;
#     }}
#     </style>
#     """,
#     unsafe_allow_html=True
#     )

# # Set page config
# st.set_page_config(page_title="Netflix Recommendation System", layout="wide")

# # Add custom CSS for Netflix-style theming
# st.markdown("""
#     <style>
#     body {
#         color: white;
#         background-color: black;
#     }
#     .stApp {
#         background-color: black;
#     }
#     .stSelectbox {
#         color: white;
#     }
#     .stButton>button {
#         color: white;
#         background-color: #E50914;
#         border-color: #E50914;
#     }
#     .stButton>button:hover {
#         color: white;
#         background-color: #B20710;
#         border-color: #B20710;
#     }
#     </style>
#     """, unsafe_allow_html=True)

# # Add Netflix logo
# st.markdown("""
#     <img src="https://upload.wikimedia.org/wikipedia/commons/0/08/Netflix_2015_logo.svg" width="100">
#     """, unsafe_allow_html=True)

# # Title and introduction
# st.write(""" <h1 style="color:white;">Netflix Recommendation System</h1>""", unsafe_allow_html=True)
# st.write(""" <p style="color:white;">Discover new movies and shows based on your interests.</p>""", unsafe_allow_html=True)
# st.write("##")

# # Movie selection
# my_expander = st.expander("Select a Movie or Show 🎬")
# # Replace this with your actual list of movies/shows
# movies_list = ["Movie 1", "Movie 2", "Movie 3"]  # You'll need to fetch this from your database
# selected_movie = my_expander.selectbox("", movies_list)

# # Recommendation function (you'll need to implement this)
# def recommend(movie_name):
#     # Your recommendation logic here
#     # Return list of recommended movies, their posters, and any additional info
#     pass

# if my_expander.button("Get Recommendations"):
#     st.text("Here are your recommendations:")
#     st.write("#")
#     recommendations = recommend(selected_movie)
    
#     # Display recommendations
#     cols = st.columns(5)
#     for i, rec in enumerate(recommendations[:5]):
#         with cols[i]:
#             st.write(f'<p style="color:white;"><b>{rec["title"]}</b></p>', unsafe_allow_html=True)
#             st.image(rec["poster"])
#             st.write("________")
#             st.write(f'<p style="color:#E50914;">Rating: <b>{rec["rating"]}</b></p>', unsafe_allow_html=True)
#             st.write(f'<p style="color:#E50914;">Votes: <b>{rec["votes"]}</b></p>', unsafe_allow_html=True)

# # Additional features (you can keep or modify these as needed)
# st.markdown("---")

# # Data visualization (modify as needed)
# st.markdown("<h3 style='color:white;'>Data Visualization</h3>", unsafe_allow_html=True)
# # Your existing data visualization code here

# # Add background image (optional)
# # add_bg_from_local('path_to_your_background_image.png')  # Uncomment and provide path to use a background image

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

# Secret manager and database connection setup (keep as is)
project_id = 'ba882-inclass-project'
secret_id = 'duckdb-token'
version_id = 'latest'
db = 'awsblogs'
schema = "stage"
db_schema = f"{db}.{schema}"

sm = secretmanager.SecretManagerServiceClient()
name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
response = sm.access_secret_version(request={"name": name})
md_token = response.payload.data.decode("UTF-8")
md = duckdb.connect(f'md:?motherduck_token={md_token}')

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

# Movie selection
with tab1:
    movie_expander = st.expander("Select a Movie")
    # Replace this with your actual list of movies
    movies_list = ["Movie 1", "Movie 2", "Movie 3"]  # You'll need to fetch this from your database
    selected_movie = movie_expander.selectbox("", movies_list, key="movie_select")
    
    if movie_expander.button("Get Movie Recommendations"):
        st.text("Here are your movie recommendations:")
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

# TV Show selection
with tab2:
    show_expander = st.expander("Select a TV Show")
    # Replace this with your actual list of TV shows
    shows_list = ["Show 1", "Show 2", "Show 3"]  # You'll need to fetch this from your database
    selected_show = show_expander.selectbox("", shows_list, key="show_select")
    
    if show_expander.button("Get TV Show Recommendations"):
        st.text("Here are your TV show recommendations:")
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
st.markdown("<h3 style='color:white;'>Top 5 Recommendations</h3>", unsafe_allow_html=True)
# Your existing data visualization code here