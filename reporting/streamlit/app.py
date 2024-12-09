# NEW STREAMLIT SCRIPT

import streamlit as st
import duckdb
import pandas as pd
from google.cloud import secretmanager
import requests
import base64
from streamlit_lottie import st_lottie
import json
import ast
import re


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


def fetch_additional_data_from_motherduck(titles):
    if not titles:
        st.warning("No titles provided for fetching additional data.")
        return pd.DataFrame()


    def escape_title(title):
        return re.sub("'", "''", title)


    title_list_str = ", ".join([f"'{escape_title(title)}'" for title in titles])


    query = f"""
    SELECT title, "cast", overview, genres
    FROM {db_schema}.netflix_api
    WHERE title IN ({title_list_str})
    """


    try:
        result_df = md.execute(query).df()
       
        def extract_genre_names(genres_str):
            try:
                if pd.isna(genres_str):
                    return ''
                # Remove any unnecessary whitespace and quotes
                genres_str = genres_str.strip()
                # Simple string manipulation to extract names
                names = [item.split("'name': ")[1].split("'")[0]
                        for item in genres_str.split("},")
                        if "'name':" in item]
                return ', '.join(names)
            except Exception as e:
                return str(genres_str)
       
        result_df['genres'] = result_df['genres'].apply(extract_genre_names)
        result_df['genres'] = result_df['genres'].str.replace('}]', '')
        result_df['cast'] = result_df['cast'].str.replace(']', '')
        result_df['cast'] = result_df['cast'].str.replace('[', '')
        return result_df


    except Exception as e:
        st.error(f"Error executing SQL query: {e}")
        return pd.DataFrame()


# Adding background img
def load_background_image(image_file):
    with open(image_file, "rb") as f:
        data = base64.b64encode(f.read()).decode()
    return data


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


# Load and encode background image
background_image = load_background_image("netflix wallpaper.jpg")


# Updated CSS with background image
st.markdown(f"""
<style>
.stApp {{
    background-image: url("data:image/jpg;base64,{background_image}");
    background-size: cover;
    background-position: center;
    background-repeat: no-repeat;
}}
body {{
    color: white;
}}
.stSelectbox {{
    color: white;
}}
.stButton>button {{
    color: white;
    background-color: #E50914;
    border-color: #E50914;
}}
.stButton>button:hover {{
    color: white;
    background-color: #B20710;
    border-color: #B20710;
}}
/* Add transparency to the content area */
.css-1d391kg {{
    background-color: rgba(0, 0, 0, 0.05);


/* Center align the dataframe */
[data-testid="stDataFrame"] {{
    width: 80% !important;
    margin: auto;
}}


/* Reduce width of the main container */
.block-container {{
    max-width: 1000px;
    padding-top: 1rem;
    padding-bottom: 1rem;
    margin: auto;
}}
</style>
""", unsafe_allow_html=True)


# Set page config and add Netflix logo
# st.set_page_config(page_title="Movie Recommendation System", layout="wide")
st.markdown("""<img src="https://upload.wikimedia.org/wikipedia/commons/0/08/Netflix_2015_logo.svg" width="100">""", unsafe_allow_html=True)


# Title and introduction
st.write("""<h1 style="color:white;">Netflix Recommendation System</h1>""", unsafe_allow_html=True)
st.write("""<p style="color:white;">Discover new movies and shows based on your interests.</p>""", unsafe_allow_html=True)


# Load Lottie animation
lottie_coding = load_lottiefile("netflix-logo.json")
col1, col2, col3 = st.columns([1,2,1])
with col2:
    st_lottie(lottie_coding, speed=1, reverse=False, loop=True, quality="low", height=150)


# Create tabs for Movies and TV Shows
tab1, tab2 = st.tabs(["Movies 🎬", "TV Shows 📺"])


# Movie selection with tab1
with tab1:
    movie_expander = st.expander("Select or Search for a Movie")
    search_movie = movie_expander.text_input("Search for a movie", key="movie_search")
    movies_list = get_movies()
    
    # Filter movies to only show titles starting with the entered search characters
    if search_movie:
        movies_list = [movie for movie in movies_list if movie.lower().startswith(search_movie.lower())]
    
    selected_movie = movie_expander.selectbox("Or select from the list", movies_list, key="movie_select")
    
    if movie_expander.button("Get Movie Recommendations"):
        if selected_movie:  # Ensure a movie is selected
            movie_recommendations_df = recommend(selected_movie, content_type='movie')
            if movie_recommendations_df is not None:
                st.markdown("<h3 style='text-align: center;'>Top Recommendations</h3>", unsafe_allow_html=True)
                st.dataframe(
                    movie_recommendations_df[['title', "cast", 'overview', 'genres']],
                    hide_index=True,
                    use_container_width=True
                )
        else:
            st.warning("Please select a movie before getting recommendations.")

# TV Show selection with tab2
with tab2:
    show_expander = st.expander("Select or Search for a TV Show")
    search_show = show_expander.text_input("Search for a TV show", key="show_search")
    shows_list = get_tv_shows()
    
    # Filter shows to only show titles starting with the entered search characters
    if search_show:
        shows_list = [show for show in shows_list if show.lower().startswith(search_show.lower())]
    
    selected_show = show_expander.selectbox("Or select from the list", shows_list, key="show_select")
    
    if show_expander.button("Get TV Show Recommendations"):
        if selected_show:  # Ensure a show is selected
            show_recommendations_df = recommend(selected_show, content_type='show')
            if show_recommendations_df is not None:
                st.markdown("<h3 style='text-align: center;'>Top Recommended TV Shows</h3>", unsafe_allow_html=True)
                st.dataframe(
                    show_recommendations_df[['title', "cast", 'overview', 'genres']],
                    hide_index=True,
                    use_container_width=True
                )
        else:
            st.warning("Please select a TV show before getting recommendations.")
