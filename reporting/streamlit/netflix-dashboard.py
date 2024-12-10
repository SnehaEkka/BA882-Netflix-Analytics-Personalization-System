import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import base64
from streamlit_lottie import st_lottie
import json
from google.cloud import secretmanager

# Secret manager and database connection setup
project_id = 'ba882-inclass-project'
region_id = "us-central1"
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
conn = duckdb.connect(f'md:?motherduck_token={md_token}')

# Load Lottie animation
def load_lottiefile(filepath: str):
    with open(filepath, "r") as f:
        return json.load(f)

# Background image loading
def load_background_image(image_file):
    with open(image_file, "rb") as f:
        data = base64.b64encode(f.read()).decode()
    return data

# Fetch movies from DuckDB
def get_titles():
    query = "SELECT DISTINCT title FROM ba882_project.stage.netflix_api"
    movies_df = conn.execute(query).df()
    return movies_df['title'].tolist()

# # Set page config
# st.set_page_config(page_title="Netflix Analytics Dashboard", layout="wide")

# st.title("Netflix Analytics Dashboard")

# Load and encode background image
background_image = load_background_image("netflix wallpaper.jpg")

# Apply Netflix styling
st.markdown(f"""
<style>
.stApp {{
    background-image: url("data:image/jpg;base64,{background_image}");
    background-size: cover;
    background-position: center;
    background-repeat: no-repeat;
}}
body {{ color: white; }}
.stSelectbox {{ color: white; }}
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
.css-1d391kg {{ background-color: rgba(0, 0, 0, 0.05); }}
[data-testid="stDataFrame"] {{
    width: 80% !important;
    margin: auto;
}}
.block-container {{
    max-width: 1200px;
    padding-top: 1rem;
    padding-bottom: 1rem;
    margin: auto;
}}
</style>
""", unsafe_allow_html=True)

# Netflix logo and title
st.image("https://upload.wikimedia.org/wikipedia/commons/0/08/Netflix_2015_logo.svg", width=100)
st.write("""<h1 style="color:white;">Netflix Analytics Dashboard</h1>""", unsafe_allow_html=True)

# Load Lottie animation
lottie_coding = load_lottiefile("netflix-logo.json")
col1, col2, col3 = st.columns([1,2,1])
with col2:
    st_lottie(lottie_coding, speed=1, reverse=False, loop=True, quality="low", height=150)

# Create dashboard tabs
tab1, tab2, tab3 = st.tabs(["Top 10 Data 🔝", "Global Metrics 🌎", "Country Analysis 🗺️"])

with tab1:
    st.header('Top 10 Titles This Week')
    
    # Language filter
    language_type = st.radio("Select Language Category", ["English", "Non-English"], horizontal=True)
    
    # Week filter
    week_query = """
        SELECT DISTINCT extraction_date as week
        FROM ba882_project.stage.netflix_most_popular 
        ORDER BY week DESC
    """
    available_weeks = conn.execute(week_query).df()
    selected_week = st.selectbox("Select Week", available_weeks['week'])
    
    # Create subtabs for Shows and Movies
    show_tab, movie_tab = st.tabs(["TV Shows", "Movies"])
    
    with show_tab:
        show_query = f"""
            SELECT title, weekly_hours_viewed, cumulative_weeks_in_top_10
            FROM ba882_project.stage.netflix_most_popular
            WHERE extraction_date = ?
            AND category = 'TV'
            AND is_english = {'true' if language_type == 'English' else 'false'}
            ORDER BY weekly_hours_viewed DESC
            LIMIT 10
        """
        shows_data = conn.execute(show_query, [selected_week]).df()
        fig_shows = px.bar(shows_data, 
                          x='title', 
                          y='weekly_hours_viewed',
                          title='Top 10 TV Shows',
                          color_discrete_sequence=['#E50914'])
        fig_shows.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='white'
        )
        st.plotly_chart(fig_shows)
        
    with movie_tab:
        movie_query = f"""
            SELECT title, weekly_hours_viewed, cumulative_weeks_in_top_10
            FROM ba882_project.stage.netflix_most_popular
            WHERE week = ?
            AND category = 'Films'
            AND is_english = {'true' if language_type == 'English' else 'false'}
            ORDER BY weekly_hours_viewed DESC
            LIMIT 10
        """
        movies_data = conn.execute(movie_query, [selected_week]).df()
        fig_movies = px.bar(movies_data, 
                           x='title', 
                           y='weekly_hours_viewed',
                           title='Top 10 Movies',
                           color_discrete_sequence=['#E50914'])
        fig_movies.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='white'
        )
        st.plotly_chart(fig_movies)

with tab2:
    st.header('Global Viewing Metrics')
    
    # Shows metrics
    shows_query = """
        SELECT show_title, SUM(weekly_hours_viewed) as total_hours
        FROM ba882_project.stage.netflix_global
        WHERE category = 'TV'
        GROUP BY show_title
        ORDER BY total_hours DESC
        LIMIT 10
    """
    top_shows = conn.execute(shows_query).df()
    fig_top_shows = px.bar(top_shows, 
                          x='show_title', 
                          y='total_hours',
                          title='Most Watched TV Shows Globally',
                          color_discrete_sequence=['#E50914'])
    fig_top_shows.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font_color='white'
    )
    st.plotly_chart(fig_top_shows)
    
    # Movies metrics
    movies_query = """
        SELECT show_title, SUM(weekly_hours_viewed) as total_hours
        FROM ba882_project.stage.netflix_global
        WHERE category = 'Films'
        GROUP BY show_title
        ORDER BY total_hours DESC
        LIMIT 10
    """
    top_movies = conn.execute(movies_query).df()
    fig_top_movies = px.bar(top_movies, 
                           x='show_title', 
                           y='total_hours',
                           title='Most Watched Movies Globally',
                           color_discrete_sequence=['#E50914'])
    fig_top_movies.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font_color='white'
    )
    st.plotly_chart(fig_top_movies)

with tab3:
    st.header('Country-wise Analysis')
    
    # Title selection
    title_query = """
        SELECT DISTINCT show_title 
        FROM ba882_project.stage.netflix_countries
        ORDER BY show_title
    """
    available_titles = conn.execute(title_query).df()
    selected_title = st.selectbox("Select Title", available_titles['show_title'])
    
    # Get country data for selected title
    country_query = f"""
        SELECT country_name, 
               SUM(weekly_hours_viewed) as total_hours
        FROM ba882_project.stage.netflix_countries
        WHERE show_title = ?
        GROUP BY country_name
        ORDER BY total_hours DESC
    """
    country_data = conn.execute(country_query, [selected_title]).df()
    
    fig_map = px.choropleth(country_data,
                           locations='country_name',
                           locationmode='country names',
                           color='total_hours',
                           title=f'Viewing Hours by Country: {selected_title}',
                           color_continuous_scale=['#FFB3B3', '#E50914'])
    fig_map.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font_color='white'
    )
    st.plotly_chart(fig_map)

# # Show Details Section
# st.header('Content Details Explorer')

# # expander = st.expander("Select a Movie/Show")
# title_list = get_titles()
# # selection = expander.selectbox(title_list, key="title_select")
# selected_show = st.selectbox('Select a Title', title_list)

# show_details_query = f"""
#     SELECT title, overview, releaseYear, rating, runtime, directors, cast
#     FROM ba882_project.stage.netflix_api
#     WHERE title = ?
#     LIMIT 1
# """
# show_details = conn.execute(show_details_query, [selected_show]).df().iloc[0]

# col1, col2 = st.columns(2)
# with col1:
#     st.write(f"**Title:** {show_details['title']}")
#     st.write(f"**Release Year:** {show_details['releaseYear']}")
#     st.write(f"**Rating:** {show_details['rating']}")
#     st.write(f"**Runtime:** {show_details['runtime']}")
# with col2:
#     st.write(f"**Directors:** {show_details['directors']}")
#     st.write(f"**Cast:** {show_details['cast']}")
#     st.write(f"**Overview:** {show_details['overview']}")