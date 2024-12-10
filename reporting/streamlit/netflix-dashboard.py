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

# with tab1:
#     st.header('Top 10 Titles This Week')
    
#     # Language filter
#     language_type = st.radio("Select Language Category", ["English", "Non-English"], horizontal=True)
    
#     # Create subtabs for Shows and Movies
#     show_tab, movie_tab = st.tabs(["TV Shows", "Movies"])
    
#     with show_tab:
#         show_query = f"""
#             SELECT 
#                 m.rank,
#                 m.category,
#                 m.show_title,
#                 COALESCE(g.cumulative_weeks_in_top_10, 1) as cumulative_weeks_in_top_10
#             FROM ba882_project.stage.netflix_most_popular m
#             LEFT JOIN (
#                 SELECT DISTINCT ON (show_title)
#                     show_title,
#                     cumulative_weeks_in_top_10
#                 FROM ba882_project.stage.netflix_global
#                 WHERE show_title IN (
#                     SELECT show_title 
#                     FROM ba882_project.stage.netflix_most_popular
#                     WHERE category = 'TV ({"English" if language_type == "English" else "Non-English"})'
#                 )
#                 ORDER BY show_title, week DESC
#             ) g
#             ON m.show_title = g.show_title
#             WHERE m.category = 'TV ({"English" if language_type == "English" else "Non-English"})'
#             ORDER BY m.rank
#         """
#         shows_data = conn.execute(show_query).df()
        
#         # Format the data for display
#         st.markdown("""
#         <style>
#         .netflix-table {
#             width: 100%;
#             color: white;
#             font-size: 16px;
#         }
#         .netflix-table td {
#             padding: 10px;
#         }
#         .weeks-indicator {
#             color: #E50914;
#             font-family: monospace;
#         }
#         </style>
#         """, unsafe_allow_html=True)
        
#         for _, row in shows_data.iterrows():
#             weeks = '▮' * int(row['cumulative_weeks_in_top_10'])
#             st.markdown(f"""
#             <div class="netflix-table">
#                 <table width="100%">
#                     <tr>
#                         <td width="5%">{int(row['rank'])}</td>
#                         <td width="45%">{row['show_title']}</td>
#                         <td width="10%">{int(row['cumulative_weeks_in_top_10'])}</td>
#                         <td width="40%" class="weeks-indicator">{weeks}</td>
#                     </tr>
#                 </table>
#             </div>
#             """, unsafe_allow_html=True)
    
#     with movie_tab:
#         movie_query = f"""
#             SELECT 
#                 m.rank,
#                 m.category,
#                 m.show_title,
#                 COALESCE(g.cumulative_weeks_in_top_10, 1) as cumulative_weeks_in_top_10
#             FROM ba882_project.stage.netflix_most_popular m
#             LEFT JOIN (
#                 SELECT DISTINCT ON (show_title)
#                     show_title,
#                     cumulative_weeks_in_top_10
#                 FROM ba882_project.stage.netflix_global
#                 WHERE show_title IN (
#                     SELECT show_title 
#                     FROM ba882_project.stage.netflix_most_popular
#                     WHERE category = 'Films ({"English" if language_type == "English" else "Non-English"})'
#                 )
#                 ORDER BY show_title, week DESC
#             ) g
#             ON m.show_title = g.show_title
#             WHERE m.category = 'Films ({"English" if language_type == "English" else "Non-English"})'
#             ORDER BY m.rank
#         """
#         movies_data = conn.execute(movie_query).df()
        
#         for _, row in movies_data.iterrows():
#             weeks = '▮' * int(row['cumulative_weeks_in_top_10'])
#             st.markdown(f"""
#             <div class="netflix-table">
#                 <table width="100%">
#                     <tr>
#                         <td width="5%">{int(row['rank'])}</td>
#                         <td width="45%">{row['show_title']}</td>
#                         <td width="10%">{int(row['cumulative_weeks_in_top_10'])}</td>
#                         <td width="40%" class="weeks-indicator">{weeks}</td>
#                     </tr>
#                 </table>
#             </div>
#             """, unsafe_allow_html=True)

with tab1:
    st.header('Top 10 Titles This Week')
    
    # Language filter
    language_type = st.radio("Select Language Category", ["English", "Non-English"], horizontal=True)
    
    # Create subtabs for Shows and Movies
    show_tab, movie_tab = st.tabs(["TV Shows", "Movies"])
    
    # Updated styling with black background and compact design
    st.markdown("""
    <style>
    .netflix-table {
        width: 100%;
        color: white;
        font-size: 14px;
        margin-bottom: 4px;
        background-color: rgba(0, 0, 0, 0.9);
        border-radius: 4px;
    }
    .netflix-table td {
        padding: 8px 12px;
        vertical-align: middle;
    }
    .netflix-table tr {
        border-bottom: 1px solid rgba(255, 255, 255, 0.1);
    }
    .weeks-indicator {
        color: #E50914;
        font-family: monospace;
        letter-spacing: 2px;
    }
    .rank-number {
        font-weight: bold;
        font-size: 16px;
    }
    .title-text {
        font-weight: 500;
    }
    .weeks-number {
        text-align: center;
        opacity: 0.9;
    }
    </style>
    """, unsafe_allow_html=True)
    
    with show_tab:
        show_query = f"""
            SELECT 
                m.rank,
                m.category,
                m.show_title,
                COALESCE(g.cumulative_weeks_in_top_10, 1) as cumulative_weeks_in_top_10
            FROM ba882_project.stage.netflix_most_popular m
            LEFT JOIN (
                SELECT DISTINCT ON (show_title)
                    show_title,
                    cumulative_weeks_in_top_10
                FROM ba882_project.stage.netflix_global
                WHERE show_title IN (
                    SELECT show_title 
                    FROM ba882_project.stage.netflix_most_popular
                    WHERE category = 'TV ({"English" if language_type == "English" else "Non-English"})'
                )
                ORDER BY show_title, week DESC
            ) g
            ON m.show_title = g.show_title
            WHERE m.category = 'TV ({"English" if language_type == "English" else "Non-English"})'
            ORDER BY m.rank
        """
        shows_data = conn.execute(show_query).df()
        
        for _, row in shows_data.iterrows():
            weeks = '▮' * int(row['cumulative_weeks_in_top_10'])
            st.markdown(f"""
            <div class="netflix-table">
                <table width="100%">
                    <tr>
                        <td width="5%" class="rank-number">{int(row['rank'])}</td>
                        <td width="45%" class="title-text">{row['show_title']}</td>
                        <td width="10%" class="weeks-number">{int(row['cumulative_weeks_in_top_10'])}</td>
                        <td width="40%" class="weeks-indicator">{weeks}</td>
                    </tr>
                </table>
            </div>
            """, unsafe_allow_html=True)
    
    with movie_tab:
        movie_query = f"""
            SELECT 
                m.rank,
                m.category,
                m.show_title,
                COALESCE(g.cumulative_weeks_in_top_10, 1) as cumulative_weeks_in_top_10
            FROM ba882_project.stage.netflix_most_popular m
            LEFT JOIN (
                SELECT DISTINCT ON (show_title)
                    show_title,
                    cumulative_weeks_in_top_10
                FROM ba882_project.stage.netflix_global
                WHERE show_title IN (
                    SELECT show_title 
                    FROM ba882_project.stage.netflix_most_popular
                    WHERE category = 'Films ({"English" if language_type == "English" else "Non-English"})'
                )
                ORDER BY show_title, week DESC
            ) g
            ON m.show_title = g.show_title
            WHERE m.category = 'Films ({"English" if language_type == "English" else "Non-English"})'
            ORDER BY m.rank
        """
        movies_data = conn.execute(movie_query).df()
        
        for _, row in movies_data.iterrows():
            weeks = '▮' * int(row['cumulative_weeks_in_top_10'])
            st.markdown(f"""
            <div class="netflix-table">
                <table width="100%">
                    <tr>
                        <td width="5%" class="rank-number">{int(row['rank'])}</td>
                        <td width="45%" class="title-text">{row['show_title']}</td>
                        <td width="10%" class="weeks-number">{int(row['cumulative_weeks_in_top_10'])}</td>
                        <td width="40%" class="weeks-indicator">{weeks}</td>
                    </tr>
                </table>
            </div>
            """, unsafe_allow_html=True)

with tab2:
    st.header('Global Viewing Metrics')
    
    # Language filter
    language_type = st.radio("Select Language Category", ["English", "Non-English"], horizontal=True, key="global_lang")
    
    # Shows metrics
    shows_query = f"""
        SELECT show_title, 
               SUM(weekly_hours_viewed) as total_hours, 
               SUM(weekly_views) as total_views,
               CAST(total_hours AS FLOAT) / NULLIF(total_views, 0) as view_rate
        FROM ba882_project.stage.netflix_global
        WHERE category = 'TV ({"English" if language_type == "English" else "Non-English"})'
        GROUP BY show_title
        HAVING total_views > 0
        ORDER BY view_rate DESC
        LIMIT 5
    """
    top_shows = conn.execute(shows_query).df()
    
    fig_top_shows = px.bar(top_shows, 
                          x='view_rate',
                          y='show_title',
                          orientation='h',
                          title=f'Top 5 {language_type} TV Shows by Average View Duration',
                          color_discrete_sequence=['#E50914'])
    fig_top_shows.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font_color='white',
        xaxis_title="Average Hours per View",
        yaxis_title="",
        yaxis={'categoryorder': 'total ascending'},
        height=400
    )
    st.plotly_chart(fig_top_shows, use_container_width=True)

    # Movies metrics
    movies_query = f"""
        SELECT show_title, 
               SUM(weekly_hours_viewed) as total_hours, 
               SUM(weekly_views) as total_views,
               CAST(total_hours AS FLOAT) / NULLIF(total_views, 0) as view_rate
        FROM ba882_project.stage.netflix_global
        WHERE category = 'Films ({"English" if language_type == "English" else "Non-English"})'
        GROUP BY show_title
        HAVING total_views > 0
        ORDER BY view_rate DESC
        LIMIT 5
    """
    top_movies = conn.execute(movies_query).df()
    
    fig_top_movies = px.bar(top_movies, 
                           x='view_rate',
                           y='show_title',
                           orientation='h',
                           title=f'Top 5 {language_type} Movies by Average View Duration',
                           color_discrete_sequence=['#E50914'])
    fig_top_movies.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font_color='white',
        xaxis_title="Average Hours per View",
        yaxis_title="",
        yaxis={'categoryorder': 'total ascending'},
        height=400
    )
    st.plotly_chart(fig_top_movies, use_container_width=True)

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
        SELECT c.country_name,
               SUM(g.weekly_hours_viewed) as total_hours
        FROM ba882_project.stage.netflix_countries c
        INNER JOIN ba882_project.stage.netflix_global g
        ON c.show_title = g.show_title
        WHERE c.show_title = ?
        GROUP BY c.country_name
        ORDER BY total_hours DESC
    """
    country_data = conn.execute(country_query, [selected_title]).df()
    
    # Create choropleth map
    fig_map = px.choropleth(country_data,
                           locations='country_name',
                           locationmode='country names',
                           color='total_hours',
                           title=f'Global Viewing Hours: {selected_title}',
                           color_continuous_scale=['#FFB3B3', '#E50914'])
    fig_map.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font_color='white',
        height=600,
        margin=dict(t=50, b=50)
    )
    st.plotly_chart(fig_map, use_container_width=True)
    
    # Get title details from netflix_api
    title_details_query = f"""
        SELECT title,
               releaseYear,
               genres,
               rating,
               overview,
               runtime,
               itemType
        FROM ba882_project.stage.netflix_api
        WHERE title = ?
        LIMIT 1
    """
    title_details = conn.execute(title_details_query, [selected_title]).df()
    
    if not title_details.empty:
        st.markdown("### Title Information")
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown(f"**Release Year:** {title_details['releaseYear'].iloc[0]}")
            st.markdown(f"**Rating:** {title_details['rating'].iloc[0]}")
            st.markdown(f"**Type:** {title_details['itemType'].iloc[0]}")
            
        with col2:
            st.markdown(f"**Genres:** {title_details['genres'].iloc[0]}")
            st.markdown(f"**Runtime:** {title_details['runtime'].iloc[0]}")
            
        st.markdown("**Overview**")
        st.write(title_details['overview'].iloc[0])