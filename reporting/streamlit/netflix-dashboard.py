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
    .netflix-table table {
        border-collapse: collapse;
        width: 100%;
    }
    .netflix-table td {
        padding: 8px 12px;
        vertical-align: middle;
    }
    .netflix-table tr {
        border-bottom: 1px solid rgba(255, 255, 255, 0.1);
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
    .season-title-text {
        font-weight: 400;
        opacity: 0.8;
    }
    .netflix-table .rank-header,
    .netflix-table .title-header,
    .netflix-table .weeks-header,
    .netflix-table .season-title-header {
        font-weight: bold;
        color: #ffffff;
        padding: 8px;
        text-align: left;
        border-bottom: 2px solid #404040;
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 1px;
        opacity: 0.7;
    }
    </style>
    """, unsafe_allow_html=True)

    with show_tab:
        # Updated header with season column
        st.markdown("""
        <div class="netflix-table">
            <table width="100%">
                <tr>
                    <td width="10%" class="rank-header">Rank</td>
                    <td width="45%" class="title-header">Title</td>
                    <td width="30%" class="season-title-header">Season</td>
                    <td width="15%" class="weeks-header">Weeks</td>
                </tr>
            </table>
        </div>
        """, unsafe_allow_html=True)
        
        show_query = f"""
            SELECT 
                m.rank,
                m.category,
                m.show_title,
                m.season_title,
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
            st.markdown(f"""
            <div class="netflix-table">
                <table width="100%">
                    <tr>
                        <td width="10%" class="rank-number">{int(row['rank'])}</td>
                        <td width="45%" class="title-text">{row['show_title']}</td>
                        <td width="30%" class="season-title-text">{row['season_title']}</td>
                        <td width="15%" class="weeks-number">{int(row['cumulative_weeks_in_top_10'])}</td>
                    </tr>
                </table>
            </div>
            """, unsafe_allow_html=True)

    with movie_tab:
        # Movies header without season column
        st.markdown("""
        <div class="netflix-table">
            <table width="100%">
                <tr>
                    <td width="10%" class="rank-header">Rank</td>
                    <td width="70%" class="title-header">Title</td>
                    <td width="20%" class="weeks-header">Weeks</td>
                </tr>
            </table>
        </div>
        """, unsafe_allow_html=True)
            
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
            st.markdown(f"""
            <div class="netflix-table">
                <table width="100%">
                    <tr>
                        <td width="10%" class="rank-number">{int(row['rank'])}</td>
                        <td width="70%" class="title-text">{row['show_title']}</td>
                        <td width="20%" class="weeks-number">{int(row['cumulative_weeks_in_top_10'])}</td>
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
                          x='show_title',
                          y='view_rate',
                          title=f'Top 5 {language_type} TV Shows by Average View Duration',
                          color_discrete_sequence=['#E50914'])
    fig_top_shows.update_layout(
        plot_bgcolor='rgba(0,0,0,1)',
        paper_bgcolor='rgba(0,0,0,0)',
        font_color='white',
        xaxis_title="",
        yaxis_title="Average Hours per View",
        xaxis={'categoryorder': 'total descending'},
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
                           x='show_title',
                           y='view_rate',
                           title=f'Top 5 {language_type} Movies by Average View Duration',
                           color_discrete_sequence=['#E50914'])
    fig_top_movies.update_layout(
        plot_bgcolor='rgba(0,0,0,1)',
        paper_bgcolor='rgba(0,0,0,0)',
        font_color='white',
        xaxis_title="",
        yaxis_title="Average Hours per View",
        xaxis={'categoryorder': 'total descending'},
        height=400
    )
    st.plotly_chart(fig_top_movies, use_container_width=True)

with tab3:
    st.header('Country-wise Analysis')
    
    # Country selection
    country_query = """
        SELECT DISTINCT country_name 
        FROM ba882_project.stage.netflix_countries
        ORDER BY country_name
    """
    available_countries = conn.execute(country_query).df()
    selected_country = st.selectbox("Select Country", available_countries['country_name'])
    
    # Get top 5 shows data for selected country
    show_query = f"""
        SELECT c.show_title,
               SUM(g.weekly_hours_viewed) as total_hours,
               MAX(g.week) as latest_week
        FROM ba882_project.stage.netflix_countries c
        INNER JOIN ba882_project.stage.netflix_global g
        ON c.show_title = g.show_title
        WHERE c.country_name = ?
        GROUP BY c.show_title
        ORDER BY total_hours DESC
        LIMIT 5
    """
    show_data = conn.execute(show_query, [selected_country]).df()
    
    # Create treemap with Netflix styling
    fig_treemap = px.treemap(
        show_data,
        path=['show_title'],
        values='total_hours',
        title=f'Top 5 Shows in {selected_country}',
        color='total_hours',
        color_continuous_scale=['#E50914', '#831010'],
        hover_data={'total_hours': ':,.0f'}
    )
    
    fig_treemap.update_layout(
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font_color='white',
        font_family='Helvetica Neue',
        height=500,
        margin=dict(t=50, b=20, l=20, r=20),
        title_font_size=20,
        title_x=0.5,
        title_font_color='white'
    )
    
    fig_treemap.update_traces(
        textfont_color='white',
        textfont_size=14,
        hovertemplate='<b>%{label}</b><br>Total Hours: %{customdata[0]:,.0f}<extra></extra>'
    )
    
    # Display treemap
    selected_title = st.plotly_chart(fig_treemap, use_container_width=True, key='show_treemap')
    
    # Add selectbox for show selection
    available_shows = show_data['show_title'].tolist()
    if available_shows:
        selected_show = st.selectbox("Select Show for Weekly Trend", available_shows)
        
        # Get weekly data for selected show in selected country
        weekly_query = """
            SELECT g.week,
                   g.weekly_hours_viewed
            FROM ba882_project.stage.netflix_global g
            INNER JOIN ba882_project.stage.netflix_countries c
            ON g.show_title = c.show_title
            WHERE g.show_title = ?
            AND c.country_name = ?
            ORDER BY g.week
        """
        weekly_data = conn.execute(weekly_query, [selected_show, selected_country]).df()
        
        # Create line chart with Netflix styling
        fig_line = px.line(
            weekly_data,
            x='week',
            y='weekly_hours_viewed',
            title=f'Weekly Viewing Hours in {selected_country}: {selected_show}',
            markers=True
        )
        
        fig_line.update_layout(
            plot_bgcolor='rgba(0,0,0,1)',
            paper_bgcolor='rgba(0,0,0,0)',
            font_color='white',
            font_family='Helvetica Neue',
            height=400,
            margin=dict(t=50, b=50, l=50, r=20),
            xaxis_title="Week",
            yaxis_title="Hours Viewed",
            showlegend=False,
            title_x=0.5,
            title_font_size=18,
            xaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.1)'),
            yaxis=dict(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
        )
        
        fig_line.update_traces(
            line_color='#E50914',
            marker_color='#E50914',
            marker_size=8,
            hovertemplate='<b>Week: %{x}</b><br>Hours: %{y:,.0f}<extra></extra>'
        )
        
        st.plotly_chart(fig_line, use_container_width=True)
                
# Show details section
    if selected_show:
        details_query = """
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
    show_details = conn.execute(details_query, [selected_show]).df()
    
    if not show_details.empty:
        st.markdown("### Show Details")
        # Format title without (nan)
        title_display = show_details['title'].iloc[0]
        if pd.notna(show_details['releaseYear'].iloc[0]):
            title_display += f" ({show_details['releaseYear'].iloc[0]})"
            
        with st.expander(title_display):
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**Rating:** {show_details['rating'].iloc[0]}")
                st.markdown(f"**Type:** {show_details['itemType'].iloc[0]}")
            with col2:
                st.markdown(f"**Genres:** {show_details['genres'].iloc[0]}")
                st.markdown(f"**Runtime:** {show_details['runtime'].iloc[0]}")
            st.markdown("**Overview**")
            st.write(show_details['overview'].iloc[0])