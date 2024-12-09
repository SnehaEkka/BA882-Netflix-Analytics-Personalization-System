import functions_framework
from google.cloud import secretmanager
import duckdb
import datetime
import uuid

# settings
project_id = 'ba882-inclass-project'
secret_id = 'duckdb-token'   #<---------- this is the name of the secret you created
version_id = 'latest'

# db setup
db = 'ba882_project'
movie_schema = "genai_movies"
show_schema = "genai_shows"
movie_db_schema = f"{db}.{movie_schema}"
show_db_schema = f"{db}.{show_schema}"

@functions_framework.http
def task(request):

    # job_id
    job_id = datetime.datetime.now().strftime("%Y%m%d%H%M") + "-" + str(uuid.uuid4())

    # instantiate the services 
    sm = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # initiate the MotherDuck connection through an access token through
    md = duckdb.connect(f'md:?motherduck_token={md_token}') 

    ##################################################### get the records delta for movies

    movie_sql = f"""
    SELECT 
        m.id 
    FROM 
        {db}.stage.netflix_api m
    WHERE
        m.showType = 'movie' AND
        m.id NOT IN (SELECT id FROM {movie_db_schema}.pinecone_movies)
    """
    
    movie_df = md.sql(movie_sql).df()
    print(f"New movies: {movie_df.shape[0]}")

    movie_ids = movie_df.id.to_list()

    ##################################################### get the records delta for shows

    show_sql = f"""
    SELECT 
        s.id 
    FROM 
        {db}.stage.shows s
    WHERE
        s.showType = 'series' AND 
        s.id NOT IN (SELECT id FROM {show_db_schema}.pinecone_shows)
    """
    
    show_df = md.sql(show_sql).df()
    print(f"New shows: {show_df.shape[0]}")

    show_ids = show_df.id.to_list()

    return {
        "num_entries": len(ids), 
        "job_id": job_id, 
        "post_ids": ids
    }, 200