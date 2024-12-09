import functions_framework
from google.cloud import secretmanager
import duckdb
from pinecone import Pinecone, ServerlessSpec

# settings
project_id = 'ba882-inclass-project'
secret_id = 'duckdb-token'
version_id = 'latest'
vector_secret = "pinecone"

# db setup
db = 'ba882_project'
movie_schema = "genai_movies"
show_schema = "genai_shows"
movie_db_schema = f"{db}.{movie_schema}"
show_db_schema = f"{db}.{show_schema}"
vector_index = "overview-content"

@functions_framework.http
def task(request):

    # instantiate the services 
    sm = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # initiate the MotherDuck connection through an access token through
    md = duckdb.connect(f'md:?motherduck_token={md_token}') 

    ##################################################### create the schema

    # create the schemas
    md.sql(f"CREATE SCHEMA IF NOT EXISTS {movie_db_schema};")
    md.sql(f"CREATE SCHEMA IF NOT EXISTS {show_db_schema};")

    ##################################################### create the tables

    # movies table
    movie_tbl_name = f"{movie_db_schema}.pinecone_movies"
    movie_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {movie_tbl_name} (
        id VARCHAR PRIMARY KEY,
        parsed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    print(f"Creating movie table: {movie_tbl_sql}")
    md.sql(movie_tbl_sql)

    # shows table
    show_tbl_name = f"{show_db_schema}.pinecone_shows"
    show_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {show_tbl_name} (
        id VARCHAR PRIMARY KEY,
        parsed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    print(f"Creating show table: {show_tbl_sql}")
    md.sql(show_tbl_sql)


    ##################################################### vectordb 

    # Build the resource name of the secret version
    vector_name = f"projects/{project_id}/secrets/{vector_secret}/versions/{version_id}"

    # Access the secret version
    response = sm.access_secret_version(request={"name": vector_name})
    pinecone_token = response.payload.data.decode("UTF-8")

    pc = Pinecone(api_key=pinecone_token)

    if not pc.has_index(vector_index):
        pc.create_index(
            name=vector_index,
            dimension=768,
            metric="cosine",
            spec=ServerlessSpec(
                cloud='aws', # gcp <- not part of free
                region='us-east-1' # us-central1 <- not part of free
            )
        )
    
    ## wrap up
    return {}, 200