# TASK:  Create the schema to ensure it exists.

import functions_framework
from google.cloud import secretmanager
import duckdb

# settings
project_id = 'ba882-inclass-project'
secret_id = 'duckdb-token'   #<---------- this is the name of the secret you created
version_id = 'latest'

# db setup
db = 'ba882_project'
schema = "stage"
db_schema = f"{db}.{schema}"

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

    # define the DDL statement with an f string
    create_db_sql = f"CREATE DATABASE IF NOT EXISTS {db};"   

    # execute the command to create the database
    md.sql(create_db_sql)

    # confirm it exists
    print(md.sql("SHOW DATABASES").show())

    # create the schema
    md.sql(f"CREATE SCHEMA IF NOT EXISTS {db_schema};") 

    ##################################################### create the core tables in stage

    # YouTube API
    raw_tbl_name = f"{db_schema}.youtube_api"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        video_id VARCHAR PRIMARY KEY			 
        ,title VARCHAR
        ,description VARCHAR
        ,published_at TIMESTAMP
        ,views INT
        ,likes INT
        ,favorites INT
        ,comments_count INT
        ,comments VARCHAR
        ,thumbnail_url VARCHAR
        ,overall_time VARCHAR
        ,hours INT
        ,minutes INT
        ,seconds INT
        ,job_id VARCHAR 
        ,ingest_timestamp TIMESTAMP
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    return {}, 200