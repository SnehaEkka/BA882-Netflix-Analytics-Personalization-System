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

    # Netflix Top 10 Country List
    raw_tbl_name = f"{db_schema}.netflix_countries"
    raw_tbl_sql = f"""
    DROP TABLE IF EXISTS {raw_tbl_name};
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        country_name VARCHAR
        ,country_code VARCHAR
        ,week TIMESTAMP
        ,category VARCHAR
        ,weekly_rank INT
        ,show_title VARCHAR
        ,season_title VARCHAR
        ,cumulative_weeks_in_top_10 INT
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)
	
	# Netflix Top 10 Global List
    raw_tbl_name = f"{db_schema}.netflix_global"
    raw_tbl_sql = f"""
    DROP TABLE IF EXISTS {raw_tbl_name};
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        week TIMESTAMP
        ,category VARCHAR
        ,weekly_rank BIGINT
        ,show_title VARCHAR
        ,season_title VARCHAR
		,weekly_hours_viewed BIGINT
		,run_time DOUBLE
		,weekly_views DOUBLE
        ,cumulative_weeks_in_top_10 BIGINT
		,is_staggered_launch BOOLEAN
		,episode_launch_details VARCHAR
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)
	
	# Netflix Top 10 Most Popular
    raw_tbl_name = f"{db_schema}.netflix_most_popular"
    raw_tbl_sql = f"""
    DROP TABLE IF EXISTS {raw_tbl_name};
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        category VARCHAR
        ,rank INT
        ,show_title VARCHAR
        ,season_title VARCHAR
		,hours_viewed_first_91_days INT64
		,run_time DOUBLE
		,views_first_91_days INT64
        ,extraction_date DATE
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    return {}, 200