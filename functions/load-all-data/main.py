# imports
import functions_framework
from google.cloud import storage
from google.cloud import secretmanager
import duckdb
import pandas as pd
import datetime
from dateutil import parser
import json   
import io
import re
# from io import BytesIO						

# setup
project_id = 'ba882-inclass-project'
secret_id = 'duckdb-token'
version_id = 'latest'
bucket_name = 'ba882-team05'

# db setup
db = 'ba882_project'
raw_db_schema = f"{db}.raw"
stage_db_schema = f"{db}.stage"

# instantiate the services
sm = secretmanager.SecretManagerServiceClient()
storage_client = storage.Client()

def get_latest_job_file(bucket_name, dataset, prefix='jobs'):
    # storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
	# List all blobs in the dataset folder									  
    blobs = list(bucket.list_blobs(prefix=f"{prefix}/{dataset}/"))
    
    if not blobs:
        print('No blobs found!')
        return None
    
	# Extract job IDs and find the latest one										 
    job_ids = set()
    for blob in blobs:
        match = re.search(r'(\d{12}-[a-f0-9-]+)/', blob.name)
        if match:
            job_ids.add(match.group(1))
    
    if not job_ids:
        print('No JOB IDs found!')
        return None
    
    latest_job_id = max(job_ids)
    
	# Find the specific JSON file in the latest job folder													  
    target_blob = next((blob for blob in blobs if f"{latest_job_id}/{dataset}.json" in blob.name), None)
    
    if target_blob:
        return f"gs://{bucket_name}/{target_blob.name}"
    else:
        print('Latest JOB folder unavailable')
        return None

def process_dataset(md, dataset, json_path, request_json):
    print(f"Processing {dataset} from {json_path}")
    
    bucket = storage_client.bucket(bucket_name)
    blob_name = '/'.join(json_path.split('/')[3:])
    blob = bucket.blob(blob_name)
    json_data = blob.download_as_text()
    
    json_file = io.StringIO(json_data)
    json_df = pd.read_json(json_file, lines=True)
    # print("Before adding columns", json_df.columns)
    # json_df['job_id'] = request_json.get('jobid')
    # json_df['ingest_timestamp'] = datetime.datetime.now().isoformat()
    # print("After adding columns", json_df.columns)
    
    raw_tbl_name = f"{raw_db_schema}.{dataset}"
    raw_tbl_sql = f"""
    DROP TABLE IF EXISTS {raw_tbl_name};
    CREATE TABLE {raw_tbl_name} AS SELECT * FROM json_df;
    """
    print(f"Executing SQL: {raw_tbl_sql}")
    md.sql(raw_tbl_sql)
    
    stage_tbl_name = f"{stage_db_schema}.{dataset}"
    
    if dataset == 'netflix_api':
        insert_sql = f"""
        INSERT INTO {stage_tbl_name}
        SELECT 
        itemType			 
        ,showType
        ,id
        ,imdbId
        ,tmdbId
        ,title
        ,overview
        ,releaseYear
        ,originalTitle
        ,genres
        ,directors
        ,"cast"
        ,rating
        ,runtime
        ,year
        ,firstAirYear
        ,lastAirYear
        ,creators
        ,seasonCount 
        ,episodeCount
        FROM {raw_tbl_name}
        WHERE CAST(id as VARCHAR) NOT IN (SELECT CAST(id as VARCHAR) FROM {stage_tbl_name});
        """
    elif dataset == 'youtube_api':
        insert_sql = f"""
        INSERT INTO {stage_tbl_name}
        SELECT * FROM {raw_tbl_name}
        WHERE CAST(video_id as VARCHAR) NOT IN (SELECT CAST(video_id as VARCHAR) FROM {stage_tbl_name});
        """
    elif dataset in ['netflix_global', 'netflix_countries']:
        insert_sql = f"""
        INSERT INTO {stage_tbl_name}
        SELECT * FROM {raw_tbl_name}
        WHERE CAST(week as VARCHAR) NOT IN (SELECT CAST(week as VARCHAR) FROM {stage_tbl_name});
        """
    elif dataset == 'netflix_most_popular':
        insert_sql = f"""
        INSERT INTO {stage_tbl_name}
        SELECT * FROM {raw_tbl_name}
        WHERE CAST(extraction_date as VARCHAR) NOT IN (SELECT CAST(extraction_date as VARCHAR) FROM {stage_tbl_name});
        """
    
    print(f"Executing SQL: {insert_sql}")
    md.sql(insert_sql)
    
    print(f"Rows in {raw_tbl_name}: {md.sql(f'SELECT COUNT(*) FROM {raw_tbl_name}').fetchone()[0]}")
    print(f"Rows in {stage_tbl_name}: {md.sql(f'SELECT COUNT(*) FROM {stage_tbl_name}').fetchone()[0]}")

@functions_framework.http
def main(request):
    # Parse the request data
    request_json = request.get_json(silent=True)
    # # instantiate the services
    # sm = secretmanager.SecretManagerServiceClient()
    # storage_client = storage.Client()
    
    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    # Access the secret version
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

	# initiate the MotherDuck connection through an access token through
    md = duckdb.connect(f'md:?motherduck_token={md_token}')
    
    # create db if not exists
    create_db_sql = f"CREATE DATABASE IF NOT EXISTS {db};"
    md.sql(create_db_sql)
    
    # drop if exists and create the raw schema
    create_schema = f"DROP SCHEMA IF EXISTS {raw_db_schema} CASCADE; CREATE SCHEMA IF NOT EXISTS {raw_db_schema};"
    md.sql(create_schema)
    
    # create stage schema if first time running function
    create_schema = f"CREATE SCHEMA IF NOT EXISTS {stage_db_schema};"
    md.sql(create_schema)
    
    print(md.sql("SHOW DATABASES;").show())
    
    # List of tables to process
    datasets = ['netflix_api', 'youtube_api', 'netflix_global', 'netflix_countries', 'netflix_most_popular']
    
    for dataset in datasets:
        json_path = get_latest_job_file(bucket_name, dataset)
        print("JSON path retrived:", json_path)
        if json_path:
            process_dataset(md, dataset, json_path, request_json)
        else:
            print(f"No file found for {dataset}")
    
    return {"message": "Data loading process completed"}, 200