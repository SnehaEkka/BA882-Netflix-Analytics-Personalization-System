# The ETL job orchestrator

# imports
import requests
import json
from prefect import flow, task

# Helper function to invoke Google Cloud Functions
def invoke_gcf(url: str, payload: dict):
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()

# Schema setup tasks
@task(retries=2)
def schema_netflix():
    """Setup the Netflix schema"""
    url = "https://us-central1-ba882-inclass-project.cloudfunctions.net/schema-netflix"
    resp = invoke_gcf(url, payload={})
    return resp

@task(retries=2)
def schema_top10():
    """Setup the Top 10 schema"""
    url = "https://us-central1-ba882-inclass-project.cloudfunctions.net/schema-top10"
    resp = invoke_gcf(url, payload={})
    return resp

@task(retries=2)
def schema_youtube():
    """Setup the YouTube schema"""
    url = "https://us-central1-ba882-inclass-project.cloudfunctions.net/schema-youtube"
    resp = invoke_gcf(url, payload={})
    return resp

# Extraction tasks
@task(retries=2)
def extract_netflix():
    """Extract Netflix data"""
    url = "https://us-central1-ba882-inclass-project.cloudfunctions.net/extract-netflix"
    resp = invoke_gcf(url, payload={})
    return resp

@task(retries=2)
def extract_top10():
    """Extract Top 10 data"""
    url = "https://us-central1-ba882-inclass-project.cloudfunctions.net/extract-top10"
    resp = invoke_gcf(url, payload={})
    return resp

@task(retries=2)
def extract_youtube():
    """Extract YouTube data"""
    url = "https://us-central1-ba882-inclass-project.cloudfunctions.net/extract-youtube"
    resp = invoke_gcf(url, payload={})
    return resp

# Load task
@task(retries=2)
def load_all_data(netflix_payload, top10_payload, youtube_payload):
    """Load all data into the database"""
    url = "https://us-central1-ba882-inclass-project.cloudfunctions.net/load-all-data"
    payload = {
        "netflix": netflix_payload,
        "top10": top10_payload,
        "youtube": youtube_payload
    }
    resp = invoke_gcf(url, payload=payload)
    return resp

# Prefect Flow
@flow(name="BA882-Project-EtLT-Flow", log_prints=True)
def elt_flow():
    """The ETL flow which orchestrates Cloud Functions"""

    # Schema setup
    schema_netflix_result = schema_netflix()
    print("Schema for Netflix set up completed")

    schema_top10_result = schema_top10()
    print("Schema for Top 10 set up completed")

    schema_youtube_result = schema_youtube()
    print("Schema for YouTube set up completed")

    # Run extraction tasks in parallel
    netflix_result = extract_netflix.submit()
    top10_result = extract_top10.submit()
    youtube_result = extract_youtube.submit()

    print("All extractions completed")
    print(f"Netflix extraction result: {netflix_result}")
    print(f"Top 10 extraction result: {top10_result}")
    print(f"YouTube extraction result: {youtube_result}")

    # Wait for all extraction results and then run load task
    load_result = load_all_data(netflix_result.result(), top10_result.result(), youtube_result.result())
    
    print("Data loaded into the database")
    
# Run the ETL flow
if __name__ == "__main__":
    elt_flow()