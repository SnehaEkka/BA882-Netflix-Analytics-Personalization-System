# The Machine Learning Datasets job

# imports
import requests
import json
from prefect import flow, task

# helper function - generic invoker
def invoke_gcf(url:str, payload:dict):
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()

@task(retries=2)
def netflix_data():
    """Extract Netflix data into CSV on GCS for downstream ML tasks"""
    url = "https://us-central1-ba882-inclass-project.cloudfunctions.net/ml-netflix-data"
    resp = invoke_gcf(url, payload={})
    return resp

# @task(retries=2)
# def post_tags():
#     """Extract the RSS feeds into JSON on GCS"""
#     url = "https://us-central1-btibert-ba882-fall24.cloudfunctions.net/ml-post-tags"
#     resp = invoke_gcf(url, payload={})
#     return resp

# the job
@flow(name="BA882-Project-ml-datasets", log_prints=True)
def ba882_ml_datasets():
    # these are independent tasks
    netflix_data()

# the job
if __name__ == "__main__":
    ba882_ml_datasets()