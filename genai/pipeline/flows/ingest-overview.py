# imports
import requests
from prefect import flow, task
from prefect.futures import wait
from prefect.task_runners import ThreadPoolTaskRunner

# helper function - generic invoker
def invoke_gcf(url:str, payload:dict):
    response = requests.post(url, json=payload)
    response.raise_for_status()
    return response.json()

# setup the schema in the warehouse and vector store (index)
@task(retries=2)
def schema_setup():
    """Setup the schemas"""
    url = "https://us-central1-ba882-inclass-project.cloudfunctions.net/genai-schema-setup"
    resp = invoke_gcf(url, payload={})
    return resp

# get the content that hasn't been processed
@task(retries=2)
def collect():
    """Collect the movies and shows that haven't been processed"""
    url = "https://us-central1-ba882-inclass-project.cloudfunctions.net/genai-schema-collector"
    resp = invoke_gcf(url, payload={})
    return resp

# process a content item passed to it from an upstream task
@task(retries=3)
def ingest(payload):
    """For a given content id and type, embed the chunks to support GenAI workflows"""
    url = "https://us-central1-ba882-inclass-project.cloudfunctions.net/genai-schema-ingestor"
    print(f"Processing {payload['content_type']} id: {payload['content_id']}")
    resp = invoke_gcf(url, payload=payload)
    return resp

# job that uses the threadpool to process content in blocks
@flow(task_runner=ThreadPoolTaskRunner(max_workers=5), log_prints=True)
def job():

    result = schema_setup()
    print("The schema setup completed")

    collect_result = collect()
    movie_ids = collect_result.get("movies", {}).get("ids", [])
    show_ids = collect_result.get("shows", {}).get("ids", [])
    print(f"Movies to process: {len(movie_ids)}")
    print(f"Shows to process: {len(show_ids)}")

    processed_content = []

    if movie_ids or show_ids:
        print("Starting map operation over the identified content")

        for movie_id in movie_ids:
            processed_content.append(ingest.submit({"content_id": movie_id, "content_type": "movie"}))

        for show_id in show_ids:
            processed_content.append(ingest.submit({"content_id": show_id, "content_type": "show"}))

        wait(processed_content)
    
    else:
        print("No content to process, exiting now.")

# the job
if __name__ == "__main__":
    job()