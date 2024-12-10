from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/SnehaEkka/BA882-Netflix-Analytics-Pipeline.git",
        entrypoint="/home/sekka/BA882-Team05-Project/genai/pipeline/flows/ingest-overview.py:job",
    ).deploy(
        name="genai-overview-ingestion",
        work_pool_name="ba882-05-pool",
        job_variables={"env": {"Team-05": "loves-to-code"},
                       "pip_packages": ["pandas", "requests"]},
        cron="15 1 * * *",
        tags=["prod"],
        description="The pipeline to grab unprocessed posts and process to store in the vector database",
        version="1.0.0",
    )