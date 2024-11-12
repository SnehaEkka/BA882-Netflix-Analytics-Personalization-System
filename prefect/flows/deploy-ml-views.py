from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/SnehaEkka/BA882-Netflix-Analytics-Pipeline.git",
        entrypoint="prefect/flows/ml-views.py:ba882_ml_datasets",
    ).deploy(
        name="ba882-ml-datasets",
        work_pool_name="ba882-05-pool",
        job_variables={"env": {"Team-05": "loves-to-code"},
                       "pip_packages": ["pandas", "requests"]},
        cron="0 6 * * *",
        tags=["prod"],
        description="The pipeline to create ML datasets off of the staged data. Version is just for illustration",
        version="1.0.0",
    )
