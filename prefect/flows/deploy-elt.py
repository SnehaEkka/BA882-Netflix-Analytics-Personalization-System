from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/SnehaEkka/BA882-Team05-Project.git",
        entrypoint="/home/sekka/BA882-Team05-Project/flows/elt.py:elt_flow",
    ).deploy(
        name="ba882-project-elt",
        work_pool_name="ba882-05-pool",
        job_variables={"pip_packages": ["pandas", "requests"]},
        cron="0 5 * * *",  # cron for daily at 5 AM
        tags=["prod"],
        description="Pipeline to populate Netflix incoming data daily",
        version="1.0.0",
    )