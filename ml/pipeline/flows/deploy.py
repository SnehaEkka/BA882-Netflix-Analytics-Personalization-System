from prefect import flow
from prefect.events import DeploymentEventTrigger

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/SnehaEkka/BA882-Netflix-Analytics-Pipeline.git",
        entrypoint="/home/sekka/BA882-Team05-Project/ml/pipeline/flows/fit-model.py:training_flow",
    ).deploy(
        name="mlops-train-model",
        work_pool_name="brock-pool1",  # Freya's worker pool here!
        job_variables={"env": {"Team-05": "loves-to-code"},
                       "pip_packages": ["pandas", "requests"]},
        tags=["prod"],
        description="Pipeline to train a model and log metrics and parameters for a training job",
        version="1.0.0",
        triggers=[
            DeploymentEventTrigger(
                expect={"prefect.flow-run.Completed"},
                match_related={"prefect.resource.name": "ba882-ml-datasets"}
            )
        ]
    )