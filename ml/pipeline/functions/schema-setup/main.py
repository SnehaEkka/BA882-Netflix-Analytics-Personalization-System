# imports
import functions_framework
from google.cloud import secretmanager
import duckdb
import datetime
import uuid

# settings
project_id = 'ba882-inclass-project'
secret_id = 'duckdb-token'
version_id = 'latest'

# db setup
db = 'ba882_project'
schema = "mlops"
db_schema = f"{db}.{schema}"

@functions_framework.http
def task(request):
    # Generate unique job ID
    job_id = datetime.datetime.now().strftime("%Y%m%d%H%M") + "-" + str(uuid.uuid4())

    # Instantiate the Secret Manager client
    sm = secretmanager.SecretManagerServiceClient()

    # Access secret for MotherDuck token
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # Establish a connection to MotherDuck
    md = duckdb.connect(f'md:?motherduck_token={md_token}')

    # Create the schema if it doesn’t exist
    md.sql(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")

    # Create tables for movies
    # Movies Model Runs Table
    movies_model_runs_table = f"{db_schema}.movies_model_runs"
    movies_model_runs_sql = f"""
    CREATE TABLE IF NOT EXISTS {movies_model_runs_table} (
        job_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        gcs_path VARCHAR,
        model_path VARCHAR,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    print(f"Creating table: {movies_model_runs_table}")
    md.sql(movies_model_runs_sql)

    # Movies Job Metrics Table
    movies_job_metrics_table = f"{db_schema}.movies_job_metrics"
    movies_job_metrics_sql = f"""
    CREATE TABLE IF NOT EXISTS {movies_job_metrics_table} (
        job_id VARCHAR,
        metric_name VARCHAR,
        metric_value FLOAT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    print(f"Creating table: {movies_job_metrics_table}")
    md.sql(movies_job_metrics_sql)

    # Movies Job Parameters Table
    movies_job_params_table = f"{db_schema}.movies_job_parameters"
    movies_job_params_sql = f"""
    CREATE TABLE IF NOT EXISTS {movies_job_params_table} (
        job_id VARCHAR,
        parameter_name VARCHAR,
        parameter_value VARCHAR,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    print(f"Creating table: {movies_job_params_table}")
    md.sql(movies_job_params_sql)

    # Create tables for shows
    # Shows Model Runs Table
    shows_model_runs_table = f"{db_schema}.shows_model_runs"
    shows_model_runs_sql = f"""
    CREATE TABLE IF NOT EXISTS {shows_model_runs_table} (
        job_id VARCHAR PRIMARY KEY,
        name VARCHAR,
        gcs_path VARCHAR,
        model_path VARCHAR,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    print(f"Creating table: {shows_model_runs_table}")
    md.sql(shows_model_runs_sql)

    # Shows Job Metrics Table
    shows_job_metrics_table = f"{db_schema}.shows_job_metrics"
    shows_job_metrics_sql = f"""
    CREATE TABLE IF NOT EXISTS {shows_job_metrics_table} (
        job_id VARCHAR,
        metric_name VARCHAR,
        metric_value FLOAT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    print(f"Creating table: {shows_job_metrics_table}")
    md.sql(shows_job_metrics_sql)

    # Shows Job Parameters Table
    shows_job_params_table = f"{db_schema}.shows_job_parameters"
    shows_job_params_sql = f"""
    CREATE TABLE IF NOT EXISTS {shows_job_params_table} (
        job_id VARCHAR,
        parameter_name VARCHAR,
        parameter_value VARCHAR,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    print(f"Creating table: {shows_job_params_table}")
    md.sql(shows_job_params_sql)

    return {"message": "Schema and tables created successfully for both movies and shows"}, 200