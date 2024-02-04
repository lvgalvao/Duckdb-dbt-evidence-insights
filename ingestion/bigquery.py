import os
from google.cloud import bigquery
from google.oauth2 import service_account
from google.auth.exceptions import DefaultCredentialsError
from ingestion.models import PypiJobParameters
from loguru import logger
import time
import pandas as pd

PYPI_PUBLIC_DATASET = "bigquery-public-data.pypi.file_downloads"


def build_pypi_query(
    params: PypiJobParameters, pypi_public_dataset: str = PYPI_PUBLIC_DATASET
) -> str:
    # Query the public PyPI dataset from BigQuery
    # /!\ This is a large dataset, filter accordingly /!\
    return f"""
    SELECT *
    FROM
        `{pypi_public_dataset}`
    WHERE
        project = '{params.pypi_project}'
        AND {params.timestamp_column} >= TIMESTAMP("{params.start_date}")
        AND {params.timestamp_column} < TIMESTAMP("{params.end_date}")
    """


def get_bigquery_client(project_name: str) -> bigquery.Client:
    """Get Big Query client"""
    try:
        service_account_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

        if service_account_path:
            credentials = service_account.Credentials.from_service_account_file(
                service_account_path
            )
            bigquery_client = bigquery.Client(
                project=project_name, credentials=credentials
            )
            return bigquery_client

        raise EnvironmentError(
            "No valid credentials found for BigQuery authentication."
        )

    except DefaultCredentialsError as creds_error:
        raise creds_error


def get_bigquery_result(
    query_str: str, bigquery_client: bigquery.Client
) -> pd.DataFrame:
    """Get query result from BigQuery and yield rows as dictionaries."""
    try:
        dataframe = bigquery_client.query(query_str).to_dataframe()
        return dataframe

    except Exception as e:
        logger.error(f"Error running query: {e}")
        raise
