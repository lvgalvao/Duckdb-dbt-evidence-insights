from ingestion.models import PypiJobParameters
from ingestion.bigquery import build_pypi_query


def test_build_pypi_query():
    params = PypiJobParameters(
        table_name="test_table",
        s3_path="s3://bucket/path",
        aws_profile="test_profile",
        gcp_project="test_project",
        start_date="2019-04-01",
        end_date="2023-11-30",
        timestamp_column="timestamp",
    )
    query = build_pypi_query(params)
    expected_query = """
    SELECT *
    FROM
        `bigquery-public-data.pypi.file_downloads`
    WHERE
        project = 'duckdb'
        AND timestamp >= TIMESTAMP("2019-04-01")
        AND timestamp < TIMESTAMP("2023-11-30")
    """
    assert query.strip() == expected_query.strip()
