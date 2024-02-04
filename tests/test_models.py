import pytest
import duckdb
from ingestion.models import (
    validate_dataframe,
    PypiJobParameters,
    DataFrameValidationError,
    FileDownloads,
)
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

@pytest.fixture
def file_downloads_df():
    # Set up DuckDB in-memory database
    conn = duckdb.connect(database=":memory:", read_only=False)
    conn.execute(
        """
    CREATE TABLE tbl (
        timestamp TIMESTAMP WITH TIME ZONE, 
        country_code VARCHAR, 
        url VARCHAR, 
        project VARCHAR, 
        file STRUCT(filename VARCHAR, project VARCHAR, version VARCHAR, type VARCHAR), 
        details STRUCT(
            installer STRUCT(name VARCHAR, version VARCHAR), 
            python VARCHAR, 
            implementation STRUCT(name VARCHAR, version VARCHAR), 
            distro STRUCT(
                name VARCHAR, 
                version VARCHAR, 
                id VARCHAR, 
                libc STRUCT(lib VARCHAR, version VARCHAR)
            ), 
            system STRUCT(name VARCHAR, release VARCHAR), 
            cpu VARCHAR, 
            openssl_version VARCHAR, 
            setuptools_version VARCHAR, 
            rustc_version VARCHAR
        ), 
        tls_protocol VARCHAR, 
        tls_cipher VARCHAR
    )
    """
    )

    # Load data from CSV
    conn.execute("COPY tbl FROM 'tests/sample.csv' (HEADER)")
    # Create DataFrame
    return conn.execute("SELECT * FROM tbl").df()

def test_file_downloads_validation(file_downloads_df):
    try:
        validate_dataframe(file_downloads_df, FileDownloads)
    except DataFrameValidationError as e:
        pytest.fail(f"DataFrame validation failed: {e}")


def test_file_downloads_invalid_data(file_downloads_df):
    # Introduce an invalid data entry
    file_downloads_df.loc[0, "details"] = 123  # Replace with an invalid entry

    # Expect DataFrameValidationError to be raised
    with pytest.raises(DataFrameValidationError):
        validate_dataframe(file_downloads_df, FileDownloads)