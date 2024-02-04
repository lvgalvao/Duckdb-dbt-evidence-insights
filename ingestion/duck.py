""" Helper functions for interacting with DuckDB """
from loguru import logger
import os

def create_table_from_dataframe(duckdb_con, table_name: str, dataframe: str):
    duckdb_con.sql(
        f"""
        CREATE TABLE {table_name} AS 
            SELECT *
            FROM {dataframe}
        """
    )

def load_aws_credentials(duckdb_con):
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY')
    aws_secret_access_key = os.getenv('AWS_SECREAT_ACCESS_KEY')
    aws_s3_region = os.getenv('AWS_S3_REGION')
    duckdb_con.execute("INSTALL 'httpfs';")
    duckdb_con.execute("LOAD 'httpfs';")
    duckdb_con.execute(f"SET s3_region = '{aws_s3_region}';")
    duckdb_con.execute(f"SET s3_access_key_id = '{aws_access_key_id}';")
    duckdb_con.execute(f"SET s3_secret_access_key = '{aws_secret_access_key}';")

def write_to_s3_from_duckdb(
    duckdb_con, table: str, s3_path: str, timestamp_column: str
):
    logger.info(f"Writing data to S3 {s3_path}/{table}")
    duckdb_con.sql(
        f"""
        COPY (
            SELECT *,
                YEAR({timestamp_column}) AS year, 
                MONTH({timestamp_column}) AS month 
            FROM {table}
        ) 
        TO '{s3_path}/{table}' 
        (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE 1, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 1000000);
    """
    )
