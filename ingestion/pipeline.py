from loguru import logger
import time

from ingestion.bigquery import (
    get_bigquery_client,
    get_bigquery_result,
    build_pypi_query,
)
import duckdb
from ingestion.duck import (
    create_table_from_dataframe,
    load_aws_credentials,
    write_to_s3_from_duckdb,
)
import fire
from ingestion.models import validate_dataframe, FileDownloads, PypiJobParameters

from ingestion.log_config import configure_file_logger, set_log_level

# Configurações de log
LOG_FILE_PATH = "log/logfile.log"  # Substitua pelo caminho do seu arquivo de log)

configure_file_logger(LOG_FILE_PATH)
set_log_level("INFO")


def main(params: PypiJobParameters):
    start_time = time.time()
    logger.info("Iniciando o processo de extração de dados do BigQuery")
    # Loading data from BigQuery
    df = get_bigquery_result(
        query_str=build_pypi_query(params),
        bigquery_client=get_bigquery_client(project_name=params.gcp_project),
    )

    logger.info("Processo de extração de dados do BigQuery finalizado")
    logger.info("Iniciando o processo de validação de Schema")
    validate_dataframe(df, FileDownloads)
    logger.info("Processo de validação de Schema finalizado")

    conn = duckdb.connect()
    create_table_from_dataframe(conn, params.table_name, "df")

    logger.info(f"Exportando dados para {params.destination}")
    if "local" in params.destination:
        conn.sql(f"COPY {params.table_name} TO '{params.table_name}.csv';")
        logger.info(f"Dados exportados para {params.table_name}.csv localmente")

    if "s3" in params.destination:
        load_aws_credentials(conn)
        write_to_s3_from_duckdb(
            conn, f"{params.table_name}", params.s3_path, "timestamp"
        )
        logger.info(f"Dados exportados para {params.s3_path}/{params.table_name} no S3")

    elapsed_time = time.time() - start_time
    logger.info(f"Pipeline executada com sucesso em {elapsed_time:.2f} segundos")


if __name__ == "__main__":
    fire.Fire(lambda **kwargs: main(PypiJobParameters(**kwargs)))
