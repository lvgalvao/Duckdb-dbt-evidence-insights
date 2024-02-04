from ingestion.bigquery import (
    get_bigquery_client,
    get_bigquery_result,
    build_pypi_query,
)
from ingestion.models import PypiJobParameters
import fire

def main(params: PypiJobParameters):
    df = get_bigquery_result(
        query_str=build_pypi_query(params), 
        bigquery_client=get_bigquery_client(project_name=params.gcp_project)
    )
    print(df)
    print("Hello Pipeline!")


if __name__ == "__main__":
    fire.Fire(lambda **kwargs: main(PypiJobParameters(**kwargs)))
