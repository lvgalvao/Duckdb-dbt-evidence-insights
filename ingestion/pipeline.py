from ingestion.bigquery import (
    get_bigquery_client,
    get_bigquery_result,
    build_pypi_query,
)
import os


def main():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "demo-pypi.json"
    df = get_bigquery_result(
        build_pypi_query(), get_bigquery_client("duckdb-dbt-evidence-insights")
    )
    print(df)
    print("Hello Pipeline!")


if __name__ == "__main__":
    main()
