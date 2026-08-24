"""Bulk-loads every row of the demo Parquet file into the all-STRING
Bronze table (orders_bronze), unfiltered. Same pattern as the main
framework's pipeline/loader.py, copied here so this demo folder is fully
self-contained and can be run, tested, and torn down independently.

demo/data/sample_orders.parquet has every column written as Arrow
`string`, matching the Bronze table's schema - a Parquet file with typed
columns would reject values like amount="N/A" at write time, which would
defeat the point of the demo (letting Dataplex, not the file format, catch
bad values).
"""
import logging

from google.cloud import bigquery

logger = logging.getLogger(__name__)


def load_orders(project: str, bucket: str, prefix: str, dataset: str, table: str) -> dict:
    client = bigquery.Client(project=project)
    source_uri = f"gs://{bucket}/{prefix}*.parquet"
    table_id = f"{project}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ignore_unknown_values=True,
    )

    load_job = client.load_table_from_uri(source_uri, table_id, job_config=job_config)
    result = load_job.result()
    logger.info("Loaded %d rows from %s into %s", result.output_rows, source_uri, table_id)
    return {"table": table_id, "rows_loaded": result.output_rows}
