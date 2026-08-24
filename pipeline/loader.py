"""Ingests every row of a flat file into its BigQuery staging table, as-is.

No validation happens here - that is Dataplex's job (dataplex/dq_scan_*.yaml),
run separately against the staging table after this load. Staging tables
are all-STRING so a malformed value (bad date, non-numeric amount, etc.)
never fails or drops a row at load time; Dataplex's regex/range/sqlAssertion
rules are what catch it afterward.
"""
import logging

from google.cloud import bigquery

logger = logging.getLogger(__name__)


def load_all_files(project: str, bucket: str, prefix: str, bq_dataset: str, bq_table: str) -> dict:
    """Loads every CSV file under gs://bucket/prefix/ into bq_dataset.bq_table,
    appending. Every row in every file is ingested - there is no row-level
    filtering in this step.
    """
    client = bigquery.Client(project=project)
    source_uri = f"gs://{bucket}/{prefix}*.csv"
    table_id = f"{project}.{bq_dataset}.{bq_table}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        # STRING for every column: ingestion never fails on a bad value -
        # Dataplex validates content after load.
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        allow_quoted_newlines=True,
        allow_jagged_rows=True,
        ignore_unknown_values=True,
    )

    load_job = client.load_table_from_uri(source_uri, table_id, job_config=job_config)
    result = load_job.result()  # raises only on a genuine job failure (bad URI, permissions), not on row content
    logger.info("Loaded %d rows from %s into %s", result.output_rows, source_uri, table_id)
    return {"table": table_id, "rows_loaded": result.output_rows}
