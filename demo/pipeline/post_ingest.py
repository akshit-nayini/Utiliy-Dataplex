"""Everything that happens after Bronze has been loaded: run the Dataplex
scan, export the bad records it finds, build Silver and Gold, and register
everything in the catalog.

Deliberately decoupled from *how* Bronze got loaded - it queries BigQuery
directly for the row count instead of taking it as an argument, so the
same function works whether Bronze was loaded by run_demo.py calling
pipeline.loader directly, by a Dataflow job (dataflow/beam_ingest.py) that
notifies over Pub/Sub (pubsub_listener.py), or by an Airflow task
(airflow_dags/demo_orders_dag.py). One function, three trigger paths.
"""
import logging
from typing import Dict

from google.cloud import bigquery

from .dataplex_export import (
    run_scan_and_wait,
    get_scan_job_rule_results,
    export_bad_records_to_parquet,
    register_parquet_quarantine_in_catalog,
)
from .silver import build_silver_table
from .gold import build_gold_table
from .dq_reporting import write_metrics_to_bq, register_table_in_catalog

logger = logging.getLogger(__name__)

DATASET = "dq_demo"
BRONZE_TABLE = "orders_bronze"
SILVER_TABLE = "orders_silver"
GOLD_TABLE = "orders_gold"
DATA_SCAN_ID = "orders-dq-scan"


def run_post_ingest(project: str, location: str, quarantine_bucket: str, entry_group: str) -> Dict:
    bq = bigquery.Client(project=project)
    bronze_rows = list(
        bq.query(f"SELECT COUNT(*) AS n FROM `{project}.{DATASET}.{BRONZE_TABLE}`").result()
    )[0]["n"]

    job_id = run_scan_and_wait(project, location, DATA_SCAN_ID)
    rule_results = get_scan_job_rule_results(project, location, DATA_SCAN_ID, job_id)

    export_result = export_bad_records_to_parquet(project, DATASET, job_id, rule_results, quarantine_bucket)
    register_parquet_quarantine_in_catalog(project, location, entry_group, export_result)

    silver_result = build_silver_table(
        project, DATASET, rule_results, bronze_table=BRONZE_TABLE, silver_table=SILVER_TABLE
    )
    gold_result = build_gold_table(project, DATASET, silver_table=SILVER_TABLE, gold_table=GOLD_TABLE)

    write_metrics_to_bq(
        project, DATASET, "dq_metrics",
        {
            "dataset": f"{DATASET}.{BRONZE_TABLE}",
            "rows_loaded": bronze_rows,
            "rows_quarantined": export_result["total_rows"],
        },
    )
    register_table_in_catalog(
        project, location, entry_group, "orders-bronze",
        {
            "display_name": "Demo Orders - Bronze (raw, ingested as-is)",
            "linked_resource": f"//bigquery.googleapis.com/projects/{project}/datasets/{DATASET}/tables/{BRONZE_TABLE}",
            "rows_loaded": bronze_rows,
            "rows_quarantined": export_result["total_rows"],
        },
    )
    register_table_in_catalog(
        project, location, entry_group, "orders-gold",
        {
            "display_name": "Demo Orders - Gold (order count / revenue by status)",
            "linked_resource": f"//bigquery.googleapis.com/projects/{project}/datasets/{DATASET}/tables/{GOLD_TABLE}",
            "built_from_silver_rows": silver_result["row_count"],
        },
    )

    result = {
        "job_id": job_id,
        "bronze_rows": bronze_rows,
        "rows_quarantined": export_result["total_rows"],
        "silver_rows": silver_result["row_count"],
        "gold_rows": gold_result["row_count"],
        "gcs_uri": export_result["gcs_uri"],
        "bigquery_quarantine_table": export_result["bigquery_table"],
    }
    logger.info("Post-ingest complete: %s", result)
    return result
