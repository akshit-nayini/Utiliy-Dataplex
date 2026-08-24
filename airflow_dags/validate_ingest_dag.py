"""Airflow (Cloud Composer) DAG: ingest three flat-file sources landing in
GCS - customer, utility_details, and utility_bills - in full, validate them
with Dataplex Auto DQ, export the bad records Dataplex finds into a Parquet
file per source, register that file in Data Catalog (Dataplex Catalog /
Knowledge Catalog), and materialize a single consolidated output table.

Nothing here filters data before it lands in BigQuery: pipeline.loader
loads every row of every file into its staging table as-is (all-STRING
columns, so a malformed value never fails the load). Validation is
entirely Dataplex's job, run against the staging table afterward.

Flow (per source: customer, utility_details, utility_bills):
  1. Wait for new files under gs://<raw_bucket>/raw/<source>/.
  2. pipeline.loader.load_all_files: bulk-load every row into staging.
  3. Run that source's Dataplex Data Quality scan.
  4. pipeline.dataplex_export: pull each failed rule's auto-generated
     failing_rows_query, union the actual bad records into one Parquet
     file in GCS, and register it as a Fileset entry in Data Catalog with
     a DQ breakdown - so the bad records are visible on that dashboard.

Once all three sources are ingested and scanned, pipeline.consolidate
joins the three staging tables into one consolidated table (the join
itself excludes cross-table referential/currency issues, mirroring what
the utility_bills scan's cross-table sqlAssertion rules flag).
"""
import logging
from datetime import datetime, timedelta

import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.dataplex import DataplexRunDataQualityScanOperator

from pipeline.loader import load_all_files
from pipeline.dataplex_gate import check_scan_job
from pipeline.dataplex_export import get_scan_job_rule_results, export_bad_records_to_parquet, register_parquet_quarantine_in_catalog
from pipeline.consolidate import build_consolidated_table
from pipeline.dq_reporting import write_metrics_to_bq, register_dataset_in_datacatalog

logger = logging.getLogger(__name__)

PROJECT = "{{ var.value.gcp_project }}"
RAW_BUCKET = "{{ var.value.utility_bills_raw_bucket }}"

with open("pipeline/dq_agent_config.yaml") as f:
    DQ_AGENT_CFG = yaml.safe_load(f)["dq_agent"]

STAGING_DATASET = DQ_AGENT_CFG["staging_dataset"]
SOURCES = DQ_AGENT_CFG["sources"]  # customer, utility_details, utility_bills
QUARANTINE_BUCKET = DQ_AGENT_CFG["quarantine_gcs_bucket"]
CATALOG_CFG = DQ_AGENT_CFG["knowledge_catalog"]

default_args = {
    "owner": DQ_AGENT_CFG.get("owner", "data-eng-team"),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _make_loader(source_name, source_cfg):
    def _run(**_):
        load_all_files(
            project=PROJECT,
            bucket=RAW_BUCKET,
            prefix=f"raw/{source_name}/",
            bq_dataset=STAGING_DATASET,
            bq_table=source_cfg["staging_table"],
        )
    return _run


def _make_scan_export(source_name):
    def _run(**context):
        job_id = context["ti"].xcom_pull(task_ids=f"dataplex_scan_{source_name}", key="job_id")

        # informational alert only - never blocks anything below
        decision = check_scan_job(PROJECT, DQ_AGENT_CFG, job_id)
        if decision["alert"]:
            logger.warning("DQ alert for %s (informational, ingestion continues): %s", source_name, decision["reason"])
        context["ti"].xcom_push(key="dq_decision", value=decision)

        rule_results = get_scan_job_rule_results(
            PROJECT, DQ_AGENT_CFG["dataplex_scan"]["location"], SOURCES[source_name]["data_scan_id"], job_id
        )
        export_result = export_bad_records_to_parquet(
            PROJECT, DQ_AGENT_CFG["reporting"]["bq_dataset"], source_name, job_id, rule_results, QUARANTINE_BUCKET
        )
        register_parquet_quarantine_in_catalog(PROJECT, CATALOG_CFG, source_name, export_result)
        context["ti"].xcom_push(key="export_result", value=export_result)
        return export_result
    return _run


def _consolidate_and_report(**context):
    consolidation = build_consolidated_table(
        PROJECT, STAGING_DATASET,
        customer_table=SOURCES["customer"]["staging_table"],
        utility_details_table=SOURCES["utility_details"]["staging_table"],
        bills_table=SOURCES["utility_bills"]["staging_table"],
        output_table=DQ_AGENT_CFG["consolidated_output_table"],
    )

    bills_decision = context["ti"].xcom_pull(task_ids="scan_export_utility_bills", key="dq_decision")
    reporting_cfg = DQ_AGENT_CFG["reporting"]

    write_metrics_to_bq(
        PROJECT, reporting_cfg["bq_dataset"], reporting_cfg["bq_table"],
        {
            "dataset": f"{STAGING_DATASET}.{consolidation['output_table']}",
            "row_count": consolidation["row_count"],
            "cross_table_invalid_ratio": bills_decision["invalid_ratio"],
        },
    )
    register_dataset_in_datacatalog(
        PROJECT, CATALOG_CFG["location"], CATALOG_CFG["entry_group"],
        consolidation["output_table"],
        {
            "display_name": "Utility Bills - Consolidated",
            "linked_resource": f"//bigquery.googleapis.com/projects/{PROJECT}/datasets/{STAGING_DATASET}/tables/{consolidation['output_table']}",
            "dq_report": bills_decision,
            "row_count": consolidation["row_count"],
        },
    )


with DAG(
    dag_id="validate_ingest_utility_bills",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dq_agent", "utility_bills"],
) as dag:

    export_tasks = []

    for source_name, source_cfg in SOURCES.items():
        wait = GCSObjectsWithPrefixExistenceSensor(
            task_id=f"wait_for_files_{source_name}",
            bucket=RAW_BUCKET,
            prefix=f"raw/{source_name}/",
            mode="reschedule",
            timeout=60 * 60,
        )
        load = PythonOperator(
            task_id=f"load_all_{source_name}",
            python_callable=_make_loader(source_name, source_cfg),
        )
        scan = DataplexRunDataQualityScanOperator(
            task_id=f"dataplex_scan_{source_name}",
            project_id=PROJECT,
            region=DQ_AGENT_CFG["dataplex_scan"]["location"],
            data_scan_id=source_cfg["data_scan_id"],
            asynchronous=False,
        )
        scan_export = PythonOperator(
            task_id=f"scan_export_{source_name}",
            python_callable=_make_scan_export(source_name),
        )
        wait >> load >> scan >> scan_export
        export_tasks.append(scan_export)

    consolidate = PythonOperator(
        task_id="consolidate_and_report",
        trigger_rule="all_done",
        python_callable=_consolidate_and_report,
    )

    export_tasks >> consolidate
