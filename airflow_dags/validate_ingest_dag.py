"""Airflow (Cloud Composer) DAG: validate three flat-file sources landing in
GCS - customer, utility_details, and utility_bills - column by column, gate
each on its own Dataplex Auto DQ scan, then run the cross-table
(utility_bills) scan for referential integrity and currency consistency,
and materialize a single consolidated output table. DQ metrics and dataset
metadata/lineage for that consolidated table are registered in Data
Catalog (Knowledge Catalog), with AI segregation enabled via config only.

Flow (per source: customer, utility_details, utility_bills):
  1. Wait for new files under gs://<raw_bucket>/raw/<source>/.
  2. Column-level validation (pipeline.validator): valid rows -> staging BQ
     table, invalid rows -> DLQ in GCS. Only bad rows are excluded.
  3. Run that source's Dataplex Data Quality scan.
  4. DQ agent gate (pipeline.dataplex_gate): stop ingestion for that source
     if its invalid ratio/count exceeds the configured threshold.

Once all three sources pass their own gate, the utility_bills scan (which
also carries the cross-table sqlAssertion rules) is re-checked for the
referential/consistency dimensions, and pipeline.consolidate joins the
three staging tables into one consolidated table - the join itself drops
any bill with no matching utility_details/customer row or a currency
mismatch, so only fully cross-validated rows reach the output.
"""
from datetime import datetime, timedelta

import yaml
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.dataplex import DataplexRunDataQualityScanOperator

from pipeline.validator import validate_prefix
from pipeline.dataplex_gate import gate_scan_job
from pipeline.consolidate import build_consolidated_table
from pipeline.dq_reporting import write_metrics_to_bq, register_dataset_in_datacatalog

PROJECT = "{{ var.value.gcp_project }}"
RAW_BUCKET = "{{ var.value.utility_bills_raw_bucket }}"
DLQ_BUCKET = "{{ var.value.utility_bills_dlq_bucket }}"

with open("pipeline/dq_agent_config.yaml") as f:
    DQ_AGENT_CFG = yaml.safe_load(f)["dq_agent"]

STAGING_DATASET = DQ_AGENT_CFG["staging_dataset"]
SOURCES = DQ_AGENT_CFG["sources"]  # customer, utility_details, utility_bills

default_args = {
    "owner": DQ_AGENT_CFG.get("owner", "data-eng-team"),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _make_column_validation(source_name, source_cfg):
    def _run(**_):
        validate_prefix(
            project=PROJECT,
            bucket=RAW_BUCKET,
            prefix=f"raw/{source_name}/",
            schema_gcs_path=source_cfg["schema"],
            bq_dataset=STAGING_DATASET,
            bq_table=source_cfg["staging_table"],
            dlq_bucket=DLQ_BUCKET,
        )
    return _run


def _make_gate(source_name):
    def _gate(**context):
        job_id = context["ti"].xcom_pull(task_ids=f"dataplex_scan_{source_name}", key="job_id")
        decision = gate_scan_job(PROJECT, DQ_AGENT_CFG, job_id)
        context["ti"].xcom_push(key="dq_decision", value=decision)
        if decision["block"]:
            raise ValueError(f"Ingestion stopped for {source_name} by DQ agent: {decision['reason']}")
        return decision
    return _gate


def _consolidate_and_report(**context):
    consolidation = build_consolidated_table(
        PROJECT, STAGING_DATASET,
        customer_table=SOURCES["customer"]["staging_table"],
        utility_details_table=SOURCES["utility_details"]["staging_table"],
        bills_table=SOURCES["utility_bills"]["staging_table"],
        output_table=DQ_AGENT_CFG["consolidated_output_table"],
    )

    bills_decision = context["ti"].xcom_pull(task_ids="dq_agent_gate_utility_bills", key="dq_decision")
    reporting_cfg = DQ_AGENT_CFG["reporting"]
    catalog_cfg = DQ_AGENT_CFG["knowledge_catalog"]

    write_metrics_to_bq(
        PROJECT, reporting_cfg["bq_dataset"], reporting_cfg["bq_table"],
        {
            "dataset": f"{STAGING_DATASET}.{consolidation['output_table']}",
            "row_count": consolidation["row_count"],
            "cross_table_invalid_ratio": bills_decision["invalid_ratio"],
        },
    )
    register_dataset_in_datacatalog(
        PROJECT, catalog_cfg["location"], catalog_cfg["entry_group"],
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

    gate_tasks = {}

    for source_name, source_cfg in SOURCES.items():
        wait = GCSObjectsWithPrefixExistenceSensor(
            task_id=f"wait_for_files_{source_name}",
            bucket=RAW_BUCKET,
            prefix=f"raw/{source_name}/",
            mode="reschedule",
            timeout=60 * 60,
        )
        validate = PythonOperator(
            task_id=f"column_level_validation_{source_name}",
            python_callable=_make_column_validation(source_name, source_cfg),
        )
        scan = DataplexRunDataQualityScanOperator(
            task_id=f"dataplex_scan_{source_name}",
            project_id=PROJECT,
            region=DQ_AGENT_CFG["dataplex_scan"]["location"],
            data_scan_id=source_cfg["data_scan_id"],
            asynchronous=False,
        )
        gate = PythonOperator(
            task_id=f"dq_agent_gate_{source_name}",
            python_callable=_make_gate(source_name),
        )
        wait >> validate >> scan >> gate
        gate_tasks[source_name] = gate

    consolidate = PythonOperator(
        task_id="consolidate_and_report",
        python_callable=_consolidate_and_report,
        trigger_rule="all_success",
    )

    list(gate_tasks.values()) >> consolidate
