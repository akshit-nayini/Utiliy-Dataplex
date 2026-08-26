"""Airflow DAG for the demo - Option 2 of the three ways to run the demo
(see ../README.md). Waits for the sample file in GCS, loads Bronze, then
runs the same post-ingest pipeline (Dataplex scan -> quarantine export ->
Silver -> Gold -> catalog) used by run_demo.py and pubsub_listener.py -
mirroring the main framework's validate_ingest_dag.py, at one-table scale.

Two ways to run this DAG:

  1. Locally in Cloud Shell, no Composer cost (good for a quick test):
       pip install "apache-airflow==2.9.3"
       export AIRFLOW_HOME=~/airflow_demo
       airflow standalone &   # prints the admin password on first run; UI on :8080
       mkdir -p $AIRFLOW_HOME/dags
       cp demo/airflow_dags/demo_orders_dag.py $AIRFLOW_HOME/dags/
       cp -r demo/pipeline $AIRFLOW_HOME/dags/
       airflow variables set gcp_project $PROJECT_ID
       airflow variables set dq_demo_raw_bucket $RAW_BUCKET
       airflow variables set dq_demo_quarantine_bucket $QUARANTINE_BUCKET
       airflow dags trigger demo_orders_bronze_silver_gold
       # or use the UI at http://localhost:8080

  2. Deployed to a real Cloud Composer environment - see DEPLOYMENT.md.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor

from pipeline.loader import load_orders
from pipeline.post_ingest import run_post_ingest, DATASET, BRONZE_TABLE

PROJECT = "{{ var.value.gcp_project }}"
RAW_BUCKET = "{{ var.value.dq_demo_raw_bucket }}"
QUARANTINE_BUCKET = "{{ var.value.dq_demo_quarantine_bucket }}"
LOCATION = "us-central1"
ENTRY_GROUP = "dq-demo-group"

default_args = {
    "owner": "data-eng-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _load_bronze(**_):
    load_orders(PROJECT, RAW_BUCKET, "raw/orders/", DATASET, BRONZE_TABLE)


def _post_ingest(**_):
    run_post_ingest(PROJECT, LOCATION, QUARANTINE_BUCKET, ENTRY_GROUP)


with DAG(
    dag_id="demo_orders_bronze_silver_gold",
    default_args=default_args,
    schedule_interval=None,  # trigger manually for the demo
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["dq_agent", "demo"],
) as dag:

    wait_for_sample_file = GCSObjectExistenceSensor(
        task_id="wait_for_sample_file",
        bucket=RAW_BUCKET,
        object="raw/orders/sample_orders.parquet",
        mode="reschedule",
        timeout=600,
    )

    load_bronze = PythonOperator(
        task_id="load_bronze",
        python_callable=_load_bronze,
    )

    validate_quarantine_silver_gold_catalog = PythonOperator(
        task_id="validate_quarantine_silver_gold_catalog",
        python_callable=_post_ingest,
    )

    wait_for_sample_file >> load_bronze >> validate_quarantine_silver_gold_catalog
