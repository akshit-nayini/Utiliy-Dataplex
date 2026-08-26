"""Runs the full demo pipeline end-to-end, synchronously, in one process -
the fastest way to smoke-test the whole Bronze -> Silver -> Gold flow.
This is Option 0 of three ways to run the demo (see README.md):

  0. This script: load Bronze directly, then call pipeline.post_ingest
     in-process. Fastest, simplest, good for a first end-to-end check.
  1. Dataflow + Pub/Sub: dataflow/beam_ingest.py loads Bronze as a Beam
     job and publishes a Pub/Sub notification; pubsub_listener.py picks
     it up and calls the same pipeline.post_ingest.
  2. Airflow: airflow_dags/demo_orders_dag.py runs the same two phases
     (load Bronze, then pipeline.post_ingest) as DAG tasks.

All three call the identical pipeline.loader / pipeline.post_ingest code -
only the orchestration/trigger differs, mirroring the main framework's
Airflow-vs-Dataflow split at demo scale, plus a Pub/Sub-driven variant.

Usage:
  python demo/run_demo.py --project=$PROJECT_ID \
    --raw_bucket=$RAW_BUCKET --quarantine_bucket=$QUARANTINE_BUCKET
"""
import argparse
import json
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pipeline.loader import load_orders
from pipeline.dataplex_export import get_scan_job_rule_results
from pipeline.post_ingest import run_post_ingest, DATASET, BRONZE_TABLE

logger = logging.getLogger(__name__)

LOCATION = "us-central1"
ENTRY_GROUP = "dq-demo-group"  # entry group / entry IDs allow only letters, numbers, hyphens


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--raw_bucket", required=True, help="Bucket holding demo/data/sample_orders.parquet under raw/orders/")
    parser.add_argument("--quarantine_bucket", required=True, help="Bucket to write the bad-records Parquet file to")
    parser.add_argument("--location", default=LOCATION)
    args = parser.parse_args()

    print("\n=== Step 1: Bronze - load every row of sample_orders.parquet into orders_bronze ===")
    load_result = load_orders(args.project, args.raw_bucket, "raw/orders/", DATASET, BRONZE_TABLE)
    print(json.dumps(load_result, indent=2))

    print("\n=== Steps 2-7: validate, quarantine, Silver, Gold, catalog ===")
    result = run_post_ingest(args.project, args.location, args.quarantine_bucket, ENTRY_GROUP)

    print("\n=== Rule results ===")
    rule_results = get_scan_job_rule_results(args.project, args.location, "orders-dq-scan", result["job_id"])
    for r in rule_results:
        status = "PASS" if r["failing_row_count"] == 0 else f"FAIL ({r['failing_row_count']} rows)"
        print(f"  [{status}] {r['rule_name']} (column={r['column']}, dimension={r['dimension']})")

    print("\n=== Done ===")
    print(f"Bronze rows loaded:  {result['bronze_rows']}")
    print(f"Rows quarantined:    {result['rows_quarantined']}")
    print(f"Silver rows (clean): {result['silver_rows']}")
    print(f"Gold rows (by status): {result['gold_rows']}")
    if result["gcs_uri"]:
        print(f"Bad records Parquet (archival): {result['gcs_uri']}")
        print(f"Bad records BigQuery table (browse rows here): {result['bigquery_quarantine_table']}")
    print(f"Check the Dataplex Universal Catalog console for entry group '{ENTRY_GROUP}' to see all entries.")
    print("Tip: the quarantine_orders table is auto-cataloged by BigQuery/Dataplex - open it in the")
    print("console's Preview tab to see the actual failed rows, not just the summary description.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
