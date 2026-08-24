"""Runs the Dataplex Auto DQ scan, then turns its findings into a Parquet
file of bad records in GCS, registered as an entry in Dataplex Universal
Catalog (aka Knowledge Catalog). Same pattern as the main framework's
pipeline/dataplex_export.py, trimmed to one table and copied here so the
demo is fully self-contained.
"""
import logging
import time
from typing import Dict, List

from google.cloud import bigquery
from google.cloud import dataplex_v1

from .catalog import upsert_entry

logger = logging.getLogger(__name__)


def run_scan_and_wait(project: str, location: str, data_scan_id: str, poll_seconds: int = 5, timeout_seconds: int = 600) -> str:
    """Triggers a run of the given Dataplex data scan and blocks until it
    finishes. Returns the scan job ID.
    """
    client = dataplex_v1.DataScanServiceClient()
    scan_name = client.data_scan_path(project, location, data_scan_id)

    run_response = client.run_data_scan(name=scan_name)
    job_id = run_response.job.name.split("/")[-1]
    logger.info("Started Dataplex scan job %s", job_id)

    job_name = client.data_scan_job_path(project, location, data_scan_id, job_id)
    waited = 0
    while waited < timeout_seconds:
        job = client.get_data_scan_job(name=job_name)
        if job.state in (
            dataplex_v1.DataScanJob.State.SUCCEEDED,
            dataplex_v1.DataScanJob.State.FAILED,
            dataplex_v1.DataScanJob.State.CANCELLED,
        ):
            logger.info("Dataplex scan job %s finished: %s", job_id, job.state.name)
            return job_id
        time.sleep(poll_seconds)
        waited += poll_seconds

    raise TimeoutError(f"Dataplex scan job {job_id} did not finish within {timeout_seconds}s")


def get_scan_job_rule_results(project: str, location: str, data_scan_id: str, job_id: str) -> List[Dict]:
    client = dataplex_v1.DataScanServiceClient()
    job_name = client.data_scan_job_path(project, location, data_scan_id, job_id)
    request = dataplex_v1.GetDataScanJobRequest(
        name=job_name, view=dataplex_v1.GetDataScanJobRequest.DataScanJobView.FULL
    )
    job = client.get_data_scan_job(request=request)

    results = []
    for rule in job.data_quality_result.rules:
        results.append({
            "rule_name": rule.rule.name or f"{rule.rule.column}_{rule.rule.dimension}",
            "column": rule.rule.column,
            "dimension": rule.rule.dimension,
            "evaluated_count": rule.evaluated_count,
            "passed_count": rule.passed_count,
            "failing_row_count": rule.evaluated_count - rule.passed_count,
            "failing_rows_query": rule.failing_rows_query,
        })
    return results


def export_bad_records_to_parquet(
    project: str,
    admin_dataset: str,
    job_id: str,
    rule_results: List[Dict],
    gcs_bucket: str,
) -> Dict:
    """Materializes every failing rule's bad records into a BigQuery table
    - kept (not dropped), so it shows up in the Dataplex Universal Catalog /
    Knowledge Catalog dashboard automatically (BigQuery tables are
    auto-cataloged with a full schema and a data Preview tab - no
    registration code needed for that part). The same table is then
    extracted to Parquet in GCS as a portable archival copy, and that
    Parquet file gets its own generic catalog entry (see
    register_parquet_quarantine_in_catalog) with a text summary.

    quarantine_orders is overwritten (CREATE OR REPLACE) on every run, so
    it always reflects the latest scan - open it directly in BigQuery or
    via the catalog's Preview tab to see the actual bad rows, not just
    counts.
    """
    failing_rules = [r for r in rule_results if r["failing_row_count"] > 0 and r["failing_rows_query"]]
    if not failing_rules:
        logger.info("No failing rules with data for job %s - nothing to export", job_id)
        return {"gcs_uri": None, "total_rows": 0, "breakdown": [], "bigquery_table": None}

    client = bigquery.Client(project=project)
    quarantine_table = f"{project}.{admin_dataset}.quarantine_orders"

    # Dataplex's auto-generated failing_rows_query ends with a trailing ";",
    # which breaks once wrapped in "FROM (...)" - strip it before embedding.
    union_parts = [
        f"""
        SELECT *, '{r['rule_name']}' AS failed_rule, '{r['dimension']}' AS dimension, '{job_id}' AS scan_job_id
        FROM ({r['failing_rows_query'].strip().rstrip(';')})
        """
        for r in failing_rules
    ]
    client.query(f"CREATE OR REPLACE TABLE `{quarantine_table}` AS\n" + "\nUNION ALL\n".join(union_parts)).result()

    breakdown = [
        dict(row)
        for row in client.query(
            f"SELECT failed_rule, dimension, COUNT(*) AS row_count FROM `{quarantine_table}` GROUP BY 1, 2 ORDER BY row_count DESC"
        ).result()
    ]
    total_rows = sum(b["row_count"] for b in breakdown)

    gcs_uri_prefix = f"gs://{gcs_bucket}/quarantine/orders/{job_id}"
    extract_job = client.extract_table(
        quarantine_table,
        destination_uris=[f"{gcs_uri_prefix}/part-*.parquet"],
        job_config=bigquery.ExtractJobConfig(destination_format=bigquery.DestinationFormat.PARQUET),
    )
    extract_job.result()

    logger.info("Exported %d bad records for job %s to %s and kept table %s", total_rows, job_id, gcs_uri_prefix, quarantine_table)
    return {
        "gcs_uri": f"{gcs_uri_prefix}/*.parquet",
        "total_rows": total_rows,
        "breakdown": breakdown,
        "bigquery_table": quarantine_table,
    }


def register_parquet_quarantine_in_catalog(project: str, location: str, entry_group_id: str, export_result: Dict):
    if not export_result.get("gcs_uri"):
        logger.info("Nothing exported - skipping catalog registration")
        return

    upsert_entry(
        project, location, entry_group_id, "orders-quarantine",
        display_name="Demo Orders - quarantine (bad records)",
        description=(
            f"Dataplex DQ scan bad records for demo orders: {export_result['total_rows']} rows. "
            f"Browse the actual rows in BigQuery table {export_result.get('bigquery_table')} "
            f"(auto-cataloged with a Preview tab) or the archival Parquet copy at "
            f"{export_result['gcs_uri']}. Breakdown: {export_result['breakdown']}"
        ),
        resource=export_result["gcs_uri"],
    )
