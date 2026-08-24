"""Runs the Dataplex Auto DQ scan, then turns its findings into a Parquet
file of bad records in GCS, registered as a Fileset entry in Data Catalog /
Dataplex Catalog. Same pattern as the main framework's
pipeline/dataplex_export.py, trimmed to one table and copied here so the
demo is fully self-contained.
"""
import logging
import time
from typing import Dict, List

from google.cloud import bigquery
from google.cloud import dataplex_v1
from google.cloud import datacatalog_v1

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
    job = client.get_data_scan_job(name=job_name, view=dataplex_v1.GetDataScanJobRequest.DataScanJobView.FULL)

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
    failing_rules = [r for r in rule_results if r["failing_row_count"] > 0 and r["failing_rows_query"]]
    if not failing_rules:
        logger.info("No failing rules with data for job %s - nothing to export", job_id)
        return {"gcs_uri": None, "total_rows": 0, "breakdown": []}

    client = bigquery.Client(project=project)
    temp_table = f"{project}.{admin_dataset}.quarantine_export_orders_{job_id}"

    union_parts = [
        f"""
        SELECT *, '{r['rule_name']}' AS failed_rule, '{r['dimension']}' AS dimension
        FROM ({r['failing_rows_query']})
        """
        for r in failing_rules
    ]
    client.query(f"CREATE OR REPLACE TABLE `{temp_table}` AS\n" + "\nUNION ALL\n".join(union_parts)).result()

    breakdown = [
        dict(row)
        for row in client.query(
            f"SELECT failed_rule, dimension, COUNT(*) AS row_count FROM `{temp_table}` GROUP BY 1, 2 ORDER BY row_count DESC"
        ).result()
    ]
    total_rows = sum(b["row_count"] for b in breakdown)

    gcs_uri_prefix = f"gs://{gcs_bucket}/quarantine/orders/{job_id}"
    extract_job = client.extract_table(
        temp_table,
        destination_uris=[f"{gcs_uri_prefix}/part-*.parquet"],
        job_config=bigquery.ExtractJobConfig(destination_format=bigquery.DestinationFormat.PARQUET),
    )
    extract_job.result()
    client.delete_table(temp_table, not_found_ok=True)

    logger.info("Exported %d bad records for job %s to %s", total_rows, job_id, gcs_uri_prefix)
    return {"gcs_uri": f"{gcs_uri_prefix}/*.parquet", "total_rows": total_rows, "breakdown": breakdown}


def register_parquet_quarantine_in_catalog(project: str, location: str, entry_group_id: str, export_result: Dict):
    if not export_result.get("gcs_uri"):
        logger.info("Nothing exported - skipping catalog registration")
        return

    client = datacatalog_v1.DataCatalogClient()
    parent = datacatalog_v1.DataCatalogClient.common_location_path(project, location)
    entry_group_name = datacatalog_v1.DataCatalogClient.entry_group_path(project, location, entry_group_id)
    try:
        client.get_entry_group(name=entry_group_name)
    except Exception:
        client.create_entry_group(parent=parent, entry_group_id=entry_group_id, entry_group={})

    entry_id = "orders_quarantine"
    entry = datacatalog_v1.types.Entry()
    entry.display_name = "Demo Orders - quarantine (bad records)"
    entry.type_ = datacatalog_v1.types.EntryType.FILESET
    entry.gcs_fileset_spec = datacatalog_v1.types.GcsFilesetSpec(file_patterns=[export_result["gcs_uri"]])
    entry.description = (
        f"Dataplex DQ scan bad records for demo orders: {export_result['total_rows']} rows. "
        f"Breakdown: {export_result['breakdown']}"
    )

    entry_name = client.entry_path(project, location, entry_group_id, entry_id)
    try:
        entry.name = entry_name
        client.update_entry(entry=entry)
        logger.info("Updated quarantine Fileset entry %s", entry_name)
    except Exception:
        client.create_entry(parent=entry_group_name, entry_id=entry_id, entry=entry)
        logger.info("Created quarantine Fileset entry %s", entry_name)
