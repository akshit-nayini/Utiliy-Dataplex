"""Turns a Dataplex Auto DQ scan job's results into a visible, physical
bad-records artifact: a Parquet file in GCS, registered as a Fileset entry
in Data Catalog (Dataplex Catalog / Knowledge Catalog is the same
underlying service).

Dataplex itself never removes rows - it only evaluates each rule and, for
every failed rule, exposes an auto-generated `failing_rows_query`: a SQL
query that returns the actual non-conforming rows from the source table.
This module runs those queries, unions the results into one table tagged
with which rule each row failed, extracts that table to Parquet in GCS,
and registers the file in the catalog with a DQ breakdown - all read-only
against the staging table, so nothing here removes data from it either.
Everything stays ingested; this is purely the visibility/reporting layer.
"""
import logging
from typing import Dict, List

from google.cloud import bigquery
from google.cloud import dataplex_v1
from google.cloud import datacatalog_v1

logger = logging.getLogger(__name__)


def get_scan_job_rule_results(project: str, location: str, data_scan_id: str, job_id: str) -> List[Dict]:
    """Returns one dict per rule evaluated in this scan job: column, dimension,
    rule name, evaluated/passed counts, and failing_rows_query (empty string
    if the rule fully passed or Dataplex didn't generate one for it).
    """
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
    source: str,
    job_id: str,
    rule_results: List[Dict],
    gcs_bucket: str,
) -> Dict:
    """Unions every failing rule's failing_rows_query into one temp table
    (each row tagged with which rule/dimension it failed), extracts that
    table to Parquet in GCS, then drops the temp table. Returns the GCS URI
    pattern, total row count, and a breakdown by rule - or a no-op result
    if nothing failed.
    """
    failing_rules = [r for r in rule_results if r["failing_row_count"] > 0 and r["failing_rows_query"]]
    if not failing_rules:
        logger.info("No failing rules with data for %s job %s - nothing to export", source, job_id)
        return {"gcs_uri": None, "total_rows": 0, "breakdown": []}

    client = bigquery.Client(project=project)
    temp_table = f"{project}.{admin_dataset}.quarantine_export_{source}_{job_id}"

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

    gcs_uri_prefix = f"gs://{gcs_bucket}/quarantine/{source}/{job_id}"
    extract_job = client.extract_table(
        temp_table,
        destination_uris=[f"{gcs_uri_prefix}/part-*.parquet"],
        job_config=bigquery.ExtractJobConfig(destination_format=bigquery.DestinationFormat.PARQUET),
    )
    extract_job.result()
    client.delete_table(temp_table, not_found_ok=True)

    logger.info("Exported %d bad records for %s job %s to %s", total_rows, source, job_id, gcs_uri_prefix)
    return {"gcs_uri": f"{gcs_uri_prefix}/*.parquet", "total_rows": total_rows, "breakdown": breakdown}


def register_parquet_quarantine_in_catalog(
    project: str,
    catalog_cfg: Dict,
    source: str,
    export_result: Dict,
):
    """Registers (or refreshes) a Fileset entry in Data Catalog / Dataplex
    Catalog pointing at the exported Parquet file(s), with the DQ breakdown
    attached - so the bad records for this source are visible on the
    catalog dashboard next to the staging/consolidated table entries.
    """
    if not export_result.get("gcs_uri"):
        logger.info("Nothing exported for %s - skipping catalog registration", source)
        return

    client = datacatalog_v1.DataCatalogClient()
    location = catalog_cfg["location"]
    entry_group_id = catalog_cfg["entry_group"]
    entry_id = f"{source}_quarantine"

    parent = datacatalog_v1.DataCatalogClient.common_location_path(project, location)
    entry_group_name = datacatalog_v1.DataCatalogClient.entry_group_path(project, location, entry_group_id)
    try:
        client.get_entry_group(name=entry_group_name)
    except Exception:
        client.create_entry_group(parent=parent, entry_group_id=entry_group_id, entry_group={})

    entry = datacatalog_v1.types.Entry()
    entry.display_name = f"Utility Bills - {source} quarantine (bad records)"
    entry.type_ = datacatalog_v1.types.EntryType.FILESET
    entry.gcs_fileset_spec = datacatalog_v1.types.GcsFilesetSpec(
        file_patterns=[export_result["gcs_uri"]]
    )
    entry.description = (
        f"Dataplex DQ scan bad records for {source}: {export_result['total_rows']} rows. "
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
