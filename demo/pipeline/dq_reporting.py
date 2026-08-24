"""Registers the demo's Bronze and Gold tables' own metadata/lineage in
Data Catalog (separate from the quarantine Fileset entry), and writes a DQ
summary row to BigQuery - so the demo also exercises catalog visibility
for the "good" tables, not just the bad-records one.
"""
import json
import logging
from typing import Dict

from google.cloud import bigquery
from google.cloud import datacatalog_v1

logger = logging.getLogger(__name__)


def write_metrics_to_bq(project: str, dataset: str, table: str, metrics: Dict):
    client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{table}"
    errors = client.insert_rows_json(table_id, [metrics])
    if errors:
        logger.error("Failed writing DQ metrics to BQ: %s", errors)
    else:
        logger.info("DQ metrics written to %s", table_id)


def register_table_in_catalog(project: str, location: str, entry_group_id: str, entry_id: str, metadata: Dict):
    client = datacatalog_v1.DataCatalogClient()
    parent = datacatalog_v1.DataCatalogClient.common_location_path(project, location)
    entry_group_name = datacatalog_v1.DataCatalogClient.entry_group_path(project, location, entry_group_id)

    try:
        client.get_entry_group(name=entry_group_name)
    except Exception:
        client.create_entry_group(parent=parent, entry_group_id=entry_group_id, entry_group={})

    entry = datacatalog_v1.types.Entry()
    entry.display_name = metadata.get("display_name", entry_id)
    entry.user_specified_system = "dq_agent_demo"
    entry.user_specified_type = "dataset"
    entry.linked_resource = metadata.get("linked_resource", "")

    entry_name = client.entry_path(project, location, entry_group_id, entry_id)
    try:
        client.get_entry(name=entry_name)
        patch = {"name": entry_name, "description": json.dumps(metadata)}
        client.update_entry(entry=patch)
        logger.info("Updated Data Catalog entry %s", entry_name)
    except Exception:
        entry.description = json.dumps(metadata)
        client.create_entry(parent=entry_group_name, entry_id=entry_id, entry=entry)
        logger.info("Created Data Catalog entry %s", entry_name)
