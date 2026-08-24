"""Registers the demo's Bronze and Gold tables' own metadata/lineage in
the catalog (separate from the quarantine entry), and writes a DQ summary
row to BigQuery - so the demo also exercises catalog visibility for the
"good" tables, not just the bad-records one.
"""
import json
import logging
from typing import Dict

from google.cloud import bigquery

from .catalog import upsert_entry

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
    upsert_entry(
        project, location, entry_group_id, entry_id,
        display_name=metadata.get("display_name", entry_id),
        description=json.dumps(metadata),
        resource=metadata.get("linked_resource", ""),
    )
