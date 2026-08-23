import json
import logging
from typing import Dict

from google.cloud import bigquery
from google.cloud import datacatalog_v1

logger = logging.getLogger(__name__)


def write_metrics_to_bq(project: str, dataset: str, table: str, metrics: Dict):
    client = bigquery.Client(project=project)
    table_id = f"{client.project}.{dataset}.{table}"
    errors = client.insert_rows_json(table_id, [metrics])
    if errors:
        logger.error('Failed writing DQ metrics to BQ: %s', errors)
    else:
        logger.info('DQ metrics written to %s', table_id)


def register_dataset_in_datacatalog(project: str, location: str, entry_group_id: str, entry_id: str, metadata: Dict):
    # Create a Data Catalog entry (or update). This uses google-cloud-datacatalog.
    client = datacatalog_v1.DataCatalogClient()
    parent = datacatalog_v1.DataCatalogClient.common_location_path(project, location)
    entry_group_name = datacatalog_v1.DataCatalogClient.entry_group_path(project, location, entry_group_id)

    try:
        # Try to get existing entry group, else create
        client.get_entry_group(name=entry_group_name)
    except Exception:
        client.create_entry_group(parent=parent, entry_group_id=entry_group_id, entry_group={})

    entry = datacatalog_v1.types.Entry()
    entry.display_name = metadata.get('display_name', entry_id)
    entry.user_specified_system = 'utility_bills_dq_agent'
    entry.user_specified_type = 'dataset'
    entry.linked_resource = metadata.get('linked_resource', '')
    entry.schema = None
    entry.user_specified_system = 'dq_agent'

    entry_name = client.entry_path(project, location, entry_group_id, entry_id)
    try:
        client.update_entry(entry=entry)
        logger.info('Updated Data Catalog entry %s', entry_name)
    except Exception:
        # create
        client.create_entry(parent=entry_group_name, entry_id=entry_id, entry=entry)
        logger.info('Created Data Catalog entry %s', entry_name)

    # attach DQ metrics as system tags or tag templates could be used; for simplicity, write a JSON tag
    # Tag templates and attaching tags require additional setup; here we log the metadata as an entry's description via Entry.user_specified_system (limited)
    try:
        entry = client.get_entry(name=entry_name)
        patch = {'name': entry.name, 'description': json.dumps(metadata)}
        client.update_entry(entry=patch)
        logger.info('Attached DQ metadata to %s', entry_name)
    except Exception as exc:
        logger.error('Failed attaching metadata to Data Catalog entry: %s', exc)
