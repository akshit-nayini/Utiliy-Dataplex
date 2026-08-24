import json
import logging
from typing import Dict

from google.cloud import bigquery

from .catalog import upsert_entry

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
    """Registers/refreshes an entry in Dataplex Universal Catalog (Knowledge
    Catalog). Named for historical continuity with the original Data
    Catalog-based implementation, but uses dataplex_v1.CatalogServiceClient -
    Data Catalog's write API is deprecated and blocked on newer projects.
    """
    # entry IDs allow only letters, numbers, and hyphens - no underscores
    upsert_entry(
        project, location, entry_group_id, entry_id.replace('_', '-'),
        display_name=metadata.get('display_name', entry_id),
        description=json.dumps(metadata, default=str),
        resource=metadata.get('linked_resource', ''),
    )
