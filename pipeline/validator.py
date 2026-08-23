import io
import json
import csv
import logging
from typing import List

from google.cloud import storage
from google.cloud import bigquery
import yaml

from .validators import apply_validations

logger = logging.getLogger(__name__)


def load_schema_from_gcs(bucket_name: str, schema_path: str) -> dict:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(schema_path)
    content = blob.download_as_text()
    return yaml.safe_load(content)


def list_files(bucket: str, prefix: str) -> List[str]:
    client = storage.Client()
    blobs = client.list_blobs(bucket, prefix=prefix)
    return [b.name for b in blobs if not b.name.endswith('/')]


def validate_prefix(project: str, bucket: str, prefix: str, schema_gcs_path: str, bq_dataset: str, bq_table: str, dlq_bucket: str):
    files = list_files(bucket, prefix)
    if not files:
        logger.info('No files found under prefix %s', prefix)
        return
    for f in files:
        validate_file(project, bucket, f, schema_gcs_path, bq_dataset, bq_table, dlq_bucket)


def validate_file(project: str, bucket: str, file_path: str, schema_gcs_path: str, bq_dataset: str, bq_table: str, dlq_bucket: str):
    storage_client = storage.Client()
    bq_client = bigquery.Client(project=project)

    # Assume schema_gcs_path is relative in same bucket or absolute like 'schemas/...'
    schema = load_schema_from_gcs(bucket, schema_gcs_path)

    blob = storage_client.bucket(bucket).blob(file_path)
    content = blob.download_as_text()
    reader = csv.DictReader(io.StringIO(content))

    valid_rows = []
    invalid_rows = []
    rownum = 0
    for row in reader:
        rownum += 1
        ok, errors = apply_validations(row, schema)
        if ok:
            valid_rows.append(row)
        else:
            invalid_rows.append({
                'row_number': rownum,
                'row': row,
                'errors': errors,
            })

    # Write valid rows to BigQuery
    if valid_rows:
        table_id = f"{bq_client.project}.{bq_dataset}.{bq_table}"
        errors = bq_client.insert_rows_json(table_id, valid_rows)
        if errors:
            logger.error('BigQuery insert errors: %s', errors)

    # Write invalid rows to DLQ bucket
    if invalid_rows:
        dlq_blob_name = f"dlq/{file_path}.invalid.jsonl"
        dlq_blob = storage_client.bucket(dlq_bucket).blob(dlq_blob_name)
        lines = '\n'.join(json.dumps(r) for r in invalid_rows)
        dlq_blob.upload_from_string(lines)

    # Write report
    report = {
        'file': file_path,
        'total_rows': rownum,
        'valid_rows': len(valid_rows),
        'invalid_rows': len(invalid_rows),
    }
    report_blob = storage_client.bucket(bucket).blob(f"reports/{file_path}.report.json")
    report_blob.upload_from_string(json.dumps(report))

    # Record DQ metrics and register metadata to Data Catalog / Dataplex via dq_reporting
    try:
        from .dq_reporting import write_metrics_to_bq, register_dataset_in_datacatalog
        # write metrics to admin BQ table (configurable)
        write_metrics_to_bq(project, 'dq_admin', 'dq_metrics', report)

        # Register dataset/metadata in Data Catalog (Knowledge Catalog integration)
        metadata = {
            'display_name': file_path,
            'linked_resource': f'//storage.googleapis.com/{bucket}/{file_path}',
            'dq_report': report,
        }
        register_dataset_in_datacatalog(project, 'us-central1', 'utility_bills_group', file_path.replace('/', '_'), metadata)
    except Exception as e:
        logger.warning('DQ reporting/registration failed: %s', e)

    logger.info('Validation completed for %s: %s', file_path, report)
