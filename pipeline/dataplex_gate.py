"""Gates ingestion on Dataplex Auto Data Quality scan results.

Dataplex runs the rules in dataplex/dq_scan_utility_bills.yaml against the
staging table and exports one row per (rule, column) to a BigQuery results
table (dq_agent_config.yaml: dataplex_scan.results_table). This module reads
that export for a given scan job and decides, using the DQ agent's
thresholds, whether the file's rows may be promoted to the curated table or
must be quarantined / ingestion stopped.
"""
import logging
from typing import Dict, List

from google.cloud import bigquery

logger = logging.getLogger(__name__)


def fetch_scan_results(project: str, results_dataset: str, results_table: str, job_id: str) -> List[Dict]:
    client = bigquery.Client(project=project)
    table_id = f"{client.project}.{results_dataset}.{results_table}"
    query = f"""
        SELECT column, dimension, passed, pass_ratio, evaluated_row_count, failing_row_count
        FROM `{table_id}`
        WHERE data_scan_job_id = @job_id
    """
    job = client.query(
        query,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("job_id", "STRING", job_id)]
        ),
    )
    return [dict(row) for row in job.result()]


def evaluate(results: List[Dict], dq_agent_cfg: Dict) -> Dict:
    """Applies enforce_block_threshold / enforce_block_count to scan results.

    Returns a decision dict: {'block': bool, 'reason': str, 'failed_rules': [...]}
    """
    block_threshold = dq_agent_cfg.get('enforce_block_threshold', 0.01)
    block_count = dq_agent_cfg.get('enforce_block_count', 100)

    total_evaluated = sum(r.get('evaluated_row_count', 0) for r in results) or 1
    total_failing = sum(r.get('failing_row_count', 0) for r in results)
    failed_rules = [r for r in results if not r.get('passed', True)]

    invalid_ratio = total_failing / total_evaluated
    block = invalid_ratio > block_threshold or total_failing > block_count

    reason = None
    if block:
        reason = (
            f"invalid_ratio={invalid_ratio:.4f} > threshold={block_threshold} "
            f"or failing_rows={total_failing} > max={block_count}"
        )
        logger.warning('Dataplex DQ gate BLOCKING ingestion: %s', reason)
    else:
        logger.info(
            'Dataplex DQ gate PASSED: invalid_ratio=%.4f, failing_rows=%d',
            invalid_ratio, total_failing,
        )

    return {
        'block': block,
        'reason': reason,
        'invalid_ratio': invalid_ratio,
        'total_failing_rows': total_failing,
        'failed_rules': failed_rules,
    }


def gate_scan_job(project: str, dq_agent_cfg: Dict, job_id: str) -> Dict:
    scan_cfg = dq_agent_cfg['dataplex_scan']
    results = fetch_scan_results(
        project, scan_cfg['results_dataset'], scan_cfg['results_table'], job_id
    )
    return evaluate(results, dq_agent_cfg)
