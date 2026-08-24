"""Reads Dataplex Auto Data Quality scan results for reporting/alerting.

Dataplex runs the rules in dataplex/dq_scan_*.yaml against each staging
table and exports one row per (rule, column) to a BigQuery results table
(dq_agent_config.yaml: dataplex_scan.results_table). This module reads that
export for a given scan job and computes the invalid ratio/count against
the DQ agent's alert thresholds.

This is monitoring only - it never stops ingestion. Bad rows are always
routed to the quarantine table (pipeline/quarantine.py) and the rest of
the batch keeps processing regardless of how this evaluates. `alert=True`
just means the DQ agent should notify someone; the pipeline does not act
on it by blocking anything.
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
    """Applies alert_threshold / alert_count to scan results for reporting.

    Returns a decision dict: {'alert': bool, 'reason': str, 'failed_rules': [...]}
    `alert` never stops the pipeline - it's surfaced in dq_metrics / Data
    Catalog so the DQ agent owner can be notified.
    """
    alert_threshold = dq_agent_cfg.get('alert_threshold', dq_agent_cfg.get('enforce_block_threshold', 0.01))
    alert_count = dq_agent_cfg.get('alert_count', dq_agent_cfg.get('enforce_block_count', 100))

    total_evaluated = sum(r.get('evaluated_row_count', 0) for r in results) or 1
    total_failing = sum(r.get('failing_row_count', 0) for r in results)
    failed_rules = [r for r in results if not r.get('passed', True)]

    invalid_ratio = total_failing / total_evaluated
    alert = invalid_ratio > alert_threshold or total_failing > alert_count

    reason = None
    if alert:
        reason = (
            f"invalid_ratio={invalid_ratio:.4f} > threshold={alert_threshold} "
            f"or failing_rows={total_failing} > max={alert_count}"
        )
        logger.warning('Dataplex DQ ALERT (informational only, ingestion continues): %s', reason)
    else:
        logger.info(
            'Dataplex DQ within threshold: invalid_ratio=%.4f, failing_rows=%d',
            invalid_ratio, total_failing,
        )

    return {
        'alert': alert,
        'reason': reason,
        'invalid_ratio': invalid_ratio,
        'total_failing_rows': total_failing,
        'failed_rules': failed_rules,
    }


def check_scan_job(project: str, dq_agent_cfg: Dict, job_id: str) -> Dict:
    scan_cfg = dq_agent_cfg['dataplex_scan']
    results = fetch_scan_results(
        project, scan_cfg['results_dataset'], scan_cfg['results_table'], job_id
    )
    return evaluate(results, dq_agent_cfg)
