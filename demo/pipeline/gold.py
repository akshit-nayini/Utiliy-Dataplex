"""Builds the Gold layer: a small business-level aggregate over Silver.

For a single-table demo there's nothing to consolidate (consolidation -
joining multiple sources - belongs at this same Gold layer once there's
more than one Silver table to bring together; see the main framework's
pipeline/consolidate.py, which is exactly that: a Gold-layer join across
three Silver-equivalent staging tables). Here, Gold is just order counts
and revenue by status - the kind of table a dashboard would query.
"""
import logging
from typing import Dict

from google.cloud import bigquery

logger = logging.getLogger(__name__)

GOLD_QUERY_TEMPLATE = """
CREATE OR REPLACE TABLE `{project}.{dataset}.{gold_table}` AS
SELECT
    status,
    COUNT(*) AS order_count,
    ROUND(SUM(amount), 2) AS total_revenue,
    ROUND(AVG(amount), 2) AS avg_order_value
FROM `{project}.{dataset}.{silver_table}`
GROUP BY status
"""


def build_gold_table(
    project: str,
    dataset: str,
    silver_table: str = "orders_silver",
    gold_table: str = "orders_gold",
) -> Dict:
    query = GOLD_QUERY_TEMPLATE.format(
        project=project, dataset=dataset, silver_table=silver_table, gold_table=gold_table,
    )

    client = bigquery.Client(project=project)
    client.query(query).result()

    table = client.get_table(f"{project}.{dataset}.{gold_table}")
    logger.info("Gold table %s built with %d rows", gold_table, table.num_rows)
    return {"table": gold_table, "row_count": table.num_rows}
