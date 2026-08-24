"""Builds the Silver layer: bronze rows that passed every Dataplex rule,
with proper types instead of all-STRING.

Bad rows aren't deleted from Bronze - they stay there forever as the raw,
unmodified record of what was ingested. Silver is a derived view built by
excluding, via NOT IN, every key that appears in any failed rule's
failing_rows_query - the same Dataplex-generated queries
pipeline/dataplex_export.py uses to build the quarantine Parquet file. So
a row that's in the quarantine Parquet is guaranteed not to be in Silver,
and vice versa: Dataplex's scan result is the single source of truth for
both.
"""
import logging
from typing import Dict, List

from google.cloud import bigquery

logger = logging.getLogger(__name__)

SILVER_QUERY_TEMPLATE = """
CREATE OR REPLACE TABLE `{project}.{dataset}.{silver_table}` AS
SELECT
    order_id,
    customer_email,
    SAFE_CAST(amount AS FLOAT64) AS amount,
    status,
    SAFE_CAST(order_date AS DATE) AS order_date
FROM `{project}.{dataset}.{bronze_table}`
{where_clause}
"""


def build_silver_table(
    project: str,
    dataset: str,
    rule_results: List[Dict],
    bronze_table: str = "orders_bronze",
    silver_table: str = "orders_silver",
    key_column: str = "order_id",
) -> Dict:
    failing_rules = [r for r in rule_results if r["failing_row_count"] > 0 and r["failing_rows_query"]]

    where_clause = ""
    if failing_rules:
        bad_keys_union = "\nUNION ALL\n".join(
            f"SELECT {key_column} FROM ({r['failing_rows_query']})" for r in failing_rules
        )
        where_clause = f"WHERE {key_column} NOT IN (\n{bad_keys_union}\n)"

    query = SILVER_QUERY_TEMPLATE.format(
        project=project, dataset=dataset, bronze_table=bronze_table,
        silver_table=silver_table, where_clause=where_clause,
    )

    client = bigquery.Client(project=project)
    client.query(query).result()

    table = client.get_table(f"{project}.{dataset}.{silver_table}")
    logger.info("Silver table %s built with %d clean rows", silver_table, table.num_rows)
    return {"table": silver_table, "row_count": table.num_rows}
