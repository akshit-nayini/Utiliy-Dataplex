"""Builds the single consolidated output table from the three validated
staging tables (customer, utility_details, utility_bills).

The join itself enforces the cross-table checks a second time at the row
level: an INNER JOIN drops any bill whose account has no utility_details
record, or whose utility_details has no matching customer (referential
integrity), and the WHERE clause drops any bill whose currency doesn't
match its customer's registered currency (attribute consistency). Rows
dropped here were already flagged by the Dataplex sqlAssertion rules in
dataplex/dq_scan_utility_bills.yaml; this just materializes the clean,
denormalized result the DQ agent promotes to.
"""
import logging

from google.cloud import bigquery

logger = logging.getLogger(__name__)

CONSOLIDATE_QUERY = """
CREATE OR REPLACE TABLE `{project}.{dataset}.{output_table}` AS
SELECT
    c.customer_id,
    c.name AS customer_name,
    c.email AS customer_email,
    c.region AS customer_region,
    c.registered_currency,
    ud.account_id,
    ud.utility_type,
    ud.meter_id,
    ud.region AS utility_region,
    b.bill_date,
    b.amount,
    b.currency
FROM `{project}.{dataset}.{bills_table}` b
JOIN `{project}.{dataset}.{utility_details_table}` ud ON ud.account_id = b.account_id
JOIN `{project}.{dataset}.{customer_table}` c ON c.customer_id = ud.customer_id
WHERE b.currency = c.registered_currency
"""


def build_consolidated_table(
    project: str,
    dataset: str,
    customer_table: str = "customer_staging",
    utility_details_table: str = "utility_details_staging",
    bills_table: str = "staging",
    output_table: str = "utility_bills_consolidated",
) -> dict:
    client = bigquery.Client(project=project)
    query = CONSOLIDATE_QUERY.format(
        project=project,
        dataset=dataset,
        customer_table=customer_table,
        utility_details_table=utility_details_table,
        bills_table=bills_table,
        output_table=output_table,
    )
    client.query(query).result()

    table = client.get_table(f"{project}.{dataset}.{output_table}")
    logger.info("Consolidated table %s built with %d rows", output_table, table.num_rows)
    return {"output_table": output_table, "row_count": table.num_rows}
