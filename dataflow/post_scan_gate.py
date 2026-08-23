"""Follow-on step after the three Dataflow loads (customer, utility_details,
utility_bills): gate each source on its own Dataplex Auto DQ scan (the
utility_bills scan also carries the cross-table referential/consistency
sqlAssertion rules), then build the single consolidated output table and
register DQ metrics + lineage in Data Catalog (Knowledge Catalog).

Trigger from a Cloud Composer task or a Cloud Function subscribed to the
Dataflow job-completion Pub/Sub notification for all three jobs. Reuses the
same pipeline.dataplex_gate / pipeline.consolidate / pipeline.dq_reporting
modules as the Airflow DAG so both architectures apply identical
thresholds, joins, and catalog registration.

Usage:
  python -m dataflow.post_scan_gate --project=$PROJECT \
    --job_id customer=<scan_job_id> \
    --job_id utility_details=<scan_job_id> \
    --job_id utility_bills=<scan_job_id>
"""
import argparse
import logging

import yaml

from pipeline.dataplex_gate import gate_scan_job
from pipeline.consolidate import build_consolidated_table
from pipeline.dq_reporting import write_metrics_to_bq, register_dataset_in_datacatalog

logger = logging.getLogger(__name__)


def _parse_job_id(value):
    source, job_id = value.split("=", 1)
    return source, job_id


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument(
        "--job_id", action="append", type=_parse_job_id, required=True,
        help="source=data_scan_job_id, one per source (customer, utility_details, utility_bills)",
    )
    parser.add_argument("--config", default="pipeline/dq_agent_config.yaml")
    args = parser.parse_args()
    job_ids = dict(args.job_id)

    with open(args.config) as f:
        dq_agent_cfg = yaml.safe_load(f)["dq_agent"]

    sources = dq_agent_cfg["sources"]
    decisions = {}
    for source_name in sources:
        decision = gate_scan_job(args.project, dq_agent_cfg, job_ids[source_name])
        decisions[source_name] = decision
        if decision["block"]:
            logger.error("Ingestion stopped for %s by DQ agent: %s", source_name, decision["reason"])

    if any(d["block"] for d in decisions.values()):
        logger.error("Skipping consolidation - one or more sources failed their DQ gate")
        return

    consolidation = build_consolidated_table(
        args.project, dq_agent_cfg["staging_dataset"],
        customer_table=sources["customer"]["staging_table"],
        utility_details_table=sources["utility_details"]["staging_table"],
        bills_table=sources["utility_bills"]["staging_table"],
        output_table=dq_agent_cfg["consolidated_output_table"],
    )

    reporting_cfg = dq_agent_cfg["reporting"]
    catalog_cfg = dq_agent_cfg["knowledge_catalog"]
    bills_decision = decisions["utility_bills"]

    write_metrics_to_bq(
        args.project, reporting_cfg["bq_dataset"], reporting_cfg["bq_table"],
        {
            "dataset": f"{dq_agent_cfg['staging_dataset']}.{consolidation['output_table']}",
            "row_count": consolidation["row_count"],
            "cross_table_invalid_ratio": bills_decision["invalid_ratio"],
        },
    )
    register_dataset_in_datacatalog(
        args.project, catalog_cfg["location"], catalog_cfg["entry_group"],
        consolidation["output_table"],
        {
            "display_name": "Utility Bills - Consolidated",
            "linked_resource": f"//bigquery.googleapis.com/projects/{args.project}/datasets/{dq_agent_cfg['staging_dataset']}/tables/{consolidation['output_table']}",
            "dq_report": bills_decision,
            "row_count": consolidation["row_count"],
        },
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
