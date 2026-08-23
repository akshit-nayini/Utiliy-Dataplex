This folder contains a minimal validation pipeline, shared by the Airflow
DAG and the Dataflow pipeline, for three sources: customer,
utility_details, and utility_bills.

Usage
- Validation schemas: `schemas/customer_schema.yaml`, `schemas/utility_details_schema.yaml`, `schemas/utility_bill_schema.yaml`.
- Configure Airflow DAG `airflow_dags/validate_ingest_dag.py` with project, buckets, dataset/table.
- Install requirements in Composer environment or a virtualenv: `pip install -r requirements.txt`.

Files
- `validator.py`: lists GCS files, validates rows, writes valid rows to BigQuery and invalid rows to DLQ in GCS.
- `validators.py`: schema-driven per-column checks, shared with the Dataflow pipeline (`dataflow/beam_dq_pipeline.py`).
- `dq_reporting.py`: records DQ metrics to BigQuery and registers dataset metadata in Data Catalog / Knowledge Catalog (Dataplex integration).
- `dq_agent_config.yaml`: per-source thresholds, Dataplex DQ scan references, consolidated output table name, and Knowledge Catalog AI segregation (config-only).
- `dataplex_gate.py`: reads a source's Dataplex Auto DQ scan results and decides whether to promote or block ingestion based on the DQ agent's thresholds.
- `consolidate.py`: joins the three staging tables (customer, utility_details, utility_bills) into the single `utility_bills_consolidated` output table, enforcing referential integrity and currency consistency at the row level.

See the top-level [README.md](../README.md) for the full Airflow vs. Dataflow architecture and the Dataplex scan spec under `dataplex/`.
