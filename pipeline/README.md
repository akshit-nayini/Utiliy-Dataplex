This folder contains a minimal ingestion + Dataplex-export pipeline,
shared by the Airflow DAG and the Dataflow pipeline, for three sources:
customer, utility_details, and utility_bills. All data is ingested as-is;
validation is entirely Dataplex's job, run against the staging tables
after load.

Usage
- Dataplex scan specs (the actual validation rules): `dataplex/dq_scan_customer.yaml`, `dq_scan_utility_details.yaml`, `dq_scan_utility_bills.yaml`.
- Configure Airflow DAG `airflow_dags/validate_ingest_dag.py` with project and raw bucket.
- Install requirements in Composer environment or a virtualenv: `pip install -r requirements.txt`.

Files
- `loader.py`: bulk-loads every row of every file under a source's GCS prefix into its (all-`STRING`) BigQuery staging table - nothing is filtered.
- `dataplex_gate.py`: reads a source's Dataplex Auto DQ scan results and computes an informational `alert` flag against the DQ agent's thresholds. Never blocks or stops anything - purely for reporting/notification.
- `dataplex_export.py`: pulls every failed rule's auto-generated `failing_rows_query` from a Dataplex scan job, unions the actual bad records into a kept BigQuery table `quarantine_<source>` (auto-cataloged with a Preview tab), extracts that table to a Parquet archival copy in GCS, and registers a catalog entry pointing at both (with a DQ breakdown by rule).
- `catalog.py`: shared helper for registering/refreshing entries in Dataplex Universal Catalog (Knowledge Catalog) via `dataplex_v1.CatalogServiceClient` - the current API; Data Catalog's older `datacatalog_v1` write API is deprecated and blocked on newer projects.
- `consolidate.py`: joins the three staging tables (customer, utility_details, utility_bills) into the single `utility_bills_consolidated` output table - an `INNER JOIN` plus a currency-match filter, mirroring the same cross-table conditions Dataplex's `sqlAssertion` rules check.
- `dq_reporting.py`: records DQ metrics to BigQuery and registers dataset metadata in Data Catalog / Knowledge Catalog (Dataplex integration).
- `dq_agent_config.yaml`: per-source alert thresholds, Dataplex DQ scan references, the quarantine GCS bucket, consolidated output table name, and Knowledge Catalog AI segregation (config-only).
- `validators.py`: kept as a reference/utility module for schema-driven rule definitions; not used for row filtering anymore (that logic now lives in `dataplex/dq_scan_*.yaml`).

See the top-level [README.md](../README.md) for the full Airflow vs. Dataflow architecture and the Dataplex scan specs under `dataplex/`.
