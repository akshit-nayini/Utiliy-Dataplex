# Deployment Guide

Concrete, ordered steps to stand up the framework described in
[README.md](README.md) in a GCP project. Commands are `bash`/`gcloud` —
translate to PowerShell (`` ` `` line continuation instead of `\`) if
running from Windows directly rather than Cloud Shell.

## 0. Prerequisites

Set your working variables once:

```bash
export PROJECT_ID=<your-project-id>
export REGION=us-central1
export RAW_BUCKET=${PROJECT_ID}-utility-bills-raw
export QUARANTINE_BUCKET=${PROJECT_ID}-utility-bills-quarantine
```

Enable the required APIs:

```bash
gcloud services enable \
  bigquery.googleapis.com \
  storage.googleapis.com \
  dataplex.googleapis.com \
  datacatalog.googleapis.com \
  composer.googleapis.com \
  dataflow.googleapis.com \
  --project=${PROJECT_ID}
```

IAM: the service account running the DAG/Dataflow jobs needs
`roles/bigquery.dataEditor`, `roles/bigquery.jobUser`,
`roles/storage.objectAdmin` (on the raw and quarantine buckets),
`roles/dataplex.dataScanEditor`, and `roles/datacatalog.entryGroupOwner`.

## 1. GCS buckets

```bash
gsutil mb -l ${REGION} gs://${RAW_BUCKET}
gsutil mb -l ${REGION} gs://${QUARANTINE_BUCKET}
```

Raw files land under `gs://${RAW_BUCKET}/raw/customer/`,
`raw/utility_details/`, and `raw/utility_bills/` respectively.
`pipeline/dataplex_export.py` writes bad-record Parquet files under
`gs://${QUARANTINE_BUCKET}/quarantine/<source>/<scan_job_id>/`.
`pipeline/dq_agent_config.yaml`'s `quarantine_gcs_bucket` must match
`${QUARANTINE_BUCKET}` (without the `gs://` prefix).

## 2. BigQuery datasets and tables

Staging tables are all-`STRING` on purpose: `pipeline/loader.py` /
`dataflow/beam_dq_pipeline.py` load every row as-is, so a malformed value
never fails the load - Dataplex validates content afterward.

```bash
bq mk --dataset --location=${REGION} ${PROJECT_ID}:utility_bills
bq mk --dataset --location=${REGION} ${PROJECT_ID}:dq_admin

bq mk --table ${PROJECT_ID}:utility_bills.customer_staging \
  customer_id:STRING,name:STRING,email:STRING,region:STRING,registered_currency:STRING

bq mk --table ${PROJECT_ID}:utility_bills.utility_details_staging \
  account_id:STRING,customer_id:STRING,utility_type:STRING,meter_id:STRING,region:STRING

bq mk --table ${PROJECT_ID}:utility_bills.staging \
  account_id:STRING,bill_date:STRING,amount:STRING,currency:STRING,meter_id:STRING

# utility_bills_consolidated is created/replaced by pipeline/consolidate.py
# on first run - no need to pre-create it.

bq mk --table ${PROJECT_ID}:dq_admin.dq_metrics \
  dataset:STRING,row_count:INTEGER,invalid_ratio:FLOAT,\
cross_table_invalid_ratio:FLOAT,failing_rows:INTEGER

# dq_admin.dataplex_dq_results is created automatically by Dataplex the
# first time a scan with a bigqueryExport postScanAction runs - no need to
# pre-create it, just make sure the dq_admin dataset (above) already exists.
# pipeline/dataplex_export.py also creates and drops its own temp tables
# (dq_admin.quarantine_export_<source>_<job_id>) per run - no setup needed.
```

## 3. Dataplex Auto DQ scans

Each spec file under `dataplex/` has its `gcloud dataplex datascans create`
command in its header comment. Substitute `${PROJECT_ID}` in the
`sqlAssertion` rules before deploying (the placeholder is literal text in
the YAML, not shell-expanded):

```bash
for f in dataplex/dq_scan_customer.yaml dataplex/dq_scan_utility_details.yaml dataplex/dq_scan_utility_bills.yaml; do
  sed "s/\${PROJECT_ID}/${PROJECT_ID}/g" "$f" > "/tmp/$(basename "$f")"
done

gcloud dataplex datascans create data-quality customer-dq-scan \
  --project=${PROJECT_ID} --location=${REGION} \
  --data-source-resource=//bigquery.googleapis.com/projects/${PROJECT_ID}/datasets/utility_bills/tables/customer_staging \
  --data-quality-spec-file=/tmp/dq_scan_customer.yaml

gcloud dataplex datascans create data-quality utility-details-dq-scan \
  --project=${PROJECT_ID} --location=${REGION} \
  --data-source-resource=//bigquery.googleapis.com/projects/${PROJECT_ID}/datasets/utility_bills/tables/utility_details_staging \
  --data-quality-spec-file=/tmp/dq_scan_utility_details.yaml

gcloud dataplex datascans create data-quality utility-bills-dq-scan \
  --project=${PROJECT_ID} --location=${REGION} \
  --data-source-resource=//bigquery.googleapis.com/projects/${PROJECT_ID}/datasets/utility_bills/tables/staging \
  --data-quality-spec-file=/tmp/dq_scan_utility_bills.yaml
```

These scan IDs must match `sources.*.data_scan_id` in
[pipeline/dq_agent_config.yaml](pipeline/dq_agent_config.yaml) — they already do,
out of the box. Each scan job's ID (`gcloud dataplex datascan-jobs list ...`
or the `job_id` an Airflow XCom/Dataflow trigger passes along) is what
`pipeline/dataplex_export.py` needs to pull that run's bad records.

## 4. Data Catalog (Knowledge Catalog)

```bash
gcloud data-catalog entry-groups create utility_bills_group \
  --project=${PROJECT_ID} --location=${REGION}

gcloud data-catalog tag-templates create dq_metrics_template \
  --project=${PROJECT_ID} --location=${REGION} \
  --field=id=invalid_ratio,type=double \
  --field=id=row_count,type=double
```

Enable Knowledge Catalog's AI-based metadata segregation on this entry
group from the console (Dataplex Universal Catalog → Governance →
Metadata enrichment → AI-generated metadata) — this is config only, no
code change needed on our side. Dataplex Catalog and Data Catalog /
Knowledge Catalog are the same underlying service (Dataplex Universal
Catalog), so `customer_staging`, `utility_bills_consolidated`, and each
source's `<source>_quarantine` Fileset entry all show up in the same
dashboard once registered.

## 5a. Cloud Composer (Airflow) path

```bash
# create a Composer environment if you don't already have one:
gcloud composer environments create utility-bills-composer \
  --project=${PROJECT_ID} --location=${REGION} --image-version=composer-2-airflow-2

# set the Airflow variables the DAG reads:
gcloud composer environments run utility-bills-composer \
  --location=${REGION} variables set -- gcp_project ${PROJECT_ID}
gcloud composer environments run utility-bills-composer \
  --location=${REGION} variables set -- utility_bills_raw_bucket ${RAW_BUCKET}

# upload the DAG and the shared pipeline package into the Composer DAGs bucket:
DAGS_BUCKET=$(gcloud composer environments describe utility-bills-composer \
  --location=${REGION} --format="value(config.dagGcsPrefix)")
gsutil -m cp -r airflow_dags/validate_ingest_dag.py pipeline schemas ${DAGS_BUCKET}/
```

Trigger it once manually to verify: `gcloud composer environments run
utility-bills-composer --location=${REGION} dags trigger --
validate_ingest_utility_bills`.

## 5b. Dataflow path

```bash
pip install -r requirements.txt

for SRC in customer utility_details utility_bills; do
  case $SRC in
    customer)        TABLE=customer_staging ;;
    utility_details) TABLE=utility_details_staging ;;
    utility_bills)   TABLE=staging ;;
  esac
  python -m dataflow.beam_dq_pipeline \
    --runner=DataflowRunner --project=${PROJECT_ID} --region=${REGION} \
    --temp_location=gs://${RAW_BUCKET}/tmp \
    --input="gs://${RAW_BUCKET}/raw/${SRC}/*.csv" \
    --output_table=${PROJECT_ID}:utility_bills.${TABLE}
done
```

After all three Dataflow jobs and their Dataplex scans finish, run the
export + consolidate step (wire this into a Cloud Function on the Dataflow
job-completion Pub/Sub topic, or a Composer task, for production). It
always runs consolidation - Dataplex scan alerts are logged for the DQ
agent owner but never skip this step:

```bash
python -m dataflow.post_scan_gate --project=${PROJECT_ID} \
  --job_id customer=<customer_scan_job_id> \
  --job_id utility_details=<utility_details_scan_job_id> \
  --job_id utility_bills=<utility_bills_scan_job_id>
```

## 6. Verify

```bash
bq query --use_legacy_sql=false \
  "SELECT * FROM \`${PROJECT_ID}.utility_bills.utility_bills_consolidated\` LIMIT 10"

bq query --use_legacy_sql=false \
  "SELECT * FROM \`${PROJECT_ID}.dq_admin.dq_metrics\` ORDER BY 1 DESC LIMIT 10"

gsutil ls -r gs://${QUARANTINE_BUCKET}/quarantine/

gcloud data-catalog entries lookup \
  "//bigquery.googleapis.com/projects/${PROJECT_ID}/datasets/utility_bills/tables/utility_bills_consolidated"

gcloud data-catalog entries lookup \
  "//bigquery.googleapis.com/projects/${PROJECT_ID}/datasets/utility_bills/utility_bills_group/entries/utility_bills_quarantine"
```

The catalog lookups (or the Dataplex Universal Catalog console) should
show a `<source>_quarantine` Fileset entry per source, pointing at its
Parquet file(s) in GCS, with a description containing the DQ breakdown by
rule and dimension — that's the bad-record visibility on the catalog
dashboard, refreshed on every scan run that finds something.

## Operational note: load order matters for consolidation

`pipeline/consolidate.py`'s join means a `utility_bills` row referencing an
`account_id`/`customer_id` that hasn't landed yet in `customer_staging` /
`utility_details_staging` won't appear in `utility_bills_consolidated`
until that master data lands — it's still in `staging` and still gets
scanned and reported by Dataplex, it just isn't joinable yet. Land
customer and utility_details files in the same batch as (or ahead of) the
bills that reference them to minimize this.
