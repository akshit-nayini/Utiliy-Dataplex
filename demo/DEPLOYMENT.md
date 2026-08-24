# Demo Deployment Guide

One-time setup plus the single command to run the demo end-to-end. Takes
about 10-15 minutes including Dataplex scan creation propagation time.
Commands are `bash`/`gcloud` - translate to PowerShell if running directly
on Windows rather than Cloud Shell.

## 0. Prerequisites

```bash
export PROJECT_ID=<your-project-id>
export REGION=us-central1
export RAW_BUCKET=${PROJECT_ID}-dq-demo-raw
export QUARANTINE_BUCKET=${PROJECT_ID}-dq-demo-quarantine

gcloud services enable \
  bigquery.googleapis.com \
  storage.googleapis.com \
  dataplex.googleapis.com \
  datacatalog.googleapis.com \
  --project=${PROJECT_ID}
```

IAM: your user or service account needs `roles/bigquery.dataEditor`,
`roles/bigquery.jobUser`, `roles/storage.objectAdmin` (on both buckets),
`roles/dataplex.dataScanEditor`, and `roles/datacatalog.entryGroupOwner`.

Authenticate for local Python calls (if running `run_demo.py` from a
workstation rather than Cloud Shell):

```bash
gcloud auth application-default login
```

## 1. Install dependencies

```bash
pip install -r demo/requirements.txt
```

## 2. GCS buckets and sample data

`demo/data/sample_orders.parquet` (~1000 rows, ~15% deliberately bad) is
already checked in and ready to upload as-is. Only regenerate it if you
want a different size/mix - see `demo/data/generate_sample_orders.py`.

```bash
gsutil mb -l ${REGION} gs://${RAW_BUCKET}
gsutil mb -l ${REGION} gs://${QUARANTINE_BUCKET}

gsutil cp demo/data/sample_orders.parquet gs://${RAW_BUCKET}/raw/orders/sample_orders.parquet
```

## 3. BigQuery dataset and the Bronze table

Only Bronze needs to be pre-created. It's all-`STRING` on purpose - see the
main README for why (Dataplex validates content after load, so nothing can
fail the load itself). Silver and Gold are created automatically by
`demo/pipeline/silver.py` and `demo/pipeline/gold.py` on every run
(`CREATE OR REPLACE TABLE`).

```bash
bq mk --dataset --location=${REGION} ${PROJECT_ID}:dq_demo

bq mk --table ${PROJECT_ID}:dq_demo.orders_bronze \
  order_id:STRING,customer_email:STRING,amount:STRING,status:STRING,order_date:STRING

bq mk --table ${PROJECT_ID}:dq_demo.dq_metrics \
  dataset:STRING,rows_loaded:INTEGER,rows_quarantined:INTEGER

# dq_demo.dataplex_dq_results and the run's quarantine_export_orders_<job_id>
# temp table are both created automatically - no need to pre-create them.
```

## 4. Deploy the Dataplex scan

The scan runs against Bronze - Dataplex is what actually decides which
Bronze rows make it into Silver.

```bash
sed "s/\${PROJECT_ID}/${PROJECT_ID}/g" demo/dataplex/dq_scan_orders.yaml > /tmp/dq_scan_orders.yaml

gcloud dataplex datascans create data-quality orders-dq-scan \
  --project=${PROJECT_ID} --location=${REGION} \
  --data-source-resource=//bigquery.googleapis.com/projects/${PROJECT_ID}/datasets/dq_demo/tables/orders_bronze \
  --data-quality-spec-file=/tmp/dq_scan_orders.yaml
```

## 5. Create the Data Catalog entry group

```bash
gcloud data-catalog entry-groups create dq_demo_group \
  --project=${PROJECT_ID} --location=${REGION}
```

(Optional) enable Knowledge Catalog's AI-based metadata segregation on this
entry group from the console (Dataplex Universal Catalog → Governance →
Metadata enrichment → AI-generated metadata) to see that feature applied
to the demo entries too - it's a console toggle, no code change.

## 6. Run the demo

```bash
python demo/run_demo.py \
  --project=${PROJECT_ID} \
  --raw_bucket=${RAW_BUCKET} \
  --quarantine_bucket=${QUARANTINE_BUCKET}
```

Expected output (row counts may shift slightly if you regenerate the
sample with a different seed/ratio): a PASS/FAIL line per rule, then
Bronze/Silver/Gold row counts, ending with something like:

```
Bronze rows loaded:  1000
Rows quarantined:    150
Silver rows (clean): 850
Gold rows (by status): 4
Bad records Parquet: gs://<quarantine_bucket>/quarantine/orders/<job_id>/*.parquet
```

(~150 of the 1000 sample rows each fail exactly one rule - see the
breakdown table in demo/README.md - leaving ~850 clean rows in Silver,
grouped into 4 Gold rows, one per `status` value.)

## 7. Verify

```bash
bq query --use_legacy_sql=false \
  "SELECT * FROM \`${PROJECT_ID}.dq_demo.orders_bronze\` ORDER BY order_id"

bq query --use_legacy_sql=false \
  "SELECT * FROM \`${PROJECT_ID}.dq_demo.orders_silver\` ORDER BY order_id"

bq query --use_legacy_sql=false \
  "SELECT * FROM \`${PROJECT_ID}.dq_demo.orders_gold\` ORDER BY status"

bq query --use_legacy_sql=false \
  "SELECT * FROM \`${PROJECT_ID}.dq_demo.dq_metrics\` ORDER BY 1 DESC LIMIT 5"

gsutil ls -r gs://${QUARANTINE_BUCKET}/quarantine/

# browse the catalog directly:
echo "Console: https://console.cloud.google.com/dataplex/governance/quality?project=${PROJECT_ID}"
echo "Console: https://console.cloud.google.com/dataplex/catalog?project=${PROJECT_ID}"
```

You should see three entries under the `dq_demo_group` entry group:
`orders_bronze` (with rows-loaded/rows-quarantined in its description),
`orders_gold` (with its row count built from Silver), and
`orders_quarantine` (a Fileset entry pointing at the Parquet file, with the
rule/dimension breakdown in its description). Silver isn't separately
registered in this demo - it's an intermediate, always-rebuildable layer;
add a `register_table_in_catalog` call for it too if you want it visible
as well.

## 8. Re-running

`run_demo.py` is safe to re-run - the Bronze load is `WRITE_APPEND` (so
re-running adds another copy of the sample rows; truncate first with
`bq query "TRUNCATE TABLE ${PROJECT_ID}.dq_demo.orders_bronze"` for a clean
slate), the Dataplex scan always runs fresh, Silver and Gold are fully
rebuilt (`CREATE OR REPLACE`) every run, and the catalog entries are
updated in place, not duplicated.

## 9. Tear down

```bash
gsutil -m rm -r gs://${RAW_BUCKET}
gsutil -m rm -r gs://${QUARANTINE_BUCKET}
bq rm -r -f -d ${PROJECT_ID}:dq_demo
gcloud dataplex datascans delete orders-dq-scan --project=${PROJECT_ID} --location=${REGION} --quiet
gcloud data-catalog entry-groups delete dq_demo_group --project=${PROJECT_ID} --location=${REGION} --quiet
```
