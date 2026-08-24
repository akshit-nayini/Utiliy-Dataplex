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
`roles/dataplex.dataScanEditor`, and `roles/dataplex.catalogEditor` (create/
update entry groups and entries in Dataplex Universal Catalog / Knowledge
Catalog - the old `roles/datacatalog.entryGroupOwner` is for the deprecated
Data Catalog write API and won't work on projects where it's blocked).

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

# dq_demo.dataplex_dq_results (Dataplex's own results export) and
# dq_demo.quarantine_orders (the bad-records table run_demo.py builds and
# keeps every run - browsable directly, and auto-cataloged by Dataplex
# Universal Catalog with a Preview tab) are both created automatically.
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

## 5. Create the catalog entry group

`gcloud data-catalog entry-groups create` is the deprecated Data Catalog
API and is blocked for write operations on newer projects
("INVALID_ARGUMENT: ... not allowed to perform write operations due to
Data Catalog deprecation"). Use the Dataplex Universal Catalog (now also
called Knowledge Catalog) equivalent instead - same underlying service,
current API:

```bash
gcloud dataplex entry-groups create dq-demo-group \
  --project=${PROJECT_ID} --location=${REGION}
```

This step is actually optional - `run_demo.py` creates the entry group
itself on first use if it doesn't already exist (`pipeline/catalog.py`).
Running it here just lets you confirm the command/permissions work before
the full script runs.

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

# the actual bad rows, browsable directly - this is what shows up in the
# catalog's Preview tab too:
bq query --use_legacy_sql=false \
  "SELECT failed_rule, dimension, * FROM \`${PROJECT_ID}.dq_demo.quarantine_orders\` LIMIT 20"

gsutil ls -r gs://${QUARANTINE_BUCKET}/quarantine/

# browse the catalog directly:
echo "Console: https://console.cloud.google.com/dataplex/governance/quality?project=${PROJECT_ID}"
echo "Console: https://console.cloud.google.com/dataplex/catalog?project=${PROJECT_ID}"
echo "Console: https://console.cloud.google.com/bigquery?project=${PROJECT_ID}&ws=!1m5!1m4!4m3!1s${PROJECT_ID}!2sdq_demo!3squarantine_orders"
```

You should see three entries under the `dq-demo-group` entry group:
`orders-bronze` (with rows-loaded/rows-quarantined in its description),
`orders-gold` (with its row count built from Silver), and
`orders-quarantine` (pointing at the Parquet file as its linked resource,
with the rule/dimension breakdown in its description). The
`dq_demo.quarantine_orders` **table** is separate from that entry and is
where the actual failed rows live - since it's a real BigQuery table (not
just a GCS file), it's auto-cataloged by Dataplex Universal Catalog with
its own schema and a data **Preview** tab, so you can browse the rows
themselves there, not just the summary description. Silver isn't
separately registered in this demo - it's an intermediate,
always-rebuildable layer; add a `register_table_in_catalog` call for it
too if you want it visible as well.

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
gcloud dataplex entry-groups delete dq-demo-group --project=${PROJECT_ID} --location=${REGION} --quiet
```
