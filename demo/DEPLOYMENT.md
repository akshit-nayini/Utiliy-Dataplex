# Demo Deployment Guide

One-time setup, then three ways to actually run the demo (§6a/6b/6c) -
pick whichever matches what you want to test. Section numbering matches
the shared setup steps 0-5 and 7-9; only step 6 branches by option.
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
  pubsub.googleapis.com \
  dataflow.googleapis.com \
  composer.googleapis.com \
  --project=${PROJECT_ID}
```

(`pubsub`/`dataflow` are only needed for Option 1, `composer` only if you
deploy Option 2 to a real Composer environment rather than running it
locally - enabling all of them up front avoids switching back later.)

IAM: your user or service account needs `roles/bigquery.dataEditor`,
`roles/bigquery.jobUser`, `roles/storage.objectAdmin` (on both buckets),
`roles/dataplex.dataScanEditor`, and `roles/dataplex.catalogEditor` (create/
update entry groups and entries in Dataplex Universal Catalog / Knowledge
Catalog - the old `roles/datacatalog.entryGroupOwner` is for the deprecated
Data Catalog write API and won't work on projects where it's blocked). For
Option 1 also add `roles/pubsub.editor` and `roles/dataflow.developer`
(plus `roles/iam.serviceAccountUser` if running on the Dataflow service
under a non-default service account).

Authenticate for local Python calls (if running from a workstation rather
than Cloud Shell):

```bash
gcloud auth application-default login
```

## 1. Install dependencies

```bash
pip install -r demo/requirements.txt
```

This installs everything needed for Option 0 and Option 1 (including
`apache-beam[gcp]` and `google-cloud-pubsub`). Option 2 (Airflow) has its
own install step if running locally - see §6c.

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

## 6a. Run the demo - Option 0 (one process, synchronous)

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

## 6b. Run the demo - Option 1 (Dataflow + Pub/Sub)

Create the topic and subscription once:

```bash
gcloud pubsub topics create dq-demo-ingest-complete --project=${PROJECT_ID}
gcloud pubsub subscriptions create dq-demo-ingest-complete-sub \
  --topic=dq-demo-ingest-complete --project=${PROJECT_ID}
```

Start the listener **first**, in one terminal (or background it with `&`)
- it needs to already be subscribed before the Dataflow job publishes,
since Pub/Sub doesn't replay messages to a subscription that didn't exist
yet:

```bash
python demo/pubsub_listener.py --project=${PROJECT_ID} \
  --subscription=dq-demo-ingest-complete-sub --once \
  --quarantine_bucket=${QUARANTINE_BUCKET}
```

Then, in a second terminal, run the Beam ingestion job. `DirectRunner`
runs locally against your real GCP resources (fast, no Dataflow service
cost - good for a first test); `DataflowRunner` actually launches a
managed Dataflow job (takes a few minutes to spin up a worker):

```bash
# fast local test:
python demo/dataflow/beam_ingest.py --runner=DirectRunner --project=${PROJECT_ID} \
  --input=gs://${RAW_BUCKET}/raw/orders/sample_orders.parquet \
  --output_table=${PROJECT_ID}:dq_demo.orders_bronze \
  --topic=projects/${PROJECT_ID}/topics/dq-demo-ingest-complete

# actual Dataflow service:
python demo/dataflow/beam_ingest.py --runner=DataflowRunner --project=${PROJECT_ID} \
  --region=${REGION} --temp_location=gs://${RAW_BUCKET}/tmp \
  --input=gs://${RAW_BUCKET}/raw/orders/sample_orders.parquet \
  --output_table=${PROJECT_ID}:dq_demo.orders_bronze \
  --topic=projects/${PROJECT_ID}/topics/dq-demo-ingest-complete
```

Once the Beam job finishes and publishes, the listener in terminal 1 picks
up the message, runs the same validate/quarantine/Silver/Gold/catalog
pipeline as Option 0, prints the result, and exits (`--once`). Drop
`--once` to keep it running and process multiple ingestion jobs over time.

## 6c. Run the demo - Option 2 (Airflow)

**Locally in Cloud Shell** (no Composer cost, good for a quick test):

```bash
pip install "apache-airflow==2.9.3" "apache-airflow-providers-google"
export AIRFLOW_HOME=~/airflow_demo
airflow standalone &   # prints the admin password on first run; UI on :8080 (use `cloud shell web preview`)

mkdir -p $AIRFLOW_HOME/dags
cp demo/airflow_dags/demo_orders_dag.py $AIRFLOW_HOME/dags/
cp -r demo/pipeline $AIRFLOW_HOME/dags/

airflow variables set gcp_project ${PROJECT_ID}
airflow variables set dq_demo_raw_bucket ${RAW_BUCKET}
airflow variables set dq_demo_quarantine_bucket ${QUARANTINE_BUCKET}

airflow dags trigger demo_orders_bronze_silver_gold
airflow dags list-runs -d demo_orders_bronze_silver_gold   # check status
```

**On a real Cloud Composer environment:**

```bash
gcloud composer environments create dq-demo-composer \
  --project=${PROJECT_ID} --location=${REGION} --image-version=composer-2-airflow-2

gcloud composer environments run dq-demo-composer --location=${REGION} \
  variables set -- gcp_project ${PROJECT_ID}
gcloud composer environments run dq-demo-composer --location=${REGION} \
  variables set -- dq_demo_raw_bucket ${RAW_BUCKET}
gcloud composer environments run dq-demo-composer --location=${REGION} \
  variables set -- dq_demo_quarantine_bucket ${QUARANTINE_BUCKET}

DAGS_BUCKET=$(gcloud composer environments describe dq-demo-composer \
  --location=${REGION} --format="value(config.dagGcsPrefix)")
gsutil -m cp -r demo/airflow_dags/demo_orders_dag.py demo/pipeline ${DAGS_BUCKET}/

gcloud composer environments run dq-demo-composer --location=${REGION} \
  dags trigger -- demo_orders_bronze_silver_gold
```

Composer takes 20-25 minutes to provision if you don't already have an
environment - the local `airflow standalone` route is much faster for
just testing the DAG logic.

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

# if you did Option 1:
gcloud pubsub subscriptions delete dq-demo-ingest-complete-sub --project=${PROJECT_ID} --quiet
gcloud pubsub topics delete dq-demo-ingest-complete --project=${PROJECT_ID} --quiet

# if you deployed Option 2 to a real Composer environment:
gcloud composer environments delete dq-demo-composer --project=${PROJECT_ID} --location=${REGION} --quiet
# if you ran Option 2 locally instead, just remove the local Airflow home:
rm -rf ~/airflow_demo
```
