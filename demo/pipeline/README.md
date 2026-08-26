# Pipeline Library

The core data logic for the demo — how each table gets loaded and exactly
what queries run against it. **This folder has nothing to do with
Dataflow, Airflow, or Pub/Sub** — those are alternate *triggers* for this
same code, documented separately (`../dataflow/`, `../airflow_dags/`,
`../pubsub_listener.py`, and `../ARCHITECTURE.md`'s "three ways to run
it"). Everything below runs the same way regardless of which trigger
calls it — in practice, via `../run_demo.py`, which calls this code
directly in one process.

## Tables, in load order

### 1. `orders_bronze` — raw ingest

**Loaded by:** [`loader.py`](loader.py), `load_orders()`

**How:** a plain BigQuery **load job** reading the Parquet file straight
from GCS — no SQL, no filtering, nothing rejected:

```python
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    ignore_unknown_values=True,
)
client.load_table_from_uri(
    "gs://<raw_bucket>/raw/orders/*.parquet",
    "<project>.dq_demo.orders_bronze",
    job_config=job_config,
)
```

**Notes:** `WRITE_APPEND` — re-running adds another copy of the sample
rows (truncate manually for a clean slate; see `../DEPLOYMENT.md` §8).
The destination table is pre-created with an all-`STRING` schema, so
there's no type coercion at load time to fail on.

### 2. *(not a table)* — the Dataplex scan

**Triggered by:** [`dataplex_export.py`](dataplex_export.py),
`run_scan_and_wait()` and `get_scan_job_rule_results()`

**How:** no SQL here at all — these two functions call the Dataplex API
directly (`DataScanServiceClient.run_data_scan`, then poll
`get_data_scan_job` until it's done). The scan itself runs the rules in
`../dataplex/dq_scan_orders.yaml` against `orders_bronze`. What we get
back per rule is a pass/fail count and, for failed rules, a
`failing_rows_query` string.

For the one `sqlAssertion` rule (`amount-is-non-negative-number`), that
query is **exactly** the SQL we wrote in `dq_scan_orders.yaml` — Dataplex
just runs it as-is:

```sql
SELECT order_id FROM `<project>.dq_demo.orders_bronze`
WHERE SAFE_CAST(amount AS FLOAT64) IS NULL OR SAFE_CAST(amount AS FLOAT64) < 0
```

For the seven built-in rules (`nonNullExpectation`, `regexExpectation`,
`setExpectation`, `uniquenessExpectation`), Dataplex generates the
`failing_rows_query` internally — we don't author or control that SQL,
and its exact syntax isn't documented, so don't treat any specific text
for those as authoritative. To see the real query text for any rule on a
given run, print it: `get_scan_job_rule_results(...)` returns a
`failing_rows_query` field per rule, which is exactly what
`run_demo.py`'s "Rule results" step reports the pass/fail summary from.

Every downstream query in this pipeline (quarantine, Silver) is built
from these `failing_rows_query` strings, whatever they turn out to
contain — nothing re-implements the rule logic in Python, so there's
never a second, independently-maintained copy of any rule to drift out of
sync.

**Note:** this is a different table from `dataplex_dq_results` (the
BigQuery table Dataplex itself populates via `postScanActions.bigqueryExport`
in `dq_scan_orders.yaml`). `dataplex_dq_results` holds rule-level
aggregate stats only (pass/fail counts, scores) — no row-level data — and
nothing in this pipeline reads from it. Everything below is built from
`get_scan_job_rule_results()`'s in-memory result and `orders_bronze`
directly.

#### How good vs. bad is actually determined

Dataplex evaluates **per rule, not per row** — there's no table or API
response that says "row 42: PASS" or "row 42: FAIL". What comes back is
one summary per rule:

```python
[
  {"rule_name": "order-id-format", "failing_row_count": 25, "failing_rows_query": "SELECT ... WHERE ..."},
  {"rule_name": "amount-is-non-negative-number", "failing_row_count": 30, "failing_rows_query": "SELECT order_id FROM orders_bronze WHERE SAFE_CAST(amount AS FLOAT64) IS NULL OR ... < 0"},
  {"rule_name": "customer-email-not-null", "failing_row_count": 0, "failing_rows_query": ""},  # passed - nothing to chase
  ...
]
```

From that list, "good" and "bad" are two separate derivations, both built
from the *same* set of queries (the rules where `failing_row_count > 0`):

- **Bad** (§3 below, `quarantine_orders`) = the **union** of every failed
  rule's `failing_rows_query` results. A row can appear more than once,
  tagged per rule, if it fails multiple rules.
- **Good** (§4 below, `orders_silver`) = `orders_bronze` rows whose key
  appears in **none** of those same queries — a plain `NOT IN` filter
  over the identical query set.

Nothing else decides good/bad. Because both derivations read off the
exact same `failing_rows_query` strings rather than two independently
maintained rule sets, a row can never end up simultaneously "clean" in
Silver and "bad" in quarantine.

### 3. `quarantine_orders` — the bad rows

**Loaded by:** [`dataplex_export.py`](dataplex_export.py),
`export_bad_records_to_parquet()`

**How:** for every rule that failed, its `failing_rows_query` gets
wrapped and tagged, and all of them are `UNION ALL`-ed into one
`CREATE OR REPLACE TABLE` — overwritten fresh every run, not appended:

```sql
CREATE OR REPLACE TABLE `<project>.dq_demo.quarantine_orders` AS
SELECT *, 'order-id-format' AS failed_rule, 'VALIDITY' AS dimension, '<scan_job_id>' AS scan_job_id
FROM (
    SELECT order_id FROM `<project>.dq_demo.orders_bronze`
    WHERE NOT REGEXP_CONTAINS(order_id, r'^ORD[0-9]{6}$')
)
UNION ALL
SELECT *, 'amount-is-non-negative-number' AS failed_rule, 'VALIDITY' AS dimension, '<scan_job_id>' AS scan_job_id
FROM (
    SELECT order_id FROM `<project>.dq_demo.orders_bronze`
    WHERE SAFE_CAST(amount AS FLOAT64) IS NULL OR SAFE_CAST(amount AS FLOAT64) < 0
)
-- ... one UNION ALL branch per failed rule ...
```

(The actual `failing_rows_query` text is inserted exactly as Dataplex
returns it, with only its trailing `;` stripped — Dataplex always appends
one, which breaks once the query is wrapped inside `FROM (...)`.)

Then a summary query, and a `bq extract`-equivalent to Parquet:

```sql
SELECT failed_rule, dimension, COUNT(*) AS row_count
FROM `<project>.dq_demo.quarantine_orders`
GROUP BY 1, 2
ORDER BY row_count DESC
```

```python
client.extract_table(
    "quarantine_orders",
    destination_uris=["gs://<quarantine_bucket>/quarantine/orders/<job_id>/part-*.parquet"],
    job_config=bigquery.ExtractJobConfig(destination_format=bigquery.DestinationFormat.PARQUET),
)
```

**Why a real table and not just the Parquet file:** a native BigQuery
table is auto-cataloged by Dataplex Universal Catalog with a full schema
and a data **Preview** tab — the Parquet file alone only gets a text-only
catalog entry (see `catalog.py` below). Kept, not dropped, so it's always
queryable directly.

### 4. `orders_silver` — clean, typed rows

**Loaded by:** [`silver.py`](silver.py), `build_silver_table()`

**How:** one `CREATE OR REPLACE TABLE`, casting types and excluding every
key that showed up in *any* failed rule's `failing_rows_query`:

```sql
CREATE OR REPLACE TABLE `<project>.dq_demo.orders_silver` AS
SELECT
    order_id,
    customer_email,
    SAFE_CAST(amount AS FLOAT64) AS amount,
    status,
    SAFE_CAST(order_date AS DATE) AS order_date
FROM `<project>.dq_demo.orders_bronze`
WHERE order_id NOT IN (
    SELECT order_id FROM (
        SELECT order_id FROM `<project>.dq_demo.orders_bronze`
        WHERE NOT REGEXP_CONTAINS(order_id, r'^ORD[0-9]{6}$')
    )
    UNION ALL
    SELECT order_id FROM (
        SELECT order_id FROM `<project>.dq_demo.orders_bronze`
        WHERE SAFE_CAST(amount AS FLOAT64) IS NULL OR SAFE_CAST(amount AS FLOAT64) < 0
    )
    -- ... one UNION ALL branch per failed rule, same failing_rows_query
    --     text as quarantine_orders above, so the two are always in sync ...
)
```

**Why this is safe/consistent:** see "How good vs. bad is actually
determined" under §2 above — this `NOT IN` filter and `quarantine_orders`'s
`UNION ALL` are two views of the identical `failing_rows_query` set, so a
row can never end up "clean" here and "bad" there.

### 5. `orders_gold` — the business rollup

**Loaded by:** [`gold.py`](gold.py), `build_gold_table()`

**How:** a single aggregate query over Silver:

```sql
CREATE OR REPLACE TABLE `<project>.dq_demo.orders_gold` AS
SELECT
    status,
    COUNT(*) AS order_count,
    ROUND(SUM(amount), 2) AS total_revenue,
    ROUND(AVG(amount), 2) AS avg_order_value
FROM `<project>.dq_demo.orders_silver`
GROUP BY status
```

### 6. `dq_metrics` — one summary row per run

**Loaded by:** [`dq_reporting.py`](dq_reporting.py), `write_metrics_to_bq()`

**How:** not a query at all — a single `insert_rows_json` streaming
insert (appends, doesn't replace):

```python
client.insert_rows_json(
    "<project>.dq_demo.dq_metrics",
    [{"dataset": "dq_demo.orders_bronze", "rows_loaded": 1000, "rows_quarantined": 150}],
)
```

### 7. Catalog entries — not BigQuery tables at all

**Written by:** [`catalog.py`](catalog.py), `upsert_entry()` (called from
both `dataplex_export.py` and `dq_reporting.py`)

**How:** no SQL — these are Dataplex Universal Catalog API calls
(`CatalogServiceClient.create_entry_group`, `.create_entry`, or
`.update_entry` if the entry already exists), not BigQuery operations.
Three entries get written every run: `orders-bronze`, `orders-gold`, and
`orders-quarantine`, each carrying a text description (row counts, or the
quarantine breakdown) and a pointer to the relevant BigQuery table or GCS
Parquet path.

## Orchestration

[`post_ingest.py`](post_ingest.py)'s `run_post_ingest()` is the one
function that calls steps 2-7 above in order (it does *not* call
`loader.py` — that's always a separate, prior step, since it's the one
part of the flow that differs across the three trigger options this repo
supports elsewhere). It queries `orders_bronze`'s row count directly
rather than accepting it as an argument, specifically so this whole
function has no dependency on how Bronze got loaded.

```sql
SELECT COUNT(*) AS n FROM `<project>.dq_demo.orders_bronze`
```

See `../run_demo.py` for the full, single-process call sequence:
`loader.load_orders()` → `post_ingest.run_post_ingest()`.

## See also

- `../README.md` — quick start, the three ways to trigger this pipeline (including Dataflow/Airflow/Pub/Sub).
- `../ARCHITECTURE.md` — full component reference and system diagrams.
- `../DEPLOYMENT.md` — exact `gcloud`/`bq` setup commands.
