"""Generates demo/data/sample_orders.parquet: ~1000 order records, mixed
good and bad, to exercise every rule in dataplex/dq_scan_orders.yaml at a
realistic volume. Re-run this any time you want a fresh/different sample
(it's deterministic - same seed, same output - so re-runs are reproducible
unless you change NUM_ROWS or BAD_RATIO below).

All columns are written as Arrow `string` type - matching the all-STRING
Bronze convention (demo/pipeline/loader.py, dataplex/dq_scan_orders.yaml) -
so a value like amount="abc" or a negative amount can be represented at
all; a native Parquet FLOAT64/DATE column would reject those at write time
instead of letting Dataplex catch them after ingest.

Usage:
  pip install pyarrow
  python demo/data/generate_sample_orders.py
"""
import random

import pyarrow as pa
import pyarrow.parquet as pq

NUM_ROWS = 1000
BAD_RATIO = 0.15  # ~15% of rows get exactly one deliberate defect
SEED = 42

STATUSES = ["PENDING", "SHIPPED", "DELIVERED", "CANCELLED"]
BAD_STATUSES = ["IN_ORBIT", "RETURNED", "backordered", ""]
DOMAINS = ["example.com", "mail.com", "demo.org"]

random.seed(SEED)


def good_row(i):
    return {
        "order_id": f"ORD{i:06d}",
        "customer_email": f"user{i}@{random.choice(DOMAINS)}",
        "amount": f"{random.uniform(5, 500):.2f}",
        "status": random.choice(STATUSES),
        "order_date": f"2026-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
    }


DEFECTS = [
    "bad_email",
    "negative_amount",
    "non_numeric_amount",
    "bad_status",
    "bad_date_format",
    "missing_email",
    "bad_order_id_format",
    "duplicate_order_id",
]


def apply_defect(row, defect, all_order_ids):
    if defect == "bad_email":
        row["customer_email"] = "not-an-email"
    elif defect == "negative_amount":
        row["amount"] = f"-{random.uniform(1, 200):.2f}"
    elif defect == "non_numeric_amount":
        row["amount"] = "N/A"
    elif defect == "bad_status":
        row["status"] = random.choice(BAD_STATUSES)
    elif defect == "bad_date_format":
        row["order_date"] = "15-01-2026"
    elif defect == "missing_email":
        row["customer_email"] = ""
    elif defect == "bad_order_id_format":
        row["order_id"] = "ORDBAD" + row["order_id"][-3:]
    elif defect == "duplicate_order_id":
        row["order_id"] = random.choice(all_order_ids) if all_order_ids else row["order_id"]
    return row


def main():
    rows = [good_row(i) for i in range(1, NUM_ROWS + 1)]
    all_order_ids = [r["order_id"] for r in rows]

    num_bad = int(NUM_ROWS * BAD_RATIO)
    bad_indices = random.sample(range(NUM_ROWS), num_bad)
    defect_counts = {}
    for idx in bad_indices:
        defect = random.choice(DEFECTS)
        defect_counts[defect] = defect_counts.get(defect, 0) + 1
        rows[idx] = apply_defect(rows[idx], defect, all_order_ids)

    table = pa.table({
        "order_id": pa.array([r["order_id"] for r in rows], type=pa.string()),
        "customer_email": pa.array([r["customer_email"] for r in rows], type=pa.string()),
        "amount": pa.array([r["amount"] for r in rows], type=pa.string()),
        "status": pa.array([r["status"] for r in rows], type=pa.string()),
        "order_date": pa.array([r["order_date"] for r in rows], type=pa.string()),
    })

    out_path = __file__.rsplit("generate_sample_orders.py", 1)[0] + "sample_orders.parquet"
    pq.write_table(table, out_path)

    print(f"Wrote {NUM_ROWS} rows to {out_path}")
    print(f"Deliberately bad rows: {num_bad} ({BAD_RATIO:.0%})")
    print("Breakdown by defect type:")
    for defect, count in sorted(defect_counts.items(), key=lambda kv: -kv[1]):
        print(f"  {defect}: {count}")
    print(f"Clean rows: {NUM_ROWS - num_bad}")


if __name__ == "__main__":
    main()
