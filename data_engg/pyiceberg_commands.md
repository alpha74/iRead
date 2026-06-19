# PyIceberg — Practical Commands Reference

All commands use PyIceberg + Polaris REST catalog (your setup: self-hosted Docker).
Adjust `POLARIS_HOST` and credentials to match your environment.

---

## Install

```bash
pip install "pyiceberg[pyarrow,pandas,gcsfs]"
pip install duckdb   # optional, for SQL queries on Arrow data
```

---

## 1. Connect to Polaris

### Get OAuth Token

```python
import requests

POLARIS_HOST = "http://<your-polaris-host>:<port>"  # e.g. http://10.0.0.5:8181
CLIENT_ID = "<your-client-id>"
CLIENT_SECRET = "<your-client-secret>"

resp = requests.post(
    f"{POLARIS_HOST}/api/catalog/v1/oauth/tokens",
    data={
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "PRINCIPAL_ROLE:ALL",
    },
)
resp.raise_for_status()
token = resp.json()["access_token"]
print("Token acquired:", token[:20], "...")
```

> **If using Snowflake-managed Polaris (Open Catalog)**, replace host with:
> `https://<account>.snowflakecomputing.com`

---

### Load Catalog

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("polaris", **{
    "type": "rest",
    "uri": f"{POLARIS_HOST}/api/catalog",
    "token": token,
    "warehouse": "<warehouse-name>",   # the warehouse configured in Polaris
})

print("Catalog loaded:", catalog.name)
```

---

## 2. Browse the Catalog

### List All Namespaces

```python
namespaces = catalog.list_namespaces()
for ns in namespaces:
    print(ns)

# Output example:
# ('cdc_warehouse',)
# ('cdc_warehouse', 'toast')
```

### List Tables in a Namespace

```python
# Top-level namespace
tables = catalog.list_tables("cdc_warehouse")
for t in tables:
    print(t)

# Nested namespace (schema inside warehouse)
tables = catalog.list_tables(("cdc_warehouse", "toast"))
for t in tables:
    print(t)
```

### Load a Specific Table

```python
# Format: "namespace.table" or ("namespace", "schema", "table")
table = catalog.load_table("cdc_warehouse.toast.qa_metrics")
print(table)
```

---

## 3. Inspect Table Metadata

### Print Schema

```python
table = catalog.load_table("cdc_warehouse.toast.qa_metrics")

print(table.schema())

# Output example:
# table {
#   1: id: optional long
#   2: organization_id: optional long
#   3: score: optional double
#   ...
#   20: _cdc_hard_deleted: optional boolean
#   21: _cdc_ts_ms: optional long
#   22: _cdc_loaded_at: optional string
# }
```

### List All Column Names

```python
column_names = [field.name for field in table.schema().fields]
print(column_names)
```

### Print Partition Spec

```python
print(table.spec())
```

### Print Current Snapshot

```python
snap = table.current_snapshot()
print("Snapshot ID:", snap.snapshot_id)
print("Committed at:", snap.committed_at)
print("Summary:", snap.summary)
```

### Print Snapshot History

```python
for snap in table.history():
    print(f"  snapshot_id={snap.snapshot_id}  timestamp={snap.timestamp_ms}  parent={snap.parent_id}")
```

### List Data Files in Current Snapshot

```python
for task in table.scan().plan_files():
    print(task.file.file_path, "  rows:", task.file.record_count)
```

---

## 4. Scan Data

### Full Table Scan → Pandas

```python
df = table.scan().to_arrow().to_pandas()
print(df.shape)
print(df.head())
```

### Limit Rows

```python
df = table.scan(limit=100).to_arrow().to_pandas()
print(df)
```

### Select Specific Columns (drops the rest)

```python
df = table.scan(
    selected_fields=("id", "organization_id", "score", "created_at")
).to_arrow().to_pandas()
print(df.columns.tolist())
```

---

## 5. Filter Data

### Filter by Timestamp (Incremental Read)

```python
from pyiceberg.expressions import GreaterThanOrEqual

# Read only rows newer than a cursor (epoch milliseconds)
last_cursor_ms = 1700000000000   # replace with your stored cursor value

df = table.scan(
    row_filter=GreaterThanOrEqual("_cdc_ts_ms", last_cursor_ms),
).to_arrow().to_pandas()

print(f"New rows since cursor: {len(df)}")
```

### Filter Deleted Rows Out

```python
from pyiceberg.expressions import EqualTo

df = table.scan(
    row_filter=EqualTo("_cdc_hard_deleted", False),
).to_arrow().to_pandas()
```

### Combine Filters (Incremental + No Deletes)

```python
from pyiceberg.expressions import And, GreaterThanOrEqual, EqualTo

last_cursor_ms = 1700000000000

df = table.scan(
    row_filter=And(
        GreaterThanOrEqual("_cdc_ts_ms", last_cursor_ms),
        EqualTo("_cdc_hard_deleted", False),
    ),
    selected_fields=("id", "organization_id", "score", "created_at"),
).to_arrow().to_pandas()

print(df.shape)
```

### Filter by Organization ID

```python
from pyiceberg.expressions import EqualTo

df = table.scan(
    row_filter=EqualTo("organization_id", 42),
).to_arrow().to_pandas()
```

### Combine All Three (Incremental + No Deletes + Org Filter)

```python
from pyiceberg.expressions import And, GreaterThanOrEqual, EqualTo

last_cursor_ms = 1700000000000
org_id = 42

df = table.scan(
    row_filter=And(
        And(
            GreaterThanOrEqual("_cdc_ts_ms", last_cursor_ms),
            EqualTo("_cdc_hard_deleted", False),
        ),
        EqualTo("organization_id", org_id),
    ),
).to_arrow().to_pandas()
```

---

## 6. Drop CDC System Columns

Always strip `_cdc_*` columns before exporting — these are pipeline metadata, not business data.

```python
cdc_cols = [c for c in df.columns if c.startswith("_cdc_")]
df_clean = df.drop(columns=cdc_cols)
print("Remaining columns:", df_clean.columns.tolist())
```

---

## 7. Run SQL Queries with DuckDB

When you want to run SQL on the data without Spark.

### Basic Query

```python
import duckdb

arrow_table = table.scan().to_arrow()

con = duckdb.connect()
con.register("qa_metrics", arrow_table)

result = con.execute("""
    SELECT organization_id, COUNT(*) as row_count, AVG(score) as avg_score
    FROM qa_metrics
    WHERE _cdc_hard_deleted = false
    GROUP BY organization_id
    ORDER BY row_count DESC
""").df()

print(result)
```

### Incremental Query via DuckDB

```python
last_cursor_ms = 1700000000000

arrow_table = table.scan().to_arrow()   # or pre-filter with PyIceberg expressions
con = duckdb.connect()
con.register("qa_metrics", arrow_table)

result = con.execute(f"""
    SELECT * EXCLUDE (_cdc_uuid, _cdc_op, _cdc_ts, _cdc_ts_ms, _cdc_lsn,
                      _cdc_tx_id, _cdc_pks, _cdc_deleted, _cdc_hard_deleted,
                      _cdc_loaded_at, _cdc_source_file, _cdc_source_schema,
                      _cdc_source_table)
    FROM qa_metrics
    WHERE _cdc_ts_ms > {last_cursor_ms}
      AND _cdc_hard_deleted = false
""").df()

print(result.shape)
```

> **Tip:** Use PyIceberg `row_filter` for large tables to push filters down to file-level pruning,
> then use DuckDB for the final column shaping. Don't scan the full table into Arrow if you only need
> a small time window.

---

## 8. Write CSV to GCS

### Simple CSV Export

```python
# df is a pandas DataFrame with _cdc_* columns already dropped
output_path = "gs://your-bucket/exports/tenant/table/output.csv"

df_clean.to_csv(output_path, index=False)
print("Written to", output_path)
```

### Export with gcsfs (explicit credentials)

```python
import gcsfs
import pandas as pd

fs = gcsfs.GCSFileSystem(project="your-gcp-project")

output_path = "gs://your-bucket/exports/toast/qa_metrics/data.csv"

with fs.open(output_path, "w") as f:
    df_clean.to_csv(f, index=False)

print("Written:", output_path)
```

---

## 9. Cursor Management (Transfer Pointer Equivalent)

Read and write the last-processed timestamp to GCS so each run picks up where the previous left off.

```python
import json
import gcsfs

fs = gcsfs.GCSFileSystem(project="your-gcp-project")
CURSOR_PATH = "gs://your-bucket/cursors/{tenant}/{table}.json"


def read_cursor(tenant: str, table: str) -> int:
    path = CURSOR_PATH.format(tenant=tenant, table=table)
    try:
        with fs.open(path, "r") as f:
            return json.load(f)["last_cdc_ts_ms"]
    except FileNotFoundError:
        return 0   # first run — export everything


def write_cursor(tenant: str, table: str, new_cursor_ms: int):
    path = CURSOR_PATH.format(tenant=tenant, table=table)
    with fs.open(path, "w") as f:
        json.dump({"last_cdc_ts_ms": new_cursor_ms}, f)


# Usage
last_cursor = read_cursor("toast", "qa_metrics")

df = table.scan(
    row_filter=And(
        GreaterThanOrEqual("_cdc_ts_ms", last_cursor),
        EqualTo("_cdc_hard_deleted", False),
    )
).to_arrow().to_pandas()

if not df.empty:
    new_cursor = int(df["_cdc_ts_ms"].max())
    # ... export df_clean to CSV ...
    write_cursor("toast", "qa_metrics", new_cursor)
    print(f"Exported {len(df)} rows. New cursor: {new_cursor}")
else:
    print("No new rows.")
```

---

## 10. Full Export Script (End to End)

```python
import json
import requests
import gcsfs
import duckdb
from pyiceberg.catalog import load_catalog
from pyiceberg.expressions import And, GreaterThanOrEqual, EqualTo

# --- Config ---
POLARIS_HOST   = "http://<your-polaris-host>:<port>"
CLIENT_ID      = "<your-client-id>"
CLIENT_SECRET  = "<your-client-secret>"
WAREHOUSE      = "<warehouse-name>"
NAMESPACE      = "cdc_warehouse.toast"
TABLE_NAME     = "qa_metrics"
TENANT         = "toast"
GCS_PROJECT    = "your-gcp-project"
EXPORT_BUCKET  = "gs://your-export-bucket"
CURSOR_BUCKET  = "gs://your-cursor-bucket"

# --- Auth ---
resp = requests.post(
    f"{POLARIS_HOST}/api/catalog/v1/oauth/tokens",
    data={"grant_type": "client_credentials", "client_id": CLIENT_ID,
          "client_secret": CLIENT_SECRET, "scope": "PRINCIPAL_ROLE:ALL"},
)
token = resp.json()["access_token"]

# --- Catalog ---
catalog = load_catalog("polaris", **{
    "type": "rest",
    "uri": f"{POLARIS_HOST}/api/catalog",
    "token": token,
    "warehouse": WAREHOUSE,
})
table = catalog.load_table(f"{NAMESPACE}.{TABLE_NAME}")

# --- Read Cursor ---
fs = gcsfs.GCSFileSystem(project=GCS_PROJECT)
cursor_path = f"{CURSOR_BUCKET}/cursors/{TENANT}/{TABLE_NAME}.json"
try:
    with fs.open(cursor_path, "r") as f:
        last_cursor = json.load(f)["last_cdc_ts_ms"]
except FileNotFoundError:
    last_cursor = 0

print(f"Last cursor: {last_cursor}")

# --- Scan ---
arrow_data = table.scan(
    row_filter=And(
        GreaterThanOrEqual("_cdc_ts_ms", last_cursor),
        EqualTo("_cdc_hard_deleted", False),
    )
).to_arrow()

if arrow_data.num_rows == 0:
    print("No new rows. Exiting.")
    exit(0)

# --- Drop CDC columns via DuckDB ---
con = duckdb.connect()
con.register("source", arrow_data)
cdc_cols = [f.name for f in table.schema().fields if f.name.startswith("_cdc_")]
exclude_clause = ", ".join(cdc_cols)
df_clean = con.execute(f"SELECT * EXCLUDE ({exclude_clause}) FROM source").df()

# --- Export CSV ---
export_path = f"{EXPORT_BUCKET}/exports/{TENANT}/{TABLE_NAME}/data.csv"
with fs.open(export_path, "w") as f:
    df_clean.to_csv(f, index=False)

# --- Update Cursor ---
new_cursor = int(arrow_data.column("_cdc_ts_ms").to_pylist().__class__(
    arrow_data.column("_cdc_ts_ms").to_pylist()
)[-1])  # max _cdc_ts_ms
new_cursor = max(arrow_data.column("_cdc_ts_ms").to_pylist())

with fs.open(cursor_path, "w") as f:
    json.dump({"last_cdc_ts_ms": new_cursor}, f)

print(f"Exported {len(df_clean)} rows to {export_path}. New cursor: {new_cursor}")
```

---

## Quick Reference

| Task | Command |
|---|---|
| List namespaces | `catalog.list_namespaces()` |
| List tables | `catalog.list_tables("namespace")` |
| Load table | `catalog.load_table("namespace.table")` |
| Print schema | `table.schema()` |
| Full scan | `table.scan().to_arrow().to_pandas()` |
| Limit rows | `table.scan(limit=100).to_arrow().to_pandas()` |
| Filter rows | `table.scan(row_filter=EqualTo("col", val))` |
| Select columns | `table.scan(selected_fields=("col1", "col2"))` |
| Incremental filter | `GreaterThanOrEqual("_cdc_ts_ms", cursor_ms)` |
| Exclude deletes | `EqualTo("_cdc_hard_deleted", False)` |
| Run SQL | `duckdb.connect().register("t", arrow); con.execute("SELECT ...")` |
| Current snapshot | `table.current_snapshot()` |
| Snapshot history | `table.history()` |
| List data files | `table.scan().plan_files()` |
