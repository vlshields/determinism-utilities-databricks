---
title: Demo Determinism
marimo-version: 0.22.4
---

```python {.marimo}
import marimo as mo
```

# Why `drop_duplicates()` Breaks Databricks Pipelines

In a medallion architecture, data flows from bronze to silver to gold.
Along the way, system columns are added to track **record identity** (`record_id`),
**record version** (`record_ver`), and **data provenance**.

These system columns are computed from the source rows that survive deduplication.
If deduplication is non-deterministic, the same logical record gets a **different identity**
on every pipeline run. Thus, downstream merge logic, duplicate checks, and time travel all break depending on how your data governance is set up.

```python {.marimo}
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (
    SparkSession.builder
    .master("local[4]")
    .appName("determinism-demo")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("OFF")
```

## Simulated Silver Source Tables

Imagine two silver tables, `LB` (lab results) and `VS` (vital signs), that a
listing notebook joins together. Each table has system columns
that were added when the data was pushed from bronze to silver.

The patient data is the same across duplicate rows, but `record_id` differs
because each row was ingested from a different source file or batch.
This is [how the pipeline tracks provenance](https://docs.databricks.com/aws/en/lakehouse/).

```python {.marimo}
# Silver LB (lab results) table: duplicate patient visits from different ingestion batches
lb_data = [
    ("P001", "2024-01-15", "POSITIVE", "abc123", "v1", True, False),
    ("P001", "2024-01-15", "POSITIVE", "def456", "v1", True, False),
    ("P001", "2024-01-15", "POSITIVE", "ghi789", "v1", True, False),
    ("P002", "2024-01-16", "NEGATIVE", "jkl012", "v1", True, False),
    ("P002", "2024-01-16", "NEGATIVE", "mno345", "v1", True, False),
    ("P003", "2024-02-01", "POSITIVE", "pqr678", "v1", True, False),
    ("P003", "2024-02-01", "POSITIVE", "stu901", "v1", True, False),
]

df_lb = spark.createDataFrame(
    lb_data,
    ["SUBJECT", "LBDAT", "LBORRES", "lb__record_id", "lb__record_ver", "lb__is_active", "lb__is_dropped"],
)
print("=== LB (Lab Results) | Silver Source ===")
df_lb.show(truncate=False)
```

```python {.marimo}
# Silver VS (vital signs) table: one record per patient, no duplicates here
vs_data = [
    ("P001", "2024-01-15", "120/80", "vs_aaa", "v1", True, False),
    ("P002", "2024-01-16", "130/85", "vs_bbb", "v1", True, False),
    ("P003", "2024-02-01", "118/76", "vs_ccc", "v1", True, False),
]

df_vs = spark.createDataFrame(
    vs_data,
    ["SUBJECT", "VSDAT", "VSORRES", "vs__record_id", "vs__record_ver", "vs__is_active", "vs__is_dropped"],
)
print("=== VS (Vital Signs) | Silver Source ===")
df_vs.show(truncate=False)
```

## The Listing Notebook Join

A listing notebook joins LB and VS on subject + date, then deduplicates.
This is the step where `drop_duplicates()` causes trouble.

```python {.marimo}
# Join LB and VS, as a listing notebook would
df_joined = df_lb.join(
    df_vs,
    (df_lb.SUBJECT == df_vs.SUBJECT) & (df_lb.LBDAT == df_vs.VSDAT),
    "inner",
).drop(df_vs.SUBJECT)

print("=== Joined Data (before dedup) ===")
df_joined.select("SUBJECT", "LBDAT", "LBORRES", "VSORRES", "lb__record_id", "vs__record_id").show(truncate=False)
```

## The Problem: `drop_duplicates()` on Relevant Patient Columns

We want one row per patient visit. The patient data (`SUBJECT`, `LBDAT`, `LBORRES`, `VSORRES`)
are identical across duplicates, but `lb__record_id` differs.

After dedup, the pipeline computes a new `record_id` from the surviving source
record IDs. **If a different source row survives, the derived record gets a different identity.**

Below we simulate multiple pipeline runs by repartitioning the data differently before
calling `drop_duplicates()`. Each repartition changes the physical row order within
partitions, which is the same thing that happens naturally in a distributed cluster between runs.

```python {.marimo}
patient_cols = ["SUBJECT", "LBDAT", "LBORRES", "VSORRES"]

# example provinance function
def add_provenance(df):
    return df.withColumn(
        "record_id",
        F.md5(F.to_json(F.array(
            F.struct(F.lit("LB").alias("table"), F.col("lb__record_id").alias("id")),
            F.struct(F.lit("VS").alias("table"), F.col("vs__record_id").alias("id")),
        )))
    )

# Run drop_duplicates with different partition layouts
# This is a way to simulate a cluster on your local machine
# Repartition will not be necessary in your actuall databricks env
# for testing
results_nondeterministic = []
for _n in [1, 2, 3, 5, 7, 8, 11, 13]:
    _deduped = (
        df_joined
        .repartition(_n)
        .dropDuplicates(patient_cols)
    )
    _with_prov = add_provenance(_deduped)
    _res = (
        _with_prov
        .select("SUBJECT", "record_id", "lb__record_id")
        .orderBy("SUBJECT")
        .collect()
    )
    results_nondeterministic.append(
        {row["SUBJECT"]: row["record_id"][:12] for row in _res}
    )

all_same = all(r == results_nondeterministic[0] for r in results_nondeterministic)
```

```python {.marimo hide_code="true"}
if all_same:
    _detail = (
        "On this run, `drop_duplicates()` happened to return the same result "
        "across all partition layouts. This can happen with small local datasets. "
        "The non-determinism is **probabilistic**, not guaranteed on every run.\n\n"
        "**That's exactly what makes it dangerous:** it passes testing, then breaks "
        "in production when data volume or cluster topology changes."
    )
else:
    _pairs = []
    for _i, _r in enumerate(results_nondeterministic):
        if _r != results_nondeterministic[0]:
            _pairs = [results_nondeterministic[0], _r]
            break
    _detail = (
        f"**Results differed!** Same data, same code, different partitioning:\n\n"
        f"- Run A `record_id`s: `{_pairs[0]}`\n"
        f"- Run B `record_id`s: `{_pairs[1]}`\n\n"
        f"The same logical patient record got **different identities** depending on "
        f"how Spark shuffled the data. Since the silver merge operates on `record_id`, "
        f"this means inserts instead of updates, duplicate active records, and "
        f"duplicate-check failures."
    )

_status = "SAME (this time)" if all_same else "DIFFERENT"

mo.md(f"""
### Result: `drop_duplicates()` across 8 partition layouts, record_id is **{_status}**

{_detail}
""")
```

## The Fix: `drop_deterministically()`

Instead of `drop_duplicates()`, use a window function with an explicit `ORDER BY`
on a column guaranteed to be unique (`lb__record_id` in this case). No matter how
the data is partitioned, the same source row wins every time, so the derived
`record_id` is stable across pipeline runs.

```python {.marimo}
import sys
sys.path.insert(0, "..")
from dtmns_utils import drop_deterministically, exclude_columns

window_cols = exclude_columns(df_joined, ["lb__record_id"])
print(f"Window columns: {window_cols}")
print(f"(matches business cols: {sorted(window_cols) == sorted(patient_cols)})")
```

```python {.marimo}
# Run drop_deterministically with different partition layouts
results_deterministic = []
for _np in [1, 2, 3, 5, 7, 8, 11, 13]:
    _deduped = drop_deterministically(
        df_joined.repartition(_np),
        _window=window_cols,
        order="lb__record_id",
        validate=True,  
    )
    _with_prov = add_provenance(_deduped)
    _rd = (
        _with_prov
        .select("SUBJECT", "record_id", "lb__record_id")
        .orderBy("SUBJECT")
        .collect()
    )
    results_deterministic.append(
        {row["SUBJECT"]: row["record_id"][:12] for row in _rd}
    )

all_same_det = all(r == results_deterministic[0] for r in results_deterministic)
```

```python {.marimo hide_code="true"}
_status_det = "SAME" if all_same_det else "DIFFERENT"

mo.md(f"""
### Result: `drop_deterministically()` across 8 partition layouts, record_id is **{_status_det}**

Every run produced: `{results_deterministic[0]}`

The record identity is stable because `row_number()` over an explicit `ORDER BY`
always picks the same source row, so the provenance step always computes the
same `record_id`. The silver merge works correctly, duplicate checks
pass, and Delta Lake time travel remains consistent.
""")
```

```python {.marimo}
spark.stop()
```