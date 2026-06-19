# GCP Iceberg Data pipeline — Learning Path

Learning track for migrating Snowflake blob exports to GCP Spark + Iceberg.
Each stage builds on the previous — don't skip the hands-on checkpoints.

```
Iceberg concepts → PyIceberg → PySpark basics
    → Iceberg + Spark → Dataproc → Airflow
```

---

## Stage 1 — Apache Iceberg Concepts
**~2–3 hours · No code yet**

Understand what Iceberg is and what's physically on disk before touching any code.
You're already reading from Iceberg tables — this is about knowing *why* things work the way they do.

### Reading
| Resource | Focus |
|---|---|
| [Iceberg Docs — Introduction](https://iceberg.apache.org/docs/latest/) | What Iceberg solves vs plain Parquet — snapshots, time travel, schema evolution |
| [Iceberg Table Spec](https://iceberg.apache.org/spec/) | First two sections only — manifest files, data files, delete files |
| [Apache Iceberg 101 (Dremio)](https://www.dremio.com/blog/apache-iceberg-101-your-guide-to-learning-apache-iceberg-concepts-and-practices/) | Positions Iceberg relative to what you know from Snowflake |

### Videos
| Resource | Why |
|---|---|
| [Apache Iceberg Crash Course — Dremio (YouTube)](https://www.youtube.com/watch?v=MSuT20EqnnM&list=PL-gIUf9e9CCtGr_zYdWieJhiqBG_5qSPa) | Best conceptual intro, 45 min, covers snapshots + file layout visually |
| [Apache Iceberg in 20 min — Alex Merced (YouTube)](https://www.youtube.com/watch?v=SIriNcVIGJQ&list=PLsLAVBjQJO0p0Yq1fLkoHvt2lEJj5pcYe) | Quick overview, good for reinforcing concepts after the longer video |
| [Search: "Apache Iceberg table format explained"](https://www.youtube.com/results?search_query=apache+iceberg+table+format+explained) | Fallback if above links change |

### Checkpoint
Answer these before moving on:
- What is a snapshot in Iceberg?
- What is a position-delete file, and why does Snowflake need compaction before it can read Iceberg tables?
- What does merge-on-read mean vs copy-on-write?

---

## Stage 2 — PyIceberg: Read Iceberg Without Spark
**~2–3 hours · Hands-on**

Fastest path to touching real data. No cluster needed — runs from a plain VM or your laptop.
The quickstart doc from your team covers your specific Polaris + OAuth setup.

### Reading
| Resource | Focus |
|---|---|
| [PyIceberg Quickstart](https://py.iceberg.apache.org/getting-started/) | Catalog connection, `load_table`, basic scan |
| [PyIceberg Expressions](https://py.iceberg.apache.org/expressions/) | `GreaterThanOrEqual`, `And`, `EqualTo` — how to do incremental reads and filter deleted rows |
| [PyIceberg API Reference](https://py.iceberg.apache.org/api/) | `to_arrow()`, `to_pandas()`, `selected_fields` |
| [DuckDB + PyArrow](https://duckdb.org/docs/api/python/reference/) | Run SQL on top of a PyIceberg Arrow scan — useful for complex filters without Spark |

### Videos
| Resource | Why |
|---|---|
| [PyIceberg Tutorial — Dremio (YouTube)](https://www.youtube.com/watch?v=eGi_0mmw6c0) | Walks through catalog connect → scan → filter in Python |
| [Search: "PyIceberg tutorial"](https://www.youtube.com/results?search_query=pyiceberg+tutorial+polaris) | Smaller library, fewer videos — search if above changes |

### Checkpoint
Hands-on: Connect to your actual Polaris catalog, list tables, scan one table with a `_cdc_ts_ms` filter, and print to pandas.
**Don't move to Stage 3 until this works in your environment.**

---

## Stage 3 — PySpark Basics
**~3–4 hours**

You know SQL — Spark SQL is almost identical. The learning is about how jobs are structured
and how DataFrames work, not query syntax.

### Reading
| Resource | Focus |
|---|---|
| [PySpark Getting Started](https://spark.apache.org/docs/latest/api/python/getting_started/index.html) | SparkSession, reading data, DataFrames |
| [Spark SQL Programming Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html) | `spark.sql("SELECT ...")` — this is how you'll write most of your logic |
| [PySpark DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html) | `.filter()`, `.select()`, `.drop()`, `.write.csv()` |

> **Skip:** Spark Streaming, MLlib, GraphX — not relevant here.

### Videos
| Resource | Why |
|---|---|
| [PySpark Full Course — freeCodeCamp (YouTube)](https://www.youtube.com/watch?v=_C8kWso4ne4) | 2.5 hrs, well-rated, covers DataFrames + Spark SQL end to end |
| [PySpark Tutorial for Beginners — Programming with Mosh (YouTube)](https://www.youtube.com/watch?v=EB8lfdxpirM) | Shorter (~1 hr), faster-paced alternative |
| [Search: "PySpark tutorial for beginners"](https://www.youtube.com/results?search_query=pyspark+tutorial+beginners+2024) | Fallback |

### Checkpoint
Write a PySpark script locally that:
1. Reads a CSV or JSON file into a DataFrame
2. Filters some rows with `spark.sql(...)` or `.filter(...)`
3. Writes the result as CSV

---

## Stage 4 — Iceberg + Spark
**~2 hours**

Combine Stages 1–3: read Iceberg tables from Spark. This is what `pipeline.py` in your setup does.

### Reading
| Resource | Focus |
|---|---|
| [Iceberg Spark Getting Started](https://iceberg.apache.org/docs/latest/spark-getting-started/) | Catalog config (`spark.sql.catalog.*`), `spark.sql("SELECT * FROM catalog.db.table")` |
| [Iceberg Spark Queries](https://iceberg.apache.org/docs/latest/spark-queries/) | Incremental reads, time travel, snapshot-based reads — key for export logic |
| [Iceberg on GCS](https://iceberg.apache.org/docs/latest/gcs/) | How Spark talks to GCS-backed Iceberg tables |

### Videos
| Resource | Why |
|---|---|
| [Iceberg + Spark Hands-on — Dremio (YouTube)](https://www.youtube.com/watch?v=6yGcMRMlFZE) | Practical walkthrough of reading/writing Iceberg from Spark |
| [Apache Iceberg with Apache Spark — Seattle Spark Meetup](https://www.youtube.com/watch?v=nWwQMlrjhy0) | Real-world patterns, CDC-focused examples |
| [Search: "Apache Iceberg Spark tutorial"](https://www.youtube.com/results?search_query=apache+iceberg+spark+tutorial) | Fallback |

### Checkpoint
Submit a PySpark job (even locally via `spark-submit`) that:
1. Reads an Iceberg table using a catalog config pointing at Polaris
2. Filters rows where `_cdc_hard_deleted = false`
3. Drops all `_cdc_*` columns
4. Prints row count

**Focus on the catalog config** — this is the part that trips people up most.

---

## Stage 5 — Dataproc: Submitting PySpark Jobs on GCP
**~2 hours**

You already have a Dataproc cluster. This stage is about how to submit jobs to it
and understand the master/worker structure.

### Reading
| Resource | Focus |
|---|---|
| [Dataproc Overview](https://cloud.google.com/dataproc/docs/concepts/overview) | Master/worker nodes, YARN resource management |
| [Submit a PySpark Job to Dataproc](https://cloud.google.com/dataproc/docs/guides/submit-job) | Via Console first, then via gcloud CLI |
| [Dataproc Jobs API — Python client](https://cloud.google.com/dataproc/docs/reference/rest/v1/projects.regions.jobs/submit) | How the VM Poller in your setup submits jobs — same mechanism you'll use |
| [Passing args to PySpark jobs on Dataproc](https://cloud.google.com/dataproc/docs/guides/submit-job#dataproc-submit-job-python) | `args` in job spec → `argparse` in your script |

### Videos
| Resource | Why |
|---|---|
| [Google Cloud Dataproc Tutorial — Google Cloud Tech (YouTube)](https://www.youtube.com/watch?v=h1LvACJWjKc) | Official GCP channel, covers cluster + job submission end to end |
| [Running PySpark on Dataproc — GCP (YouTube)](https://www.youtube.com/watch?v=9mELEARcxJo) | More hands-on, walks through submitting a real PySpark job |
| [Search: "Google Dataproc PySpark tutorial"](https://www.youtube.com/results?search_query=google+dataproc+pyspark+tutorial) | Fallback |

### Checkpoint
Submit a simple PySpark script to your actual Dataproc cluster that reads one Iceberg table and writes CSV to GCS.
This proves your cluster config, IAM permissions, and GCS write access are all correct.

---

## Stage 6 — Apache Airflow: DAGs and Dataproc Operator
**~3 hours**

You already have Airflow running via the dbt-Spark project. This stage is about
structuring a DAG that submits Dataproc jobs on a schedule — look at existing DAGs
in your project as reference while reading.

### Reading
| Resource | Focus |
|---|---|
| [Airflow Core Concepts](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/index.html) | DAGs, Tasks, Operators, Schedule — understand these terms first |
| [Airflow DAG Writing Tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html) | How to write a DAG in Python |
| [DataprocSubmitJobOperator](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/dataproc.html) | The specific operator you'll use — maps directly to the Jobs API from Stage 5 |
| [Airflow Variables](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/variables.html) | How to pass tenant/pipeline config into a DAG at runtime |

### Videos
| Resource | Why |
|---|---|
| [Apache Airflow Full Course — Marc Lamberti (YouTube)](https://www.youtube.com/watch?v=K9AnJ9_ZAXE) | Best-rated Airflow educator, covers DAG structure + operators clearly |
| [Airflow in 10 Minutes — Marc Lamberti (YouTube)](https://www.youtube.com/watch?v=AHgxKGBqmgU) | Quick orientation before the full course |
| [Airflow + GCP Operators — Marc Lamberti (YouTube)](https://www.youtube.com/watch?v=0UepvC9X4HY) | Specifically covers GCP operators including Dataproc |
| [Search: "Apache Airflow Dataproc operator tutorial"](https://www.youtube.com/results?search_query=apache+airflow+dataproc+operator+tutorial) | Fallback |

### Checkpoint
Write a DAG that:
1. Submits your Stage 5 PySpark job to Dataproc using `DataprocSubmitJobOperator`
2. Runs on a CRON schedule
3. Has a second task that logs "done" after the job completes

---

## Recommended Order & Time Estimate

| Stage | Topic | Time |
|---|---|---|
| 1 | Iceberg Concepts | 2–3 hrs |
| 2 | PyIceberg (hands-on) | 2–3 hrs |
| 3 | PySpark Basics | 3–4 hrs |
| 4 | Iceberg + Spark | 2 hrs |
| 5 | Dataproc | 2 hrs |
| 6 | Airflow | 3 hrs |
| **Total** | | **~14–17 hrs** |

After Stage 6 you have everything needed to build the parameterized export job and wire it into Airflow.

---

## Quick Reference: Key Decisions in Your Setup

| Concept | Your Setup |
|---|---|
| Iceberg catalog | Apache Polaris (REST catalog, Docker + Postgres) |
| Auth to Polaris | OAuth2 `client_credentials` — token must be refreshed per run |
| Iceberg storage | GCS Bucket B (Parquet + position-delete files) |
| Spark cluster | Dataproc (always-on YARN, Spark 3.5) |
| Scheduler | Airflow (existing dbt-Spark project) |
| Deleted rows | Filter `_cdc_hard_deleted = false` |
| Incremental cursor | `_cdc_ts_ms` (epoch ms) or `_cdc_loaded_at` (ISO-8601) |
| System cols to exclude from CSV | All `_cdc_*` columns |
