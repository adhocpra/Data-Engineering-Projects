# Data Engineering Projects

Real, mid-level data engineering projects by Pravat Adhikari — each one
built end to end, runnable, and demo-ready. Python, SQL, PySpark, Databricks,
Delta Lake, dbt, Airflow, AWS, Azure.

## Projects

### 01 — ETL Finance Pipeline
Python + SQL batch ETL/ELT pipeline. Ingests financial transactions,
validates them at the gate (completeness, uniqueness, validity, freshness),
transforms to USD, and loads an analytics-ready warehouse with a monthly
summary. Fails the load on bad data instead of warning and carrying on.

- Folder: `01-etl-finance-pipeline/`
- Run: `pip install -r requirements.txt && python etl.py`
- Verified: 100,000 rows loaded, 4/4 quality checks pass, 24 months, $683,018,592.38 total
- Tests: `pytest tests/` — 9 passed

### 02 — PySpark Medallion Pipeline
PySpark Bronze → Silver → Gold, reusing Project 1's transactions.
Delta Lake on Databricks; Parquet locally (see the project README for why).
Same 4 gate checks at Bronze and Silver — fail the load, don't warn and
carry on. Gold monthly summary matches Project 1 exactly.

- Folder: `02-pyspark-delta-medallion/`
- Run: `pip install -r requirements.txt && python medallion.py parquet`
- Verified: Bronze 100,000 rows, Silver 100,000 rows, Gold 24 months, Gold total $683,018,592.38

### 03 — dbt + Airflow Orchestration
Scheduled dbt models over 100,000 financial transactions, orchestrated by a
daily Airflow DAG. Staging → intermediate → marts (fact table + monthly
summary), with 11 generic tests and a singular test that the invalid-row
share stays below 5%. The dataset deliberately contains ~1.5% bad rows, so
the tests have something real to catch.

- Folder: `03-dbt-airflow/`
- Run: `python scripts/load_raw.py && cd dbt_project && dbt build --profiles-dir .`
- DAG: `dags/finance_pipeline_daily` — validated with Airflow 2.10.5 (`airflow dags list`)
- Verified: `dbt build` PASS=15, 98,032 clean rows, 24 months, $13,281,468.08 total

More projects coming, one at a time: healthcare claims, AWS warehouse,
Kafka streaming.

## CI

Every push runs Project 1 (`python etl.py` + `pytest`) and Project 3
(`dbt build`) in GitHub Actions — see `.github/workflows/ci.yml`.
