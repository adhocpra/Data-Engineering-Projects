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
- Verified: 1,000 rows loaded, all quality checks pass

### 02 — PySpark Medallion Pipeline
PySpark Bronze → Silver → Gold, reusing Project 1's transactions.
Delta Lake on Databricks; Parquet locally (see the project README for why).
Same 4 gate checks at Bronze and Silver — fail the load, don't warn and
carry on. Gold monthly summary matches Project 1 exactly.

- Folder: `02-pyspark-delta-medallion/`
- Run: `pip install -r requirements.txt && python medallion.py parquet`
- Verified: Bronze 1,000 rows, Silver 1,000 rows, Gold 7 months, Gold total $8,454,814.56

More projects coming, one at a time: dbt + Airflow, healthcare claims,
AWS warehouse, Kafka streaming.
