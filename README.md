# Data Engineering projects
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

More projects coming, one at a time: PySpark + Delta Lake Medallion,
dbt + Airflow, healthcare claims, AWS warehouse, Kafka streaming.
