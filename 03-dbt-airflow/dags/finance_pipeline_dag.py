"""Daily finance pipeline DAG: load raw CSV -> dbt build (models + tests).

Runs locally with the dbt-duckdb adapter (no warehouse server needed);
in production the same DAG points at a real warehouse by swapping the
dbt profile. Validated locally with Airflow 2.10.5 (``airflow dags list``).
"""
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))

import load_raw  # noqa: E402
from airflow import DAG  # noqa: E402
from airflow.operators.bash import BashOperator  # noqa: E402
from airflow.operators.python import PythonOperator  # noqa: E402

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DBT_DIR = os.path.join(BASE_DIR, "dbt_project")

default_args = {
    "owner": "pravat",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="finance_pipeline_daily",
    default_args=default_args,
    description="Load finance transactions and build dbt models + tests",
    schedule_interval="@daily",
    start_date=datetime(2026, 11, 16),
    catchup=False,
    tags=["finance", "dbt", "duckdb"],
) as dag:

    load_raw_task = PythonOperator(
        task_id="load_raw_transactions",
        python_callable=load_raw.main,
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {DBT_DIR} && dbt build --profiles-dir .",
    )

    load_raw_task >> dbt_build
