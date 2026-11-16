# dbt + Airflow Orchestration — Finance Pipeline

Scheduled dbt models over 100,000 financial transactions, orchestrated by a
daily Airflow DAG. This is the orchestration layer the first two projects
were missing: instead of running a script by hand, the pipeline is
scheduled, retried, and tested every day.

## Architecture

```
data/transactions.csv (100,000 rows, 24 months)
        |
        v  Airflow: load_raw_transactions (PythonOperator)
raw_transactions (DuckDB)
        |
        v  Airflow: dbt_build (BashOperator: dbt build)
stg_transactions      -- cast types, convert to USD, flag invalid rows
        |
        v
int_clean_transactions -- valid rows only, deduplicated
        |
        +--> fct_transactions   (fact table, 98,032 rows)
        +--> monthly_summary    (24 monthly aggregates)
```

## Models

| Model | Layer | What it does |
| --- | --- | --- |
| `stg_transactions` | staging | Casts types, converts amounts to USD, flags invalid rows (missing amount, non-positive amount, bad currency) with a reason code |
| `int_clean_transactions` | intermediate | Valid rows only, deduplicated by `transaction_id`, adds `transaction_month` and `is_high_value` (>= $1,000) |
| `fct_transactions` | mart | Fact table of clean transactions |
| `monthly_summary` | mart | Monthly transaction count, total and average USD, high-value count |

## Tests

11 generic tests (unique / not_null / accepted_values on the clean models)
plus 1 singular test: the share of invalid rows in staging must stay below
5%. The dataset deliberately contains ~1.5% bad rows, so the tests have
something real to catch.

## Verified local run (2026-11-16)

```
dbt build: PASS=15 WARN=0 ERROR=0
raw_transactions:        100,000 rows
staging invalid rows:      1,515  (531 bad currency, 495 missing amount, 489 non-positive)
fct_transactions:         98,032 rows
monthly_summary:              24 months
total amount (USD):   $13,281,468.08
high-value transactions:     882
```

## How to run

```bash
# 1. Create the environment
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 2. Load raw data and build dbt models + tests
python scripts/load_raw.py
cd dbt_project && dbt build --profiles-dir .

# 3. Or run the whole thing on a schedule with Airflow (Docker)
docker compose up -d
# open http://localhost:8080 (airflow / airflow) and trigger finance_pipeline_daily
```

The DAG is validated locally with Airflow 2.10.5 (`airflow dags list` shows
`finance_pipeline_daily`). The local warehouse is DuckDB — the same DAG and
dbt project point at a real warehouse by swapping the dbt profile.

## Stack

dbt (DuckDB adapter) · Apache Airflow · DuckDB · Python · SQL
