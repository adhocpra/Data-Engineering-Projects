# ETL Pipeline — Financial Transactions

A production-style batch ETL/ELT pipeline in Python and SQL. It ingests raw
financial transactions, validates them at the gate, transforms them into an
analytics-ready table, and loads them into a warehouse (SQLite here, same SQL
runs on Postgres).

This is Project 1 in a series proving mid-level data engineering skills:
ETL/ELT, data quality, and auditable, reproducible pipelines.

## What it does

1. **Extract** — reads `data/transactions.csv` (raw source, 100,000 synthetic
   transactions, 24 months).
2. **Validate** — fails the load, doesn't warn and carry on:
   - Completeness: no null `transaction_id`, `account_id`, `amount`, `posted_date`
   - Uniqueness: `transaction_id` is a real key, no duplicates
   - Validity: `amount > 0`, `currency` in an allowed list, `posted_date` in range
   - Freshness: the newest record is no more than 7 days old vs. the run date
3. **Transform** — normalises currency to USD, derives `month`, flags
   high-value transactions (`amount_usd >= 10000`), drops exact duplicates.
4. **Load** — writes `transactions_clean` to `warehouse.db` (SQLite), plus a
   monthly summary table `monthly_summary`.

## Run it

```bash
pip install -r requirements.txt
python etl.py
```

Expected output: `Loaded 100000 rows into transactions_clean` (or fewer if the
source has bad rows — they are quarantined to `data/quarantine.csv` and the
run fails loudly instead of silently loading bad data).

Verified local run (2026-11-16): 100,000 rows loaded, 4/4 quality checks
passing, 24 months of history, $683,018,592.38 total, 18,038 high-value
transactions. `pytest tests/` — 9 passed.

## Tests

`tests/test_etl.py` covers the validation gate (nulls, duplicates, negative
amounts, bad currency, stale data all fail the load) and the transform
(currency conversion to USD, high-value flag, month derivation). Every push
runs `python etl.py` and `pytest` in GitHub Actions (`.github/workflows/ci.yml`
at the repo root).

## Files

- `etl.py` — the pipeline
- `schema.sql` — warehouse tables
- `data/transactions.csv` — raw source data (generated, synthetic, 100,000 rows)
- `scripts/generate_data.py` — regenerates the source data
- `tests/test_etl.py` — pytest coverage for validation and transform
- `requirements.txt` — pandas, pytest

## Why this matters

This is the same pattern used at Capital One and Kaiser Permanente:
ingest from multiple sources, check at the gate, transform once, load once.
A pipeline that stops on bad data is annoying for an hour. A dashboard built
on bad data is wrong for a month.
