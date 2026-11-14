# ETL Pipeline — Financial Transactions

A production-style batch ETL/ELT pipeline in Python and SQL. It ingests raw
financial transactions, validates them at the gate, transforms them into an
analytics-ready table, and loads them into a warehouse (SQLite here, same SQL
runs on Postgres).

This is Project 1 in a series proving mid-level data engineering skills:
ETL/ELT, data quality, and auditable, reproducible pipelines.

## What it does

1. **Extract** — reads `data/transactions.csv` (raw source, 1,000 synthetic
   transactions, last 180 days).
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

Expected output: `Loaded 1000 rows into transactions_clean` (or fewer if the
source has bad rows — they are quarantined to `data/quarantine.csv` and the
run fails loudly instead of silently loading bad data).

## Files

- `etl.py` — the pipeline
- `schema.sql` — warehouse tables
- `data/transactions.csv` — raw source data (generated, synthetic)
- `requirements.txt` — pandas only

## Why this matters

This is the same pattern used at Capital One and Kaiser Permanente:
ingest from multiple sources, check at the gate, transform once, load once.
A pipeline that stops on bad data is annoying for an hour. A dashboard built
on bad data is wrong for a month.
