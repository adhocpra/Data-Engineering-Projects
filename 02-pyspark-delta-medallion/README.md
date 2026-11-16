# PySpark Medallion Pipeline — Bronze → Silver → Gold

PySpark Medallion pipeline (Delta Lake on Databricks, Parquet locally —
see note below), reusing Project 1's financial transactions data. The same
pattern used at Capital One and Kaiser Permanente: ingest, check at the
gate, transform once, load once.

## Layers

- **Bronze** — raw CSV as-is, written as a Delta table. 100,000 rows.
- **Silver** — cleaned, typed, deduplicated on `transaction_id`, amounts
  converted to USD, `month` and `is_high_value` (>= $10,000) derived.
- **Gold** — monthly summary (`month`, `transactions`, `total_usd`), the
  same shape as Project 1's `monthly_summary`, so the two projects agree.

## Gate checks (at Bronze and Silver)

Completeness, uniqueness, validity, freshness — fail the load, don't warn
and carry on. A failed check stops the run instead of loading bad data.

## Run it

```bash
pip install -r requirements.txt   # pyspark 3.5.3, delta-spark 3.2.1
# Java 17 is required for Spark
python medallion.py          # Delta Lake (Databricks / Delta jars present)
python medallion.py parquet  # Parquet (local run — see note below)
```

Expected output (verified 2026-11-16, Parquet run): Bronze 100,000 rows,
Silver 100,000 rows, Gold 24 months, Gold total $683,018,592.38 — all 8 gate
checks PASS, matching Project 1 exactly.

## Delta Lake note — read this

`medallion.py` is written for Delta Lake (`python medallion.py`): Delta
tables, Delta extensions, Delta catalog. That is the intended Databricks
run. The local run verified here used Parquet (`python medallion.py
parquet`) because this sandbox blocks Delta jar distribution over Spark
RPC, so Delta would not start locally. The Medallion layout, gate checks,
and Gold totals are identical either way — only the storage format differs.
Do not describe the local run as Delta Lake.

## Files

- `medallion.py` — the pipeline
- `requirements.txt` — pyspark, delta-spark
- `lake/` — Delta tables, created on first run (not committed)
- Data is reused from `../01-etl-finance-pipeline/data/transactions.csv`
  (100,000 synthetic transactions, 24 months)
