"""ETL pipeline: financial transactions -> analytics-ready warehouse.

Extract CSV, validate at the gate, transform, load to SQLite.
Fails the load on bad data instead of warning and carrying on.
"""
import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

BASE = Path(__file__).parent
SOURCE = BASE / "data" / "transactions.csv"
DB = BASE / "warehouse.db"
QUARANTINE = BASE / "data" / "quarantine.csv"

ALLOWED_CURRENCIES = {"USD", "EUR", "GBP"}
FX_TO_USD = {"USD": 1.0, "EUR": 1.08, "GBP": 1.27}


def extract() -> pd.DataFrame:
    df = pd.read_csv(SOURCE, parse_dates=["posted_date"])
    return df


def validate(df: pd.DataFrame) -> pd.DataFrame:
    """Return clean rows; quarantine bad ones. Raise if any check fails."""
    bad = pd.DataFrame()
    checks = []

    # Completeness
    nulls = df[df[["transaction_id", "account_id", "amount", "posted_date"]].isna().any(axis=1)]
    checks.append(("completeness", len(nulls) == 0, f"{len(nulls)} rows with null keys"))
    bad = pd.concat([bad, nulls])

    # Uniqueness
    dups = df[df.duplicated("transaction_id", keep=False)]
    checks.append(("uniqueness", len(dups) == 0, f"{len(dups)} duplicate transaction_ids"))
    bad = pd.concat([bad, dups])

    # Validity
    invalid = df[(df["amount"] <= 0) | (~df["currency"].isin(ALLOWED_CURRENCIES))]
    checks.append(("validity", len(invalid) == 0, f"{len(invalid)} rows with bad amount/currency"))
    bad = pd.concat([bad, invalid])

    # Freshness: newest record within 7 days of today
    newest = df["posted_date"].max().date()
    fresh = (date.today() - newest) <= timedelta(days=7)
    checks.append(("freshness", fresh, f"newest record {newest}"))

    for name, passed, detail in checks:
        print(f"  [{ 'PASS' if passed else 'FAIL' }] {name}: {detail}")

    if not bad.empty:
        bad.drop_duplicates().to_csv(QUARANTINE, index=False)
        print(f"Quarantined {len(bad.drop_duplicates())} bad rows to {QUARANTINE}")

    failed = [name for name, passed, _ in checks if not passed]
    if failed:
        raise SystemExit(f"LOAD FAILED — checks failed: {', '.join(failed)}")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.drop_duplicates(subset=["transaction_id"]).copy()
    df["amount_usd"] = df["amount"] * df["currency"].map(FX_TO_USD)
    df["month"] = df["posted_date"].dt.to_period("M").astype(str)
    df["is_high_value"] = df["amount_usd"] >= 10_000
    return df


def load(df: pd.DataFrame) -> None:
    if DB.exists():
        DB.unlink()
    conn = sqlite3.connect(DB)
    df.to_sql("transactions_clean", conn, index=False)
    monthly = (
        df.groupby("month")
        .agg(transactions=("transaction_id", "count"), total_usd=("amount_usd", "sum"))
        .reset_index()
    )
    monthly.to_sql("monthly_summary", conn, index=False)
    conn.close()
    print(f"Loaded {len(df)} rows into transactions_clean")
    print(f"Monthly summary: {len(monthly)} months")


def main() -> None:
    print("Extracting…")
    df = extract()
    print(f"  {len(df)} raw rows")
    print("Validating…")
    df = validate(df)
    print("Transforming…")
    df = transform(df)
    print("Loading…")
    load(df)
    print("Done.")


if __name__ == "__main__":
    main()
