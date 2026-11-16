"""Generate 100,000 synthetic financial transactions (24 months) with realistic messiness.
~2% of rows are intentionally bad (nulls, negative amounts, bad currency, duplicate ids)
so the dbt tests and quality gates have something to catch."""
import csv, random
from datetime import date, timedelta

random.seed(42)
N = 100_000
start = date(2024, 12, 1)
currencies = [("USD", 1.0), ("EUR", 1.08), ("GBP", 1.27)]
channels = ["Online", "Branch", "Mobile", "ATM"]
merchants = ["Amazon", "Walmart", "Target", "Costco", "Starbucks", "Shell", "Netflix", "Uber", "Delta", "Home Depot"]

rows = []
for i in range(1, N + 1):
    d = start + timedelta(days=random.randint(0, 729))
    cur, fx = random.choice(currencies)
    amount = round(random.lognormvariate(4.2, 1.1), 2)
    row = {
        "transaction_id": f"T{i:06d}",
        "transaction_date": d.isoformat(),
        "amount": amount,
        "currency": cur,
        "fx_rate": fx,
        "merchant": random.choice(merchants),
        "channel": random.choice(channels),
        "account_id": f"A{random.randint(1, 5000):05d}",
        "status": "Completed",
    }
    # inject bad rows (~2%)
    r = random.random()
    if r < 0.005:
        row["amount"] = None            # missing amount
    elif r < 0.010:
        row["amount"] = -abs(amount)    # negative amount
    elif r < 0.015:
        row["currency"] = "XXX"         # bad currency
    elif r < 0.020:
        row["transaction_id"] = "T000001"  # duplicate id
    rows.append(row)

with open("data/transactions.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    w.writeheader()
    w.writerows(rows)
print(f"wrote {len(rows)} rows")
