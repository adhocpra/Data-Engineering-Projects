"""Regenerate data/transactions.csv at 100,000 rows (24 months), all clean.

Same schema as the original 1,000-row file, so etl.py runs unchanged:
transaction_id, account_id, amount, currency, posted_date, channel.
Dates run to the current day so the freshness check passes.
"""
import csv
import random
from datetime import date, timedelta

random.seed(7)
N = 100_000
END = date(2026, 11, 16)
START = date(2024, 12, 1)
DAYS = (END - START).days
CURRENCIES = ["USD", "EUR", "GBP"]
CHANNELS = ["card", "direct_debit", "transfer"]

with open("data/transactions.csv", "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["transaction_id", "account_id", "amount", "currency", "posted_date", "channel"])
    for i in range(1, N + 1):
        d = START + timedelta(days=random.randint(0, DAYS))
        amount = round(random.lognormvariate(8.0, 1.2), 2)
        w.writerow([
            f"TXN-{i:05d}",
            f"ACC-{random.randint(1000, 1999)}",
            amount,
            random.choice(CURRENCIES),
            d.isoformat(),
            random.choice(CHANNELS),
        ])
print(f"wrote {N} rows")
