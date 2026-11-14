CREATE TABLE transactions_clean (
  transaction_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  amount REAL NOT NULL,
  currency TEXT NOT NULL,
  posted_date DATE NOT NULL,
  channel TEXT,
  amount_usd REAL NOT NULL,
  month TEXT NOT NULL,
  is_high_value INTEGER NOT NULL
);
CREATE TABLE monthly_summary (
  month TEXT PRIMARY KEY,
  transactions INTEGER NOT NULL,
  total_usd REAL NOT NULL
);
