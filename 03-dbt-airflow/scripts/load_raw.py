"""Load data/transactions.csv into the DuckDB warehouse as raw_transactions.
This is the first task of the Airflow DAG (and the manual step before `dbt build`)."""
import os
import sys

import duckdb

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE, "dbt_project", "warehouse.duckdb")
CSV_PATH = os.path.join(BASE, "data", "transactions.csv")


def main():
    con = duckdb.connect(DB_PATH)
    con.execute("drop table if exists raw_transactions")
    con.execute(
        "create table raw_transactions as "
        "select * from read_csv_auto(?, header=true)",
        [CSV_PATH],
    )
    n = con.execute("select count(*) from raw_transactions").fetchone()[0]
    print(f"loaded {n} rows into raw_transactions")
    con.close()


if __name__ == "__main__":
    sys.exit(main())
