"""Project 2 — PySpark + Delta Lake Medallion pipeline.

Bronze -> Silver -> Gold, reusing Project 1's transactions.csv.
Same 4 gate checks as Project 1 at each gate: completeness, uniqueness,
validity, freshness. Fail the load on a failed check.

Run:  python medallion.py
Requires: Java 17, pyspark 3.5.3, delta-spark 3.2.1 (see requirements.txt)
"""
import sys
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Delta jars are downloaded once (curl works through the egress proxy;
# Spark's Ivy resolver does not pick up the proxy) and passed explicitly.
JARS_DIR = Path.home() / ".local" / "share" / "delta-jars"
DELTA_JARS = ",".join(
    str(p)
    for p in [
        JARS_DIR / "delta-spark_2.12-3.2.1.jar",
        JARS_DIR / "delta-storage-3.2.1.jar",
    ]
)

BASE = Path(__file__).parent
# Project 1 data: in the repo it lives in 01-etl-finance-pipeline/,
# locally it is still under project-1-etl-finance-pipeline/.
SOURCE = None
for candidate in [
    BASE.parent / "01-etl-finance-pipeline" / "data" / "transactions.csv",
    BASE.parent / "project-1-etl-finance-pipeline" / "data" / "transactions.csv",
    BASE / "data" / "transactions.csv",
]:
    if candidate.exists():
        SOURCE = candidate
        break
if SOURCE is None:
    SOURCE = BASE.parent / "01-etl-finance-pipeline" / "data" / "transactions.csv"
LAKE = BASE / "lake"
# Storage format: "delta" on Databricks / where Delta jars run, "parquet"
# locally — the sandbox blocks Delta jar distribution over Spark RPC, so
# the local verified run uses Parquet with the same Bronze/Silver/Gold
# layout. Do NOT call the local run Delta Lake.
FORMAT = sys.argv[1] if len(sys.argv) > 1 else "delta"
if FORMAT not in ("delta", "parquet"):
    FORMAT = "delta"

ALLOWED_CURRENCIES = ["USD", "EUR", "GBP"]
FX = {"USD": 1.0, "EUR": 1.08, "GBP": 1.27}


def build_spark():
    builder = (
        SparkSession.builder.appName("medallion-finance")
        .master("local[2]")
        .config("spark.driver.memory", "1g")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
    )
    if FORMAT == "delta":
        builder = builder.config(
            "spark.driver.extraClassPath", DELTA_JARS
        ).config(
            "spark.executor.extraClassPath", DELTA_JARS
        ).config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    return builder.getOrCreate()


def gate_checks(df, label):
    """Print the 4 checks; return True only if all pass."""
    total = df.count()
    nulls = df.filter(
        F.col("transaction_id").isNull()
        | F.col("account_id").isNull()
        | F.col("amount").isNull()
        | F.col("posted_date").isNull()
    ).count()
    dups = total - df.select("transaction_id").distinct().count()
    invalid = df.filter(
        (F.col("amount") <= 0) | (~F.col("currency").isin(ALLOWED_CURRENCIES))
    ).count()
    newest = df.agg(F.max("posted_date")).collect()[0][0]
    fresh = (date.today() - newest) <= timedelta(days=7) if newest else False
    checks = [
        ("completeness", nulls == 0, f"{nulls} rows with null keys"),
        ("uniqueness", dups == 0, f"{dups} duplicate transaction_ids"),
        ("validity", invalid == 0, f"{invalid} rows with bad amount/currency"),
        ("freshness", fresh, f"newest record {newest}"),
    ]
    ok = True
    for name, passed, detail in checks:
        print(f"  [{'PASS' if passed else 'FAIL'}] {label} {name}: {detail}")
        ok = ok and passed
    return ok


def main():
    print(f"Source: {SOURCE}")
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Bronze — raw, as-is
    bronze = spark.read.csv(str(SOURCE), header=True, inferSchema=True)
    bronze.write.format(FORMAT).mode("overwrite").save(str(LAKE / "bronze" / "transactions"))
    print(f"Bronze: {bronze.count()} rows")

    if not gate_checks(bronze, "bronze"):
        spark.stop()
        sys.exit("LOAD FAILED — bronze gate checks failed")

    # Silver — clean, typed, deduplicated, USD
    fx = F.create_map([F.lit(k) for kv in FX.items() for k in kv])
    silver = (
        bronze.dropDuplicates(["transaction_id"])
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("posted_date", F.to_date("posted_date"))
        .withColumn("amount_usd", F.col("amount") * fx[F.col("currency")])
        .withColumn("month", F.date_format("posted_date", "yyyy-MM"))
        .withColumn("is_high_value", F.col("amount_usd") >= 10000)
    )
    silver.write.format(FORMAT).mode("overwrite").save(str(LAKE / "silver" / "transactions"))
    print(f"Silver: {silver.count()} rows")

    if not gate_checks(silver, "silver"):
        spark.stop()
        sys.exit("LOAD FAILED — silver gate checks failed")

    # Gold — monthly summary, same shape as Project 1
    gold = (
        silver.groupBy("month")
        .agg(
            F.count("transaction_id").alias("transactions"),
            F.round(F.sum("amount_usd"), 2).alias("total_usd"),
        )
        .orderBy("month")
    )
    gold.write.format(FORMAT).mode("overwrite").save(str(LAKE / "gold" / "monthly_summary"))
    print(f"Gold: {gold.count()} months")
    gold.show(truncate=False)
    total_usd = silver.agg(F.round(F.sum("amount_usd"), 2)).collect()[0][0]
    print(f"Gold total_usd (all months): {total_usd}")
    print("Done.")
    spark.stop()


if __name__ == "__main__":
    main()
