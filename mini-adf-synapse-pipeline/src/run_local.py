"""Local validator for the mini ADF + Synapse hospital-charges pipeline.

Runs the SAME transform logic as the Synapse stored procedure
``cur.sp_transform_charges`` (``synapse/03_sp_transform_charges.sql``)
over the real CMS sample in ``data/source/`` and prints row counts plus
data-quality check results, so the pipeline logic is proven without an
Azure subscription.

Usage:
    python -m src.run_local
    python -m src.run_local --csv data/source/standard_charges_sample.csv

Exit code is 0 when every check passes, 1 otherwise.
"""

from __future__ import annotations

import argparse
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.transform import run_pipeline  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--csv",
        default=str(
            Path(__file__).resolve().parent.parent
            / "data"
            / "source"
            / "standard_charges_sample.csv"
        ),
        help="Source CSV to validate (default: the bundled real CMS sample).",
    )
    args = parser.parse_args()

    result = run_pipeline(args.csv)

    print("mini-adf-synapse-pipeline — local validation")
    print(f"  source file        : {args.csv}")
    print(f"  rows read          : {result.rows_read}")
    print(f"  rows staged        : {result.rows_staged}")
    for reason, count in result.rejected.items():
        print(f"  rejected ({reason}) : {count}")
    print(f"  rows after filter  : {result.rows_after_filter}")
    print(f"  rows removed by dedupe: {result.rows_deduped}")
    print(f"  rows curated       : {result.rows_curated}")
    print(f"  monthly summary rows: {result.summary_rows}")
    print(f"  hospitals in curated: {result.hospitals}")
    print(f"  procedures in curated: {result.procedures}")

    # ---- checks (same assertions the SQL proc's consumers rely on) ----
    checks: list[tuple[str, bool]] = []
    curated = result.curated
    checks.append(
        (
            "no negative amounts in curated",
            all(
                (v is None or v >= 0)
                for r in curated
                for v in (
                    r.gross_charge,
                    r.cash_price,
                    r.negotiated_dollar,
                )
            ),
        )
    )
    checks.append(
        (
            "every curated row has hospital_id and cpt_code",
            all(r.hospital_id and r.cpt_code for r in curated),
        )
    )
    checks.append(
        (
            "every curated row has at least one price",
            all(
                r.gross_charge is not None
                or r.cash_price is not None
                or r.negotiated_dollar is not None
                for r in curated
            ),
        )
    )
    keys = [(r.hospital_id, r.cpt_code, r.payer_name, r.plan_name) for r in curated]
    checks.append(("dedupe keys unique in curated", len(keys) == len(set(keys))))
    checks.append(
        (
            "negotiated rows always have a payer",
            all(
                r.negotiated_dollar is None or bool(r.payer_name) for r in curated
            ),
        )
    )
    checks.append(
        (
            "summary charge counts reconcile with curated rows",
            sum(s.charge_count for s in result.summary)
            == sum(1 for r in curated if r.price_month is not None),
        )
    )
    checks.append(
        (
            "summary medians within [min, max] negotiated",
            all(
                s.median_negotiated is None
                or (s.min_negotiated <= s.median_negotiated <= s.max_negotiated)
                for s in result.summary
            ),
        )
    )
    # cash price should never exceed gross charge in this source data
    checks.append(
        (
            "cash price never exceeds gross charge",
            all(
                r.cash_price is None
                or r.gross_charge is None
                or r.cash_price <= r.gross_charge
                for r in curated
            ),
        )
    )

    print("checks:")
    failed = 0
    for name, ok in checks:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
        failed += 0 if ok else 1

    # ---- headline numbers ----
    cash = [r.cash_price for r in curated if r.cash_price is not None]
    if cash:
        print(
            f"cash price range in curated: "
            f"${min(cash):,.2f} - ${max(cash):,.2f} "
            f"(n={len(cash)})"
        )
    print(f"checks passed: {len(checks) - failed}/{len(checks)}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
