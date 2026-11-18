"""Transform logic for the mini ADF + Synapse hospital-charges pipeline.

This module implements, in plain Python, the EXACT rules of the Synapse
stored procedure ``cur.sp_transform_charges`` (see
``synapse/03_sp_transform_charges.sql``), so the pipeline logic can be
proven locally without an Azure subscription.

Pipeline stages
---------------
1. parse_row: raw CSV text -> typed staging row (money -> Decimal,
   NPI -> int, file_last_updated -> date, accepting ISO ``YYYY-MM-DD``
   and US ``M/D/YYYY``).
2. quality_filter: drop rows that cannot be trusted (see REJECT_REASONS).
3. dedupe: one row per (hospital_id, cpt_code, payer_name, plan_name),
   keeping the highest negotiated_dollar.
4. monthly_summary: per (cpt_code, procedure_name, price_month) the
   hospital count, charge count, average cash price, and the median /
   min / max negotiated charge.

The source data is real CMS hospital price-transparency data (a 247-row
stratified subset of the Project 4 sample). No rows are invented here.
"""

from __future__ import annotations

import csv
import statistics
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional

MONEY_FIELDS = (
    "gross_charge",
    "cash_price",
    "negotiated_min",
    "negotiated_max",
    "negotiated_dollar",
    "median_amount",
    "p10_amount",
    "p90_amount",
)

REJECT_REASONS = (
    "missing_key",          # hospital_id or cpt_code empty
    "negative_amount",      # any money field < 0
    "no_price_at_all",      # gross, cash and negotiated all NULL
    "negotiated_no_payer",  # negotiated_dollar present but payer_name empty
)


def parse_money(value: Optional[str]) -> Optional[Decimal]:
    """Parse a money string to Decimal; empty/blank -> None.

    Strips whitespace, '$' and thousands separators, mirroring
    TRY_CAST(x AS DECIMAL(12,2)) on a cleaned value in the SQL proc.
    """
    if value is None:
        return None
    cleaned = value.strip().replace("$", "").replace(",", "")
    if cleaned == "":
        return None
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def parse_npi(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    cleaned = value.strip()
    if cleaned == "":
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def parse_date(value: Optional[str]) -> Optional[date]:
    """Parse a date accepting ISO (YYYY-MM-DD) and US (M/D/YYYY).

    Mirrors COALESCE(TRY_CONVERT(DATE, x, 23), TRY_CONVERT(DATE, x, 101))
    in the SQL proc. Returns None when the value parses in neither format.
    """
    if value is None:
        return None
    cleaned = value.strip()
    if cleaned == "":
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            from datetime import datetime

            return datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
    return None


@dataclass
class StagedRow:
    """One typed row after parsing (staging)."""

    order: int  # file order, for deterministic tie-breaking
    hospital_id: str
    hospital_name: str
    npi: Optional[int]
    state: str
    city: str
    cpt_code: str
    procedure_name: str
    description: str
    setting: str
    gross_charge: Optional[Decimal]
    cash_price: Optional[Decimal]
    negotiated_min: Optional[Decimal]
    negotiated_max: Optional[Decimal]
    payer_name: str
    plan_name: str
    negotiated_dollar: Optional[Decimal]
    median_amount: Optional[Decimal]
    p10_amount: Optional[Decimal]
    p90_amount: Optional[Decimal]
    file_last_updated: Optional[date]
    source_url: str
    template_version: str
    format: str


@dataclass
class CuratedRow:
    """One row after quality filter + dedupe (curated)."""

    hospital_id: str
    hospital_name: str
    npi: Optional[int]
    state: str
    city: str
    cpt_code: str
    procedure_name: str
    description: str
    setting: str
    gross_charge: Optional[Decimal]
    cash_price: Optional[Decimal]
    negotiated_min: Optional[Decimal]
    negotiated_max: Optional[Decimal]
    payer_name: str
    plan_name: str
    negotiated_dollar: Optional[Decimal]
    median_amount: Optional[Decimal]
    p10_amount: Optional[Decimal]
    p90_amount: Optional[Decimal]
    file_last_updated: Optional[date]
    price_month: Optional[date]
    source_url: str
    template_version: str


@dataclass
class MonthlySummary:
    cpt_code: str
    procedure_name: str
    price_month: date
    hospital_count: int
    charge_count: int
    avg_cash_price: Optional[Decimal]
    median_negotiated: Optional[Decimal]
    min_negotiated: Optional[Decimal]
    max_negotiated: Optional[Decimal]


@dataclass
class RunResult:
    rows_read: int = 0
    rows_staged: int = 0
    rejected: dict = field(default_factory=lambda: {r: 0 for r in REJECT_REASONS})
    rows_after_filter: int = 0
    rows_deduped: int = 0
    rows_curated: int = 0
    summary_rows: int = 0
    hospitals: int = 0
    procedures: int = 0
    curated: list = field(default_factory=list)
    summary: list = field(default_factory=list)


def parse_row(raw: dict, order: int) -> StagedRow:
    return StagedRow(
        order=order,
        hospital_id=(raw.get("hospital_id") or "").strip(),
        hospital_name=(raw.get("hospital_name") or "").strip(),
        npi=parse_npi(raw.get("npi")),
        state=(raw.get("state") or "").strip(),
        city=(raw.get("city") or "").strip(),
        cpt_code=(raw.get("cpt_code") or "").strip(),
        procedure_name=(raw.get("procedure_name") or "").strip(),
        description=(raw.get("description") or "").strip(),
        setting=(raw.get("setting") or "").strip(),
        gross_charge=parse_money(raw.get("gross_charge")),
        cash_price=parse_money(raw.get("cash_price")),
        negotiated_min=parse_money(raw.get("negotiated_min")),
        negotiated_max=parse_money(raw.get("negotiated_max")),
        payer_name=(raw.get("payer_name") or "").strip(),
        plan_name=(raw.get("plan_name") or "").strip(),
        negotiated_dollar=parse_money(raw.get("negotiated_dollar")),
        median_amount=parse_money(raw.get("median_amount")),
        p10_amount=parse_money(raw.get("p10_amount")),
        p90_amount=parse_money(raw.get("p90_amount")),
        file_last_updated=parse_date(raw.get("file_last_updated")),
        source_url=(raw.get("source_url") or "").strip(),
        template_version=(raw.get("template_version") or "").strip(),
        format=(raw.get("format") or "").strip(),
    )


def reject_reason(row: StagedRow) -> Optional[str]:
    """Return the reject reason for a staged row, or None if it passes."""
    if not row.hospital_id or not row.cpt_code:
        return "missing_key"
    amounts = (
        row.gross_charge,
        row.cash_price,
        row.negotiated_min,
        row.negotiated_max,
        row.negotiated_dollar,
        row.median_amount,
        row.p10_amount,
        row.p90_amount,
    )
    if any(a is not None and a < 0 for a in amounts):
        return "negative_amount"
    if (
        row.gross_charge is None
        and row.cash_price is None
        and row.negotiated_dollar is None
    ):
        return "no_price_at_all"
    if row.negotiated_dollar is not None and not row.payer_name:
        return "negotiated_no_payer"
    return None


def dedupe(rows: list[StagedRow]) -> list[StagedRow]:
    """One row per (hospital_id, cpt_code, payer_name, plan_name).

    Keeps the row with the highest negotiated_dollar (NULL counts as
    lowest); ties break on highest gross_charge, then highest cash_price,
    then earliest file order. Mirrors the ROW_NUMBER() window in the SQL
    proc. Rows with an empty payer are cash/gross-only reference rows;
    each is unique per (hospital, cpt) in this dataset and passes through.
    """
    best: dict[tuple, StagedRow] = {}

    def sort_key(r: StagedRow):
        return (
            r.negotiated_dollar if r.negotiated_dollar is not None else Decimal("-1"),
            r.gross_charge if r.gross_charge is not None else Decimal("-1"),
            r.cash_price if r.cash_price is not None else Decimal("-1"),
            -r.order,  # earliest file order wins ties
        )

    for row in rows:
        key = (row.hospital_id, row.cpt_code, row.payer_name, row.plan_name)
        current = best.get(key)
        if current is None or sort_key(row) > sort_key(current):
            best[key] = row
    return sorted(best.values(), key=lambda r: r.order)


def to_curated(row: StagedRow) -> CuratedRow:
    price_month = None
    if row.file_last_updated is not None:
        price_month = date(
            row.file_last_updated.year, row.file_last_updated.month, 1
        )
    return CuratedRow(
        hospital_id=row.hospital_id,
        hospital_name=row.hospital_name,
        npi=row.npi,
        state=row.state,
        city=row.city,
        cpt_code=row.cpt_code,
        procedure_name=row.procedure_name,
        description=row.description,
        setting=row.setting,
        gross_charge=row.gross_charge,
        cash_price=row.cash_price,
        negotiated_min=row.negotiated_min,
        negotiated_max=row.negotiated_max,
        payer_name=row.payer_name,
        plan_name=row.plan_name,
        negotiated_dollar=row.negotiated_dollar,
        median_amount=row.median_amount,
        p10_amount=row.p10_amount,
        p90_amount=row.p90_amount,
        file_last_updated=row.file_last_updated,
        price_month=price_month,
        source_url=row.source_url,
        template_version=row.template_version,
    )


def build_summary(curated: list[CuratedRow]) -> list[MonthlySummary]:
    groups: dict[tuple, list[CuratedRow]] = {}
    for row in curated:
        if row.price_month is None:
            continue
        key = (row.cpt_code, row.procedure_name, row.price_month)
        groups.setdefault(key, []).append(row)
    out: list[MonthlySummary] = []
    for (cpt, proc, month), grp in sorted(groups.items()):
        cash = [r.cash_price for r in grp if r.cash_price is not None]
        neg = [r.negotiated_dollar for r in grp if r.negotiated_dollar is not None]
        avg_cash = (
            sum(cash) / Decimal(len(cash)) if cash else None
        )
        median_neg = None
        if neg:
            median_neg = Decimal(str(statistics.median(neg)))
        out.append(
            MonthlySummary(
                cpt_code=cpt,
                procedure_name=proc,
                price_month=month,
                hospital_count=len({r.hospital_id for r in grp}),
                charge_count=len(grp),
                avg_cash_price=avg_cash,
                median_negotiated=median_neg,
                min_negotiated=min(neg) if neg else None,
                max_negotiated=max(neg) if neg else None,
            )
        )
    return out


def run_pipeline(csv_path: str | Path) -> RunResult:
    """Run the full transform over a source CSV. Returns counts + rows."""
    result = RunResult()
    with open(csv_path, newline="", encoding="utf-8-sig") as f:
        raw_rows = list(csv.DictReader(f))
    result.rows_read = len(raw_rows)
    staged = [parse_row(raw, i) for i, raw in enumerate(raw_rows)]
    result.rows_staged = len(staged)

    passed: list[StagedRow] = []
    for row in staged:
        reason = reject_reason(row)
        if reason is None:
            passed.append(row)
        else:
            result.rejected[reason] += 1
    result.rows_after_filter = len(passed)

    deduped = dedupe(passed)
    result.rows_deduped = len(staged) - len(deduped) - sum(result.rejected.values())
    result.rows_curated = len(deduped)
    result.curated = [to_curated(r) for r in deduped]
    result.summary = build_summary(result.curated)
    result.summary_rows = len(result.summary)
    result.hospitals = len({r.hospital_id for r in result.curated})
    result.procedures = len({r.cpt_code for r in result.curated})
    return result
