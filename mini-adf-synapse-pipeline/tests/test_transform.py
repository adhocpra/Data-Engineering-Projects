"""Tests for the mini ADF + Synapse transform (same rules as the SQL proc).

Unit tests use small hand-built rows; the end-to-end test runs the real
247-row CMS sample and asserts the exact counts the local run produces.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from src.transform import (
    build_summary,
    dedupe,
    parse_date,
    parse_money,
    parse_row,
    reject_reason,
    run_pipeline,
    to_curated,
)

SAMPLE_CSV = (
    Path(__file__).resolve().parent.parent
    / "data"
    / "source"
    / "standard_charges_sample.csv"
)


def make_raw(**overrides):
    row = {
        "hospital_id": "test-hospital",
        "hospital_name": "Test General",
        "npi": "1234567890",
        "state": "OK",
        "city": "Tonkawa",
        "cpt_code": "99213",
        "procedure_name": "Office visit",
        "description": "Office visit, established patient",
        "setting": "outpatient",
        "gross_charge": "352.50",
        "cash_price": "120.00",
        "negotiated_min": "52.78",
        "negotiated_max": "233.24",
        "payer_name": "Aetna",
        "plan_name": "Aetna PPO",
        "negotiated_dollar": "150.00",
        "median_amount": "140.00",
        "p10_amount": "60.00",
        "p90_amount": "210.00",
        "file_last_updated": "2026-04-01",
        "source_url": "https://example.invalid/mrf.csv",
        "template_version": "2.0.0",
        "format": "csv-tall",
    }
    row.update(overrides)
    return row


def test_parse_money_handles_empty_decimals_and_symbols():
    assert parse_money("") is None
    assert parse_money("   ") is None
    assert parse_money(None) is None
    assert parse_money("5731.0") == Decimal("5731.0")
    assert parse_money("$1,234.56") == Decimal("1234.56")
    assert parse_money("not-a-number") is None


def test_parse_date_accepts_iso_and_us_formats():
    assert parse_date("2026-04-01") == date(2026, 4, 1)
    assert parse_date("3/31/2026") == date(2026, 3, 31)
    assert parse_date("08/14/2026") == date(2026, 8, 14)
    assert parse_date("") is None
    assert parse_date("31/04/2026") is None  # not a real date in either format


def test_quality_filter_rejects_all_null_prices():
    row = parse_row(
        make_raw(gross_charge="", cash_price="", negotiated_dollar=""), 0
    )
    assert reject_reason(row) == "no_price_at_all"


def test_quality_filter_rejects_negative_amount():
    row = parse_row(make_raw(cash_price="-5.00"), 0)
    assert reject_reason(row) == "negative_amount"
    row = parse_row(make_raw(negotiated_dollar="-1.00"), 0)
    assert reject_reason(row) == "negative_amount"


def test_quality_filter_rejects_negotiated_without_payer():
    row = parse_row(make_raw(payer_name="", plan_name=""), 0)
    assert reject_reason(row) == "negotiated_no_payer"


def test_quality_filter_rejects_missing_key():
    row = parse_row(make_raw(hospital_id=""), 0)
    assert reject_reason(row) == "missing_key"
    row = parse_row(make_raw(cpt_code=""), 0)
    assert reject_reason(row) == "missing_key"


def test_quality_filter_passes_cash_only_reference_row():
    # Rows with gross + cash but no payer are legitimate reference rows.
    row = parse_row(
        make_raw(payer_name="", plan_name="", negotiated_dollar=""), 0
    )
    assert reject_reason(row) is None


def test_dedupe_keeps_highest_negotiated():
    rows = [
        parse_row(make_raw(negotiated_dollar="100.00"), 0),
        parse_row(make_raw(negotiated_dollar="150.00"), 1),
        parse_row(make_raw(negotiated_dollar="120.00"), 2),
    ]
    kept = dedupe(rows)
    assert len(kept) == 1
    assert kept[0].negotiated_dollar == Decimal("150.00")


def test_dedupe_keeps_distinct_payers_and_cash_only_rows():
    rows = [
        parse_row(make_raw(payer_name="Aetna", plan_name="Aetna PPO"), 0),
        parse_row(make_raw(payer_name="BCBS", plan_name="BCBS PPO"), 1),
        parse_row(
            make_raw(
                payer_name="", plan_name="", negotiated_dollar="",
            ),
            2,
        ),
    ]
    kept = dedupe(rows)
    assert len(kept) == 3


def test_monthly_summary_math():
    rows = [
        to_curated(parse_row(make_raw(cash_price="100.00", negotiated_dollar="80.00"), 0)),
        to_curated(parse_row(make_raw(payer_name="BCBS", plan_name="BCBS PPO", cash_price="200.00", negotiated_dollar="160.00"), 1)),
        to_curated(parse_row(make_raw(payer_name="Cigna", plan_name="Cigna PPO", cash_price="300.00", negotiated_dollar="240.00"), 2)),
    ]
    summary = build_summary(rows)
    assert len(summary) == 1
    s = summary[0]
    assert s.cpt_code == "99213"
    assert s.price_month == date(2026, 4, 1)
    assert s.hospital_count == 1
    assert s.charge_count == 3
    assert s.avg_cash_price == Decimal("200.00")
    assert s.median_negotiated == Decimal("160")
    assert s.min_negotiated == Decimal("80.00")
    assert s.max_negotiated == Decimal("240.00")


def test_end_to_end_on_real_sample():
    result = run_pipeline(SAMPLE_CSV)
    assert result.rows_read == 247
    assert result.rows_staged == 247
    assert result.rejected == {
        "missing_key": 0,
        "negative_amount": 0,
        "no_price_at_all": 0,
        "negotiated_no_payer": 0,
    }
    # 247 staged -> dedupe removes the duplicate payer/plan rows
    assert result.rows_curated == 228
    assert result.rows_deduped == 19
    assert result.hospitals == 10
    assert result.procedures == 7
    assert result.summary_rows == 17
    # dedupe keys unique in curated
    keys = [
        (r.hospital_id, r.cpt_code, r.payer_name, r.plan_name)
        for r in result.curated
    ]
    assert len(keys) == len(set(keys))
    # no negative amounts anywhere in curated
    assert all(
        (v is None or v >= 0)
        for r in result.curated
        for v in (r.gross_charge, r.cash_price, r.negotiated_dollar)
    )


def test_sample_is_real_cms_data_not_synthetic():
    # Spot-check rows that exist verbatim in the Project 4 source sample.
    result = run_pipeline(SAMPLE_CSV)
    hillcrest = [
        r
        for r in result.curated
        if r.hospital_id == "hillcrest-tulsa" and r.cpt_code == "43239"
    ]
    assert hillcrest, "expected real Hillcrest 43239 rows in the subset"
    assert any(r.gross_charge == Decimal("5731.0") for r in hillcrest)
    assert any(r.source_url.startswith("https://cdn.ardenthealthservices.com/") for r in hillcrest)
