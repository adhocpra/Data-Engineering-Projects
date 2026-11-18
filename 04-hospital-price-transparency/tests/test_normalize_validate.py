"""Tests for normalization and validation."""

from src.ingest import MatchedItem
from src.normalize import normalize_items
from src.validate import run_all_checks

HOSPITAL = {"id": "test-hosp", "name": "Test Hospital", "city": "Tulsa", "state": "OK"}
PROCS = {"70553": "MRI brain with and without contrast", "99213": "Office visit"}
ALLOWED = {"70553", "99213"}


def _item(**kw):
    base = dict(
        description="MRI BRAIN",
        codes=[("70553", "CPT")],
        setting="outpatient",
        gross_charge=6000.0,
        cash_price=2000.0,
        negotiated_min=500.0,
        negotiated_max=1200.0,
        payers=[
            {"payer_name": "Acme", "plan_name": "Gold", "dollar": 800.0,
             "median": 780.0, "p10": 700.0, "p90": 900.0},
            {"payer_name": "Beta", "plan_name": "Silver", "dollar": 750.0,
             "median": None, "p10": None, "p90": None},
        ],
    )
    base.update(kw)
    return MatchedItem(**base)


def test_normalize_one_row_per_payer_plus_dedupe():
    rows = normalize_items(
        [_item(), _item()],  # exact duplicate item
        HOSPITAL, PROCS, "http://example.test/x", "2026-04-01", "3.0.0", "json",
    )
    assert len(rows) == 2  # two payers, duplicate item collapsed
    assert {r["payer_name"] for r in rows} == {"Acme", "Beta"}
    assert rows[0]["hospital_id"] == "test-hosp"
    assert rows[0]["state"] == "OK"
    assert rows[0]["cpt_code"] == "70553"
    assert rows[0]["gross_charge"] == 6000.0


def test_normalize_item_without_payers_gets_null_payer_row():
    rows = normalize_items(
        [_item(payers=[])], HOSPITAL, PROCS,
        "http://example.test/x", "2026-04-01", "3.0.0", "json",
    )
    assert len(rows) == 1
    assert rows[0]["payer_name"] is None
    assert rows[0]["negotiated_dollar"] is None
    assert rows[0]["cash_price"] == 2000.0


def test_validation_catches_bad_rows():
    good = normalize_items(
        [_item()], HOSPITAL, PROCS, "http://example.test/x",
        "2026-04-01", "3.0.0", "json",
    )
    for r in good:
        r["npi"] = "1234567890"
    results = {c.name: c for c in run_all_checks(good, ALLOWED)}
    assert all(c.ok for c in results.values())

    bad = [dict(good[0])]
    bad[0]["cash_price"] = 99999.0  # cash > gross
    bad[0]["npi"] = "123"           # short NPI
    bad[0]["state"] = "XX"          # not a state
    results = {c.name: c for c in run_all_checks(bad, ALLOWED)}
    assert not results["npi_10_digits_when_present"].ok
    assert not results["state_valid_us_code"].ok
    assert not results["cash_le_gross"].ok  # warning severity
    assert results["cash_le_gross"].severity == "warning"
    assert results["prices_positive_where_present"].ok  # values are positive


def test_missing_npi_is_warning_not_error():
    # A source file that publishes no NPI (Norman Regional's v2.2.0 CSV)
    # must not fail the run; it is reported as a warning.
    rows = normalize_items(
        [_item()], HOSPITAL, PROCS, "http://example.test/x",
        "2026-04-01", "3.0.0", "json",
    )
    for r in rows:
        r["npi"] = ""
    results = {c.name: c for c in run_all_checks(rows, ALLOWED)}
    assert results["npi_10_digits_when_present"].ok
    assert not results["npi_present"].ok
    assert results["npi_present"].severity == "warning"
