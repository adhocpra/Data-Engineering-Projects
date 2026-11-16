"""Tests for etl.py: validation must catch bad data, transform must convert
currency and derive fields correctly."""
from datetime import date

import pandas as pd
import pytest

import etl


def _row(**overrides):
    base = {
        "transaction_id": "TXN-00001",
        "account_id": "ACC-1001",
        "amount": 100.0,
        "currency": "USD",
        "posted_date": pd.Timestamp(date.today()),
        "channel": "card",
    }
    base.update(overrides)
    return base


def _df(rows):
    return pd.DataFrame(rows)


def test_validate_passes_clean_data():
    df = _df([_row(), _row(transaction_id="TXN-00002", amount=250.5, currency="EUR")])
    out = etl.validate(df)
    assert len(out) == 2


def test_validate_fails_on_null_amount():
    df = _df([_row(amount=None)])
    with pytest.raises(SystemExit):
        etl.validate(df)


def test_validate_fails_on_duplicate_id():
    df = _df([_row(), _row()])
    with pytest.raises(SystemExit):
        etl.validate(df)


def test_validate_fails_on_negative_amount():
    df = _df([_row(amount=-5.0)])
    with pytest.raises(SystemExit):
        etl.validate(df)


def test_validate_fails_on_bad_currency():
    df = _df([_row(currency="XXX")])
    with pytest.raises(SystemExit):
        etl.validate(df)


def test_validate_fails_on_stale_data():
    df = _df([_row(posted_date=pd.Timestamp("2020-01-01"))])
    with pytest.raises(SystemExit):
        etl.validate(df)


def test_transform_converts_currency_to_usd():
    df = _df([_row(amount=100.0, currency="EUR"), _row(transaction_id="TXN-00002", amount=100.0, currency="GBP")])
    out = etl.transform(df)
    assert out.loc[0, "amount_usd"] == pytest.approx(108.0)
    assert out.loc[1, "amount_usd"] == pytest.approx(127.0)


def test_transform_flags_high_value():
    df = _df([_row(amount=20000.0), _row(transaction_id="TXN-00002", amount=50.0)])
    out = etl.transform(df)
    assert bool(out.loc[0, "is_high_value"]) is True
    assert bool(out.loc[1, "is_high_value"]) is False


def test_transform_derives_month():
    df = _df([_row(posted_date=pd.Timestamp("2026-03-15"))])
    out = etl.transform(df)
    assert out.loc[0, "month"] == "2026-03"
