"""Normalize matched MRF items into the single Silver schema.

One Silver row per (item, payer) — plus one row with ``payer_name = NULL``
when an item has no payer-specific negotiated charges at all (common for
office visits, where only the gross charge and cash price are published).

Silver schema
-------------
hospital_id, hospital_name, npi, state, city,
cpt_code, procedure_name, description, setting,
gross_charge, cash_price, negotiated_min, negotiated_max,
payer_name, plan_name, negotiated_dollar,
median_amount, p10_amount, p90_amount,
file_last_updated, source_url, template_version, format

Exact-duplicate rows (same hospital, code, payer, plan, dollar, setting,
description) are dropped — a few hospitals list the same item twice.
"""

from __future__ import annotations

from .ingest import MatchedItem

SILVER_COLUMNS = [
    "hospital_id",
    "hospital_name",
    "npi",
    "state",
    "city",
    "cpt_code",
    "procedure_name",
    "description",
    "setting",
    "gross_charge",
    "cash_price",
    "negotiated_min",
    "negotiated_max",
    "payer_name",
    "plan_name",
    "negotiated_dollar",
    "median_amount",
    "p10_amount",
    "p90_amount",
    "file_last_updated",
    "source_url",
    "template_version",
    "format",
]


def normalize_items(
    items: list[MatchedItem],
    hospital: dict,
    procedures: dict[str, str],
    source_url: str,
    file_last_updated: str,
    template_version: str,
    fmt: str,
) -> list[dict]:
    """Convert matched items to Silver rows. Returns de-duplicated rows."""
    rows: list[dict] = []
    seen: set[tuple] = set()
    for item in items:
        for cpt in {c for c, _ in item.codes}:
            base = {
                "hospital_id": hospital["id"],
                "hospital_name": hospital["name"],
                "npi": "",
                "state": hospital["state"],
                "city": hospital["city"],
                "cpt_code": cpt,
                "procedure_name": procedures.get(cpt, ""),
                "description": item.description,
                "setting": item.setting or "",
                "gross_charge": item.gross_charge,
                "cash_price": item.cash_price,
                "negotiated_min": item.negotiated_min,
                "negotiated_max": item.negotiated_max,
                "file_last_updated": file_last_updated,
                "source_url": source_url,
                "template_version": template_version,
                "format": fmt,
            }
            payers = item.payers or [None]
            for payer in payers:
                row = dict(base)
                if payer is None:
                    row.update(
                        payer_name=None,
                        plan_name=None,
                        negotiated_dollar=None,
                        median_amount=None,
                        p10_amount=None,
                        p90_amount=None,
                    )
                else:
                    row.update(
                        payer_name=payer.get("payer_name") or None,
                        plan_name=payer.get("plan_name") or None,
                        negotiated_dollar=payer.get("dollar"),
                        median_amount=payer.get("median"),
                        p10_amount=payer.get("p10"),
                        p90_amount=payer.get("p90"),
                    )
                key = (
                    row["hospital_id"],
                    row["cpt_code"],
                    row["payer_name"],
                    row["plan_name"],
                    row["negotiated_dollar"],
                    row["setting"],
                    row["description"],
                )
                if key in seen:
                    continue
                seen.add(key)
                rows.append(row)
    return rows


def attach_npi(rows: list[dict], npi: str) -> None:
    """Fill the NPI parsed from the MRF header onto every row (in place)."""
    if not npi:
        return
    for row in rows:
        row["npi"] = npi
