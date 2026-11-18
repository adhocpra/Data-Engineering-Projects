"""DuckDB warehouse: Bronze (raw) / Silver (standardized) / Gold (comparison).

* Bronze — one row per matched item as ingested (JSON string), plus the
  per-hospital ingest stats. Nothing is dropped here.
* Silver — the normalized schema from ``normalize.py`` (one row per
  item x payer), typed.
* Gold — one row per (hospital, procedure): gross charge, cash price,
  negotiated min / median / max across payers, payer and plan counts.
  Exported to ``data/gold.json`` for the static dashboard.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .ingest import IngestStats, MatchedItem
from .normalize import SILVER_COLUMNS

GOLD_COLUMNS = [
    "hospital_id",
    "hospital_name",
    "city",
    "state",
    "cpt_code",
    "procedure_name",
    "gross_charge",
    "cash_price",
    "negotiated_min",
    "negotiated_median",
    "negotiated_max",
    "payer_count",
    "plan_count",
    "n_silver_rows",
]


def connect(db_path: str | Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(db_path))
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    return con


def load_bronze(con, hospital_id: str, items: list[MatchedItem],
                stats: IngestStats) -> None:
    con.execute(
        """CREATE TABLE IF NOT EXISTS bronze.raw_items (
               hospital_id VARCHAR, item_json VARCHAR, ingested_at TIMESTAMP DEFAULT now()
           )"""
    )
    con.execute("DELETE FROM bronze.raw_items WHERE hospital_id = ?", [hospital_id])
    rows = [
        (hospital_id, json.dumps(_item_to_dict(it), default=str)) for it in items
    ]
    if rows:  # DuckDB executemany rejects an empty parameter list
        con.executemany("INSERT INTO bronze.raw_items (hospital_id, item_json) VALUES (?, ?)", rows)
    con.execute(
        """CREATE TABLE IF NOT EXISTS bronze.ingest_stats (
               hospital_id VARCHAR, source_url VARCHAR, format_detected VARCHAR,
               template_version VARCHAR, file_last_updated VARCHAR,
               hospital_name VARCHAR, npi VARCHAR, state VARCHAR,
               bytes_total BIGINT, bytes_scanned BIGINT, rows_scanned BIGINT,
               matched_items BIGINT, matched_rows BIGINT,
               scan_complete BOOLEAN, cap_hit VARCHAR, errors VARCHAR
           )"""
    )
    con.execute("DELETE FROM bronze.ingest_stats WHERE hospital_id = ?", [hospital_id])
    con.execute(
        "INSERT INTO bronze.ingest_stats VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            hospital_id, stats.source_url, stats.format_detected,
            stats.template_version, stats.file_last_updated, stats.hospital_name,
            stats.npi, stats.state, stats.bytes_total, stats.bytes_scanned,
            stats.rows_scanned, stats.matched_items, stats.matched_rows,
            stats.scan_complete, stats.cap_hit, "; ".join(stats.errors),
        ],
    )


def _item_to_dict(item: MatchedItem) -> dict:
    return {
        "description": item.description,
        "codes": item.codes,
        "setting": item.setting,
        "gross_charge": item.gross_charge,
        "cash_price": item.cash_price,
        "negotiated_min": item.negotiated_min,
        "negotiated_max": item.negotiated_max,
        "payers": item.payers,
    }


def load_silver(con, rows: list[dict]) -> None:
    con.execute(
        """CREATE OR REPLACE TABLE silver.standard_charges (
               hospital_id VARCHAR, hospital_name VARCHAR, npi VARCHAR,
               state VARCHAR, city VARCHAR, cpt_code VARCHAR,
               procedure_name VARCHAR, description VARCHAR, setting VARCHAR,
               gross_charge DOUBLE, cash_price DOUBLE,
               negotiated_min DOUBLE, negotiated_max DOUBLE,
               payer_name VARCHAR, plan_name VARCHAR,
               negotiated_dollar DOUBLE, median_amount DOUBLE,
               p10_amount DOUBLE, p90_amount DOUBLE,
               file_last_updated VARCHAR, source_url VARCHAR,
               template_version VARCHAR, format VARCHAR
           )"""
    )
    if not rows:
        return
    con.executemany(
        f"INSERT INTO silver.standard_charges ({', '.join(SILVER_COLUMNS)}) "
        f"VALUES ({', '.join(['?'] * len(SILVER_COLUMNS))})",
        [[r.get(c) for c in SILVER_COLUMNS] for r in rows],
    )


def build_gold(con) -> list[dict]:
    con.execute(
        """CREATE OR REPLACE TABLE gold.procedure_comparison AS
           SELECT hospital_id,
                  MAX(hospital_name) AS hospital_name,
                  MAX(city) AS city,
                  MAX(state) AS state,
                  cpt_code,
                  MAX(procedure_name) AS procedure_name,
                  MAX(gross_charge) AS gross_charge,
                  MIN(cash_price) AS cash_price,
                  LEAST(MIN(negotiated_min), MIN(negotiated_dollar)) AS negotiated_min,
                  MEDIAN(negotiated_dollar) AS negotiated_median,
                  GREATEST(MAX(negotiated_max), MAX(negotiated_dollar)) AS negotiated_max,
                  COUNT(DISTINCT payer_name) FILTER (WHERE negotiated_dollar IS NOT NULL) AS payer_count,
                  COUNT(DISTINCT plan_name) FILTER (WHERE negotiated_dollar IS NOT NULL) AS plan_count,
                  COUNT(*) AS n_silver_rows
             FROM silver.standard_charges
            GROUP BY hospital_id, cpt_code
            ORDER BY cpt_code, hospital_id"""
    )
    cur = con.execute(f"SELECT {', '.join(GOLD_COLUMNS)} FROM gold.procedure_comparison")
    return [dict(zip(GOLD_COLUMNS, row)) for row in cur.fetchall()]


def export_gold_json(gold_rows: list[dict], procedures: list[dict],
                     path: str | Path, generated_at: str) -> dict:
    """Shape Gold into the dashboard's JSON: procedures -> hospitals."""
    by_cpt: dict[str, list[dict]] = {}
    for row in gold_rows:
        by_cpt.setdefault(row["cpt_code"], []).append(
            {
                "hospital_id": row["hospital_id"],
                "hospital_name": row["hospital_name"],
                "city": row["city"],
                "state": row["state"],
                "gross_charge": row["gross_charge"],
                "cash_price": row["cash_price"],
                "negotiated_min": row["negotiated_min"],
                "negotiated_median": row["negotiated_median"],
                "negotiated_max": row["negotiated_max"],
                "payer_count": row["payer_count"],
                "plan_count": row["plan_count"],
            }
        )
    payload = {
        "generated_at": generated_at,
        "source": "CMS hospital price transparency machine-readable files (45 CFR 180.50)",
        "note": "Every price was parsed from a real hospital MRF. "
                "Hospitals whose files were only partially scanned are marked partial.",
        "procedures": [
            {
                "cpt": p["cpt"],
                "name": p["name"],
                "short_name": p["short_name"],
                "category": p["category"],
                "hospitals": sorted(
                    by_cpt.get(p["cpt"], []),
                    key=lambda h: (h["cash_price"] is None, h["cash_price"] or 0),
                ),
            }
            for p in procedures
            if by_cpt.get(p["cpt"])
        ],
    }
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload
