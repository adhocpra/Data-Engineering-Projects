"""Run the Hospital Price Transparency pipeline end to end.

Usage:
    python -m src.run --hospitals config/hospitals.yaml
    python -m src.run --hospitals config/hospitals.yaml --only memorial-hermann-memorial-city
    python -m src.run --hospitals config/hospitals.yaml --no-discovery  # use recorded mrf_url directly

Steps per hospital: discover MRF URL from cms-hpt.txt (fallback: recorded
URL) -> stream-ingest with caps -> normalize to Silver -> validate ->
load Bronze/Silver -> build Gold -> export dashboard JSON + sample CSV ->
write run_log.json. Nothing is pushed anywhere; this only writes local files.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src import discovery, ingest, normalize, validate, warehouse  # noqa: E402

SAMPLE_ROWS = 500


def load_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def write_sample_csv(rows: list[dict], path: Path, limit: int = SAMPLE_ROWS) -> int:
    """Stratified sample: up to ``limit // #hospitals`` rows per hospital, so
    the sample covers every hospital instead of only the first few in
    alphabetical order (Parkland alone contributes 584 Silver rows)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    by_hospital: dict[str, list[dict]] = {}
    for r in rows:
        by_hospital.setdefault(r["hospital_id"], []).append(r)
    per_hospital = max(1, limit // max(1, len(by_hospital)))
    sample: list[dict] = []
    for hid in sorted(by_hospital):
        sample.extend(
            sorted(by_hospital[hid], key=lambda r: r["cpt_code"])[:per_hospital]
        )
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=normalize.SILVER_COLUMNS)
        w.writeheader()
        w.writerows(sample)
    return len(sample)


def main() -> int:
    ap = argparse.ArgumentParser(description="Hospital price transparency pipeline")
    ap.add_argument("--hospitals", required=True, help="path to hospitals.yaml")
    ap.add_argument("--procedures", default="config/procedures.yaml")
    ap.add_argument("--only", action="append", default=[],
                    help="run only these hospital ids (repeatable)")
    ap.add_argument("--no-discovery", action="store_true",
                    help="skip cms-hpt.txt discovery; use recorded mrf_url")
    ap.add_argument("--db", default="warehouse.duckdb")
    args = ap.parse_args()

    hospitals_cfg = load_yaml(Path(args.hospitals))
    procedures_cfg = load_yaml(Path(args.procedures))
    procedures = procedures_cfg["procedures"]
    proc_names = {p["cpt"]: p["name"] for p in procedures}
    cpt_set = {p["cpt"] for p in procedures}

    hospitals = hospitals_cfg["hospitals"]
    if args.only:
        hospitals = [h for h in hospitals if h["id"] in args.only]
        missing = set(args.only) - {h["id"] for h in hospitals}
        if missing:
            print(f"WARNING: unknown hospital ids: {sorted(missing)}", file=sys.stderr)

    started = datetime.now(timezone.utc)
    run_log: dict = {
        "started_at_utc": started.isoformat(),
        "hospitals": {},
        "totals": {},
        "validation": [],
    }
    all_silver: list[dict] = []
    con = warehouse.connect(PROJECT_ROOT / args.db)

    for hospital in hospitals:
        hid = hospital["id"]
        print(f"\n=== {hid} ({hospital['name']}, {hospital['city']}, {hospital['state']}) ===")
        entry: dict = {"name": hospital["name"], "state": hospital["state"]}
        try:
            if args.no_discovery:
                url = hospital["mrf_url"]
                entry["discovery"] = {"method": "skipped", "resolved_url": url}
            else:
                disc = discovery.resolve_mrf_url(hospital, hospital.get("mrf_url"))
                entry["discovery"] = {
                    "method": disc.method,
                    "resolved_url": disc.resolved_url,
                    "detail": disc.detail,
                }
                url = disc.resolved_url
            if not url:
                entry["status"] = "failed"
                entry["error"] = "no MRF URL resolved"
                print("  FAILED: no MRF URL resolved")
                run_log["hospitals"][hid] = entry
                continue
            items, stats = ingest.ingest_url(
                url, hid, cpt_set, scan=hospital.get("scan", "full")
            )
            entry["ingest"] = {
                "source_url": stats.source_url,
                "format": stats.format_detected,
                "template_version": stats.template_version,
                "file_last_updated": stats.file_last_updated,
                "hospital_name_in_file": stats.hospital_name,
                "npi": stats.npi,
                "state_in_file": stats.state,
                "bytes_total": stats.bytes_total,
                "bytes_scanned": stats.bytes_scanned,
                "rows_scanned": stats.rows_scanned,
                "matched_items": stats.matched_items,
                "matched_rows": stats.matched_rows,
                "cpt_counts": stats.cpt_counts,
                "scan_complete": stats.scan_complete,
                "cap_hit": stats.cap_hit,
                "errors": stats.errors,
            }
            print(f"  format={stats.format_detected} v{stats.template_version} "
                  f"updated={stats.file_last_updated} npi={stats.npi}")
            print(f"  scanned {stats.rows_scanned:,} rows "
                  f"({stats.bytes_scanned:,} bytes) "
                  f"-> {stats.matched_items} matched items; "
                  f"complete={stats.scan_complete} cap={stats.cap_hit!r}")
            print(f"  CPT counts: {stats.cpt_counts}")
            rows = normalize.normalize_items(
                items, hospital, proc_names, url,
                stats.file_last_updated, stats.template_version,
                stats.format_detected,
            )
            normalize.attach_npi(rows, stats.npi)
            # NPI from file wins; fall back to config only if the file hid it
            entry["silver_rows"] = len(rows)
            all_silver.extend(rows)
            warehouse.load_bronze(con, hid, items, stats)
            entry["status"] = "ok" if stats.matched_items else "no_matches"
        except Exception as exc:  # noqa: BLE001 — one hospital must not kill the run
            entry["status"] = "failed"
            entry["error"] = f"{type(exc).__name__}: {exc}"
            print(f"  FAILED: {entry['error']}")
        run_log["hospitals"][hid] = entry

    warehouse.load_silver(con, all_silver)
    gold_rows = warehouse.build_gold(con)
    con.close()

    checks = validate.run_all_checks(all_silver, cpt_set)
    run_log["validation"] = [
        {
            "name": c.name, "severity": c.severity,
            "passed": c.passed, "failed": c.failed, "examples": c.examples,
        }
        for c in checks
    ]
    print("\n=== validation ===")
    for c in checks:
        print(f"  {c.name}: passed={c.passed} failed={c.failed} ({c.severity})")
        for ex in c.examples:
            print(f"    - {ex}")

    generated_at = datetime.now(timezone.utc).isoformat()
    payload = warehouse.export_gold_json(
        gold_rows, procedures, PROJECT_ROOT / "data" / "gold.json", generated_at
    )
    sample_n = write_sample_csv(
        all_silver, PROJECT_ROOT / "data" / "sample" / "standard_charges_sample.csv"
    )
    run_log["totals"] = {
        "hospitals_attempted": len(hospitals),
        "hospitals_ok": sum(1 for h in run_log["hospitals"].values() if h.get("status") == "ok"),
        "silver_rows": len(all_silver),
        "gold_rows": len(gold_rows),
        "procedures_with_data": len(payload["procedures"]),
        "sample_rows_written": sample_n,
        "validation_errors": sum(c.failed for c in checks if c.severity == "error"),
        "validation_warnings": sum(c.failed for c in checks if c.severity == "warning"),
    }
    run_log["finished_at_utc"] = datetime.now(timezone.utc).isoformat()
    (PROJECT_ROOT / "run_log.json").write_text(
        json.dumps(run_log, indent=2), encoding="utf-8"
    )
    print(f"\n=== done: {run_log['totals']} ===")
    print(f"gold.json: {len(payload['procedures'])} procedures with data")
    print(f"sample: {sample_n} rows -> data/sample/standard_charges_sample.csv")
    print("run log -> run_log.json")
    return 0 if run_log["totals"]["validation_errors"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
