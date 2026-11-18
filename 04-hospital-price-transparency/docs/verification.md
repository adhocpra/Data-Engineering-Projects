# Verification log

How every number in this project was checked, and what is known to be
partial. Run: **November 18, 2026, 05:18–05:25 UTC** (463 seconds),
`python -m src.run --hospitals config/hospitals.yaml`.
Full machine-readable log: [`../run_log.json`](../run_log.json).

## Run summary

| Metric | Value |
| --- | --- |
| Hospitals attempted / parsed | 10 / 10 |
| MRF data read | 1.52 GB (1,518,863,504 bytes; Parkland counted as downloaded zip bytes) |
| Rows scanned | 5,195,495 |
| Matched items (CPT in the 8-code set) | 75 |
| Silver rows (item × payer) | 1,250 |
| Gold rows (hospital × procedure) | 63 |
| Procedures with data | 8 / 8 |
| Validation errors | 0 |
| Validation warnings | 6 — all Norman Regional rows missing NPI (see below) |
| Tests | 10 passed (`pytest tests/`) |

## Per-hospital verification

### Saint Francis Hospital, Tulsa OK — full file, JSON v3.0.0
- MRF: 34.9 MB, 32,808 items scanned, 14 matched items, all 8 CPTs.
- NPI 1144228487, file date 2026-04-01, discovered via `cms-hpt.txt`.
- Spot check: MRI brain (70553) cash $1,327.34, gross $3,318.35; ECG (93000)
  gross $52.00, cash $20.80 — the only hospital publishing an ECG cash price.

### Norman Regional Health System, Norman OK — full file, CSV tall v2.2.0
- MRF: 5.4 MB, 17,776 rows scanned, 6 matched items (6 of 8 CPTs;
  27447 and 93000 absent).
- File date 2026-01-01, discovered via `cms-hpt.txt`.
- **No NPI in the file.** The v2.2.0 template's general headers
  (`hospital_name`, `last_updated_on`, `version`, `hospital_location`,
  `hospital_address`, `license_number|OK`, attestation, `financial_aid_policy`)
  contain no `type_2_npi` column. The 6 Silver rows therefore carry a blank
  NPI, reported as a warning, not an error.
- The pipeline does **not** guess an NPI: the public NPPES registry
  (npiregistry.cms.hhs.gov, queried 2026-11-18) lists **two** active Type 2
  NPIs for NORMAN REGIONAL HOSPITAL at the same address
  (901 N Porter Ave, Norman, OK 73070), both taxonomy 282N00000X General
  Acute Care Hospital: **1427244409** and **1922273937**. Picking one would
  risk publishing the wrong identifier.
- **No payer rows for its CPTs — verified against the raw file.** All 8 raw
  rows for CPT 70553 carry gross $4,759.00 and cash $2,855.40 with empty
  payer/plan/negotiated fields (8 different MRI descriptions sharing one
  CPT, grouped into one item). The dashboard's "no negotiated figures" for
  Norman is the file's own content, not a parser bug.

### Hillcrest Medical Center, Tulsa OK — full file, CSV tall v3.0.0
- MRF: 314 MB, 730,566 rows scanned, 5 matched items (70553, 74177, 43239,
  45378, 80053). NPI 1629057229, file date 3/31/2026, via `cms-hpt.txt`.

### Memorial Hermann Memorial City Medical Center, Houston TX — full file, CSV tall v3.0.0
- MRF: 30.7 MB, 116,042 rows scanned, 7 matched items (7 of 8 CPTs; 93000
  absent). NPI 1740233782, file date 2026-04-01, via `cms-hpt.txt`.
- **HCPCS quirk (found here):** this file labels CPT codes with type
  `HCPCS`, not `CPT` (e.g. code 70553, type HCPCS, alongside CDM/RC codes).
  The first smoke test found 0 matches because the parser required
  type == "CPT". The parser now accepts a 5-digit code labelled HCPCS
  (HCPCS Level I *is* CPT) and rejects letter-prefixed Level II codes.
  Regression test in `tests/test_ingest.py`.

### The Methodist Hospital, Houston TX — full file, JSON v3.0.0
- MRF: 72 MB, 29,585 items scanned, 14 matched items, all 8 CPTs.
  NPI 1548387418, file date 2026-04-01.
- **Discovery needed a fix:** the `cms-hpt.txt` location name is
  "Houston Methodist Hospital", not the file's `hospital_name`
  ("The Methodist Hospital"). `location_match` in `config/hospitals.yaml`
  now uses the txt's name.
- **Format detection needed a fix:** the MRF is served as
  `application/octet-stream` from a `.ashx` URL. The first run classified it
  as CSV and found 0 rows. The parser now sniffs content (`{` → JSON).
  Regression: content sniffing is exercised by the Houston Methodist run
  itself (JSON detected, 14 items parsed).

### Parkland Health, Dallas TX — capped scan, CSV tall v3.0.0 (zip)
- MRF: 15.6 MB zip → 521 MB CSV inside. 1,000,001 rows scanned (row cap hit),
  7 matched items (7 of 8 CPTs; 93000 absent). NPI 1932123247, file date
  2026-10-16, via `cms-hpt.txt`.
- **Two real-world quirks fixed here:** (1) the CSV is not valid UTF-8
  (a `0xAD` byte at decompressed offset 871,718) — decoding now uses
  `errors="replace"`; (2) the general headers are capitalized
  (`Hospital_Name`, `Last_updated_on`, `Version`) — meta extraction is now
  case-insensitive. Regression test added for capitalized headers.
- Parkland contributes 584 of the 1,250 Silver rows (16–25 payers per item),
  which is why the sample CSV is stratified per hospital instead of taking
  the first 500 rows alphabetically.

### William P Clements Jr University Hospital (UT Southwestern), Dallas TX — capped scan, CSV tall v3.0.0
- MRF: 1.48 GB. 1,000,001 rows scanned (row cap hit, ~273 MB read),
  6 matched items (99213, 80053, 70553, 45378, 43239, 93000; 74177 and
  27447 not seen in the scanned prefix). NPI 1285798918, file date
  2026-03-10, via `cms-hpt.txt`.

### Texas Health Presbyterian Hospital Allen, Allen TX — capped scan, CSV tall v3.0.0
- MRF: 1.94 GB. 1,000,001 rows scanned (row cap hit, ~265 MB read),
  8 matched items — **all 8 CPTs**, the only capped hospital with full
  procedure coverage. NPI 1548291883, file date 2026-04-15, via
  `cms-hpt.txt`.

### Mayo Clinic Hospital-Rochester, Rochester MN — capped scan, CSV tall v3.0.0
- MRF: 13.35 GB (the largest in the set). 926,534 rows scanned (400 MB byte
  cap hit), **1 matched item** — total knee replacement (27447) only, gross
  $13,820.00, cash $12,299.80, no payer rows in the scanned prefix.
  NPI 1841266194, file date 08/14/2026.
- Discovery fell back to the recorded `mrf_url`: `mayoclinic.org/cms-hpt.txt`
  returned HTTP 403 on this run (it returned 200 during an earlier probe —
  intermittent bot protection). The fallback exists for exactly this case.
- Mayo's single procedure is a **coverage limit of the cap, not of the
  hospital's file** — the other 7 CPTs almost certainly exist deeper in the
  13 GB file. Raising the byte cap (or running the full file on a bigger
  machine) would find them; the dashboard shows only what was scanned.

### UPMC Altoona, Altoona PA — full file, CSV tall v3.0.0
- MRF: 88.5 MB, 342,181 rows scanned, 7 matched items (7 of 8 CPTs; 93000
  absent). NPI 1124185707, file date 3/6/2026, via `cms-hpt.txt`.

## Validation results (1,250 Silver rows)

| Check | Severity | Passed | Failed |
| --- | --- | ---: | ---: |
| required_fields_non_null | error | 1,250 | 0 |
| cpt_valid_5_digit_in_set | error | 1,250 | 0 |
| state_valid_us_code | error | 1,250 | 0 |
| npi_10_digits_when_present | error | 1,250 | 0 |
| npi_present | warning | 1,244 | 6 (Norman Regional) |
| prices_positive_where_present | error | 1,250 | 0 |
| cash_le_gross | warning | 1,250 | 0 |
| negotiated_min_le_max | warning | 1,250 | 0 |

## Known quirks in the published data (not pipeline errors)

- **Negotiated median can exceed gross charge.** Saint Francis, upper
  endoscopy (43239): gross $800.85, negotiated median $1,846.97. Houston
  Methodist, colonoscopy (45378): gross $1,038.00, cash $519.00, negotiated
  median $2,017.86. These are the hospitals' own published figures
  (facility vs professional component splits); `cash_le_gross` passes
  everywhere, and no check asserts median ≤ gross.
- **Several hospitals publish no gross/cash for total knee replacement**
  (Houston Methodist, Memorial Hermann, Saint Francis, Texas Health Allen,
  UPMC Altoona) while publishing negotiated medians — the dashboard shows
  "—" for the missing figures.
- **93000 (ECG) is the thinnest procedure:** 4 hospitals, and only Saint
  Francis publishes a cash price ($20.80). It was kept in the set because it
  does appear in real MRFs; the other 4 CPTs in the original smoke-test
  shortlist all verified, so nothing was dropped.

## Hospitals excluded, with reasons

Recorded in `config/hospitals.yaml` under `excluded`:

| Hospital | Reason |
| --- | --- |
| OU Health (Oklahoma City, OK) | MRF endpoint in `cms-hpt.txt` returns HTTP 404 |
| INTEGRIS Health (Oklahoma City, OK) | `cms-hpt.txt` returns Cloudflare 403 |
| Baylor Scott & White (Dallas, TX) | No `cms-hpt.txt` at website root (404) |
| Johns Hopkins (Baltimore, MD) | `cms-hpt.txt` returns Cloudflare 403 |
| SSM Health St. Anthony (Oklahoma City, OK) | Host intermittently unreachable (proxy disconnects) |
| Cleveland Clinic (Cleveland, OH) | Reachable — 48.9 MB zip holding a 1.43 GB CSV. First alternate if a hospital is added; not in the 10 because of size |

## Sample data

`data/sample/standard_charges_sample.csv` — 392 real Silver rows, stratified
(up to 50 per hospital, all 10 hospitals, all 8 CPTs, 376 rows with a payer).
Regenerated from the same run's warehouse after the run, using the same
stratified sampler the pipeline now uses (`src/run.py::write_sample_csv`);
`run_log.json` was updated to record 392 sample rows.
