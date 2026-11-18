# Hospital Price Transparency Pipeline

Compare what the same procedure actually costs at different US hospitals —
gross charge, discounted cash price, and payer-negotiated charges — parsed
from the hospitals' own machine-readable files.

Under the CMS price transparency rule (45 CFR 180.50), every US hospital
must publish a machine-readable file (MRF) of its standard charges. Those
files are public, but they are enormous (some over 1 GB), inconsistent
across hospitals, and unreadable to a patient. This pipeline ingests them,
normalizes them to one schema, validates them, and publishes a comparison
dataset plus a static dashboard anyone can use.

**Live dashboard:** [`dashboard/index.html`](dashboard/index.html)
(served from the project root, or from GitHub Pages — see below)
**Comparison data:** [`data/gold.json`](data/gold.json) ·
**Row-level sample:** [`data/sample/standard_charges_sample.csv`](data/sample/standard_charges_sample.csv)

## Results — run of November 18, 2026

One command (`python -m src.run --hospitals config/hospitals.yaml`, ~8 minutes)
read **1.52 GB of MRF data, 5,195,495 rows**, from **10 hospitals** in 4 states
and produced **1,250 standardized rows** covering **8 common procedures**.

| Procedure (CPT) | Hospitals | Cash price range | Negotiated-median range |
| --- | ---: | ---: | ---: |
| MRI brain (70553) | 9 | $1,327.34 – $2,855.40 | $478.38 – $2,194.02 |
| CT abdomen/pelvis (74177) | 8 | $1,610.82 – $3,998.40 | $478.38 – $2,939.78 |
| Office visit (99213) | 8 | $62.86 – $352.50 | $52.78 – $233.24 |
| Total knee replacement (27447) | 7 | $12,299.80 – $25,627.48 | $12,897.19 – $23,612.64 |
| Upper endoscopy (43239) | 9 | $320.34 – $3,547.22 | $956.42 – $3,112.80 |
| Colonoscopy (45378) | 9 | $519.00 – $2,180.28 | $930.05 – $2,211.71 |
| Metabolic panel (80053) | 9 | $15.00 – $572.50 | $11.46 – $399.30 |
| ECG (93000) | 4 | $20.80 (1 hospital) | $14.84 – $124.34 |

What the numbers actually say:

- **The same MRI brain costs 2.2× more in cash depending on the hospital:**
  $1,327.34 at Saint Francis (Tulsa, OK) vs $2,855.40 at Norman Regional
  (Norman, OK) — two hospitals 90 miles apart.
- **A metabolic panel ranges 38× in cash:** $15.00 at UT Southwestern
  (Dallas, TX) vs $572.50 at Houston Methodist.
- **Most hospitals do not publish a cash price for big procedures.** Only 2 of
  7 hospitals publish a cash price for total knee replacement, and only 1 of 4
  for an ECG — the negotiated median is the only comparable figure for the rest.
- **Negotiated medians can exceed the gross charge.** Saint Francis publishes
  a $800.85 gross charge for upper endoscopy with a $1,846.97 negotiated
  median. That is the hospital's own published data (facility vs professional
  component splits), not a pipeline error — see `docs/verification.md`.

### Hospitals verified

| Hospital | Format | Template | File date | Rows scanned | Matched items | Coverage |
| --- | --- | --- | --- | ---: | ---: | --- |
| Saint Francis Hospital, Tulsa OK | JSON | 3.0.0 | 2026-04-01 | 32,808 | 14 | full file |
| Norman Regional Health System, Norman OK | CSV tall | 2.2.0 | 2026-01-01 | 17,776 | 6 | full file |
| Hillcrest Medical Center, Tulsa OK | CSV tall | 3.0.0 | 2026-03-31 | 730,566 | 5 | full file |
| Memorial Hermann Memorial City, Houston TX | CSV tall | 3.0.0 | 2026-04-01 | 116,042 | 7 | full file |
| The Methodist Hospital, Houston TX | JSON | 3.0.0 | 2026-04-01 | 29,585 | 14 | full file |
| Parkland Health, Dallas TX | CSV tall (zip) | 3.0.0 | 2026-10-16 | 1,000,001 | 7 | capped — 1M rows |
| William P Clements Jr University Hospital, Dallas TX | CSV tall | 3.0.0 | 2026-03-10 | 1,000,001 | 6 | capped — 1M rows |
| Texas Health Presbyterian Hospital Allen, Allen TX | CSV tall | 3.0.0 | 2026-04-15 | 1,000,001 | 8 | capped — 1M rows |
| Mayo Clinic Hospital-Rochester, Rochester MN | CSV tall | 3.0.0 | 2026-08-14 | 926,534 | 1 | capped — 400 MB |
| UPMC Altoona, Altoona PA | CSV tall | 3.0.0 | 2026-03-06 | 342,181 | 7 | full file |

Validation on the 1,250 Silver rows: **0 errors, 6 warnings** — all 6 are
Norman Regional rows carrying no NPI (its v2.2.0 file publishes none; see
below). Full per-hospital verification log: [`docs/verification.md`](docs/verification.md).
Full run log with per-hospital stats: [`run_log.json`](run_log.json).

## Architecture

```
cms-hpt.txt ──► discovery ──► streaming ingest ──► normalize ──► validate ──► DuckDB
  (per hospital)   JSON / CSV tall      one Silver row       8 checks      Bronze / Silver / Gold
                   (zip, 1 GB+)        per item × payer                  gold.json + dashboard
```

- **Discovery** (`src/discovery.py`) — fetches each hospital's `cms-hpt.txt`
  pointer file and resolves the MRF URL for the requested location. A recorded
  `mrf_url` in `config/hospitals.yaml` is used as fallback when discovery is
  blocked (Mayo Clinic's `cms-hpt.txt` intermittently returns 403).
- **Ingest** (`src/ingest.py`) — streams JSON incrementally (`ijson`-style
  parsing with `json.JSONDecoder.raw_decode` on a buffer, never loading the
  whole file) and CSV row-by-row, with a byte cap (400 MB) and row cap
  (1,000,000) so a 13 GB file cannot exhaust the machine. Handles zip
  archives (streaming decompression), BOMs, and non-UTF-8 bytes.
- **Normalize** (`src/normalize.py`) — one Silver row per
  item × payer: hospital name, NPI, state, CPT code, description, gross
  charge, cash price, negotiated min/median/max, payer and plan name, file
  last-updated date, source URL. CSV tall rows sharing a CPT are grouped into
  one item; JSON payer entries become one row each.
- **Validate** (`src/validate.py`) — 8 checks over every Silver row
  (required fields, valid CPT, valid US state code, NPI format, prices
  positive, cash ≤ gross, min ≤ max). Errors fail the run; warnings are
  reported and the run continues.
- **Warehouse** (`src/warehouse.py`) — DuckDB with three layers:
  `bronze.raw_items` (75 matched items as ingested),
  `silver.standard_charges` (1,250 rows), `gold.procedure_comparison`
  (63 rows: one per hospital × procedure, with cash, negotiated median/min/max,
  and payer counts). Exports `data/gold.json` for the dashboard and a
  stratified sample of Silver rows (up to 50 per hospital).

Stack: Python 3.12, `requests`, `duckdb`, `PyYAML`, `pytest`. No cloud, no
paid services. The warehouse file (`warehouse.duckdb`) is a local build
artifact and is not committed; `data/gold.json`, the sample, and
`run_log.json` are.

## Schema (Silver)

| Column | Notes |
| --- | --- |
| `hospital_id`, `hospital_name`, `city`, `state` | `state` from the file's `license_number\|XX` header |
| `npi` | First Type 2 NPI from the file; blank when the file publishes none |
| `cpt_code`, `procedure_name`, `description` | CPT from the 8-code curated set |
| `setting` | inpatient / outpatient / both |
| `gross_charge`, `cash_price` | `standard_charge\|gross`, `standard_charge\|discounted_cash` |
| `negotiated_min`, `negotiated_max` | de-identified min/max across payers |
| `payer_name`, `plan_name`, `negotiated_dollar` | one row per payer × plan |
| `median_amount`, `p10_amount`, `p90_amount` | percentiles, template v3.0 files |
| `file_last_updated`, `template_version`, `source_url`, `format` | provenance |

## Procedures

Eight common shoppable procedures, all verified to appear in real MRFs
(`config/procedures.yaml`): 70553 MRI brain, 74177 CT abdomen/pelvis,
99213 office visit, 27447 total knee arthroplasty, 43239 upper GI
endoscopy, 45378 colonoscopy, 80053 comprehensive metabolic panel,
93000 ECG.

## Run it

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m pytest tests/ -q
.venv/bin/python -m src.run --hospitals config/hospitals.yaml
```

Options: `--only <hospital-id>` (repeatable) to re-run selected hospitals,
`--no-discovery` to use the recorded MRF URLs without fetching
`cms-hpt.txt`. Caps are in `config/hospitals.yaml` per hospital (`scan:
full`, or `max_rows` / `max_bytes`).

## Things the real files taught me

The CMS template is a standard; hospitals' implementations are not.

- **CPT codes are sometimes labelled "HCPCS".** Memorial Hermann tags CPT
  codes with type `HCPCS` (HCPCS Level I *is* CPT). The parser accepts a
  5-digit code labelled HCPCS and rejects letter-prefixed Level II codes.
- **Content-type and file extension lie.** Houston Methodist serves JSON as
  `application/octet-stream` from a `.ashx` URL. The format is decided by
  sniffing the first bytes (`{` → JSON), not by header or extension.
- **Header casing varies.** Parkland's general headers are capitalized
  (`Hospital_Name`, `Last_updated_on`, `Version`); meta extraction is
  case-insensitive.
- **One hospital's CSV is not valid UTF-8.** Parkland's file contains a
  `0xAD` byte; decoding uses `errors="replace"` so one stray byte cannot
  kill a 500 MB file.
- **Template v2.2.0 files publish no NPI.** Norman Regional's file has no
  `type_2_npi` column. The pipeline leaves NPI blank and warns rather than
  guessing: the public NPPES registry lists *two* active Type 2 NPIs for
  the same Norman Regional address (1427244409 and 1922273937), and picking
  one would risk publishing the wrong identifier.
- **Some hospitals publish no payer rows at all for a procedure.** Norman
  Regional's CPT 70553 rows carry gross and cash only — verified against the
  raw file, not a parser bug. Those hospitals appear in the dashboard with a
  cash price and no negotiated figures.
- **Capped scans are partial by design.** UT Southwestern's file is 1.48 GB
  and Mayo's is 13.35 GB; the caps (1M rows / 400 MB) mean only the first
  portion is scanned, so a procedure may be missing from a hospital even
  though it exists deeper in its file. Mayo shows only total knee replacement
  for this reason. The dashboard shows exactly what was scanned — nothing
  is extrapolated.

## Dashboard

`dashboard/index.html` is a single static file (plain JS + Chart.js 4.4.1
via CDN). Pick a procedure and it charts cash price vs negotiated median
across hospitals, highlights the cheapest cash price and the lowest
negotiated median, and lists gross / cash / median / range / payer count
per hospital. It reads `../data/gold.json`, so it works from any static host
or from a local server (`python -m http.server` from the project root);
opening it straight from disk is blocked by browsers' fetch rules, and the
page says so.

## Weekly refresh

`.github/workflows/refresh.yml` runs every Monday 06:00 UTC: tests → pipeline
→ commit refreshed `data/gold.json`, sample, and `run_log.json`.
**The GitHub App cannot push workflow files, so add this file by hand:**
in the repo on GitHub, go to *Add file → Create new file*, path
`.github/workflows/refresh.yml`, paste the contents of the local file, and
commit. Then, in *Settings → Actions → General → Workflow permissions*,
choose **Read and write permissions** so the job can commit its output.

## Limitations

- Capped scans (above) mean coverage is a lower bound, not a census.
- Prices are the hospitals' own published standard charges as of the file's
  last-updated date; they are not a quote, and your bill depends on your
  plan, deductible, and setting.
- Hospitals that could not be ingested at all are listed in
  `config/hospitals.yaml` under `excluded` with the reason (404, 403,
  unreachable, no `cms-hpt.txt`).
- No data is invented. Every price in `data/gold.json`, the sample, and the
  dashboard was parsed from a real downloaded MRF; where a figure is absent
  in the source file it is shown as absent ("—"), never estimated.

## Sources

- CMS hospital price transparency rule, 45 CFR 180.50
- CMS templates and data dictionary (v3.0), github.com/CMSgov/hospital-price-transparency
- Each hospital's `cms-hpt.txt` pointer file and MRF, linked from
  `config/hospitals.yaml` (`source_url` is recorded on every Silver row)

Built by Pravat Adhikari.
