# Mini ADF + Synapse Pipeline — Hospital Price Transparency

A small, deployable Azure Data Factory + Synapse pipeline over **real**
CMS hospital price-transparency data. ADF copies a CSV from ADLS Gen2
into a Synapse staging table; a stored procedure cleans, dedupes, and
aggregates it into curated tables.

> **Honest status:** the Azure run itself was **not** executed here —
> there is no live Azure subscription behind this repo. The pipeline
> definitions are valid, deployable ADF JSON and Synapse SQL, and the
> transform logic is proven by the local Python validator below, which
> runs the *same* rules as the stored procedure. No row counts, timings,
> or costs are claimed for Azure.

## Architecture

```
data/source/standard_charges_sample.csv   (real CMS rows, 247)
        |
        v  (upload once)
ADLS Gen2  landing / raw/hospital-charges /
        |
        v  ADF Copy activity  (PL_HospitalCharges_Daily)
Synapse dedicated SQL pool
        stg.standard_charges      (raw text, TRUNCATE + load)
        |
        v  ADF Stored Procedure activity
        cur.sp_transform_charges
        |
        +--> cur.standard_charges          (typed, deduped)
        +--> cur.monthly_procedure_summary (per CPT x month)
```

## The data — real, not synthetic

`data/source/standard_charges_sample.csv` is a **247-row subset of real
rows** from the Project 4 hospital price-transparency build
(`04-hospital-price-transparency`), which parsed hospitals' published
CMS machine-readable files (45 CFR 180.50). Selection: the 392-row
Project 4 sample, sorted by hospital / CPT / payer / plan, first 30 rows
per hospital — 10 hospitals, 7 procedures (CPTs 27447, 43239, 45378,
70553, 74177, 80053, 99213), file dates spanning Jan–Oct 2026.
No rows were invented, edited, or synthesized; the subset is verbatim
source rows. One consequence of the stratified cut: CPT 93000 (ECG),
which had only 3 rows in the source sample, is not in this subset.

## Transform rules (identical in SQL and in `src/transform.py`)

1. **Parse** — money text → `DECIMAL(12,2)` (empty → NULL), NPI →
   `BIGINT`, `file_last_updated` → `DATE` accepting ISO `YYYY-MM-DD`
   and US `M/D/YYYY` (the source files mix both).
2. **Quality filter** — a row is rejected when: hospital or CPT is
   missing; any money amount is negative; gross, cash, and negotiated
   charges are *all* missing; or a negotiated charge exists with no
   payer to attribute it to.
3. **Dedupe** — one row per (hospital, CPT, payer, plan), keeping the
   highest negotiated charge (ties: highest gross, then cash, then
   earliest row). Rows with no payer are cash/gross-only reference
   rows; each is unique per (hospital, CPT) here and passes through.
4. **Monthly aggregate** — per (CPT, procedure, price month): hospital
   count, charge count, average cash price, median / min / max
   negotiated charge. (Dedicated SQL pool has no `PERCENTILE_CONT`,
   so the median is computed with the standard `ROW_NUMBER` method.)

## Local validation — real numbers

```bash
python -m src.run_local          # prints counts + checks
python -m pytest tests/ -q       # 12 tests
```

Run on 2026-11-17 (Python 3.12, pytest 9.2.0), over the 247-row sample:

| Step | Rows |
|---|---|
| Rows read / staged | 247 |
| Rejected by quality filter | 0 (0 missing key, 0 negative, 0 no-price, 0 negotiated-no-payer) |
| Removed by dedupe | 19 |
| **Curated rows** | **228** |
| Monthly summary rows | 17 (5 price months: Jan, Mar, Apr, Aug, Oct 2026) |
| Hospitals / procedures in curated | 10 / 7 |

Checks: **8/8 pass** — no negative amounts, every curated row has a
hospital + CPT and at least one price, dedupe keys unique, negotiated
rows always have a payer, summary counts reconcile with curated rows,
medians inside [min, max], cash never exceeds gross. Cash prices in
curated range $88.20–$25,627.48 (n=154). Tests: **12/12 pass**.

## Deploy to Azure (not run here)

Replace every `<...>` placeholder, then:

```bash
# 1. Resource group, storage (ADLS Gen2), Synapse workspace + pool
az group create --name rg-hospital-charges --location centralus
az storage account create --name <your-storage-account> \
  --resource-group rg-hospital-charges --sku Standard_LRS \
  --kind StorageV2 --hierarchical-namespace true
az storage fs create -n landing --account-name <your-storage-account> --auth-mode login
az storage blob upload --account-name <your-storage-account> \
  --container landing --name raw/hospital-charges/standard_charges_sample.csv \
  --file data/source/standard_charges_sample.csv --auth-mode login
az synapse workspace create --name <your-synapse-workspace> \
  --resource-group rg-hospital-charges \
  --storage-account <your-storage-account> --file-system landing \
  --sql-admin-login-user sqladminuser --sql-admin-login-password '<your-sql-password>'
az synapse sql pool create --name hospitalcharges \
  --workspace-name <your-synapse-workspace> --performance-level DW100c

# 2. Tables + stored procedure (run from this repo root)
sqlcmd -S <your-synapse-workspace>.sql.azuresynapse.net -d hospitalcharges \
  -U sqladminuser -P '<your-sql-password>' -i synapse/01_create_staging_table.sql
sqlcmd -S <your-synapse-workspace>.sql.azuresynapse.net -d hospitalcharges \
  -U sqladminuser -P '<your-sql-password>' -i synapse/02_create_curated_tables.sql
sqlcmd -S <your-synapse-workspace>.sql.azuresynapse.net -d hospitalcharges \
  -U sqladminuser -P '<your-sql-password>' -i synapse/03_sp_transform_charges.sql

# 3. Data Factory + ADF objects (or import the JSON files in ADF Studio:
#    Manage -> Linked services / Datasets / Pipelines -> Import from JSON)
az datafactory create --resource-group rg-hospital-charges --name <your-data-factory>
az datafactory linked-service create --resource-group rg-hospital-charges \
  --factory-name <your-data-factory> --name LS_ADLSGen2 \
  --properties "$(jq '.properties' adf/linkedServices/LS_ADLSGen2.json)"
az datafactory linked-service create --resource-group rg-hospital-charges \
  --factory-name <your-data-factory> --name LS_AzureSynapseAnalytics \
  --properties "$(jq '.properties' adf/linkedServices/LS_AzureSynapseAnalytics.json)"
az datafactory dataset create --resource-group rg-hospital-charges \
  --factory-name <your-data-factory> --name DS_SourceCSV \
  --properties "$(jq '.properties' adf/datasets/DS_SourceCSV.json)"
az datafactory dataset create --resource-group rg-hospital-charges \
  --factory-name <your-data-factory> --name DS_StagingTable \
  --properties "$(jq '.properties' adf/datasets/DS_StagingTable.json)"
az datafactory pipeline create --resource-group rg-hospital-charges \
  --factory-name <your-data-factory> --name PL_HospitalCharges_Daily \
  --properties "$(jq '.properties' adf/pipelines/PL_HospitalCharges_Daily.json)"

# 4. Run it, then check the counts the procedure returns
az datafactory pipeline create-run --resource-group rg-hospital-charges \
  --factory-name <your-data-factory> --name PL_HospitalCharges_Daily \
  --parameters '{"RunDate":"2026-11-17"}'

# 5. Pause the SQL pool when done so it stops accruing cost
az synapse sql pool pause --name hospitalcharges \
  --workspace-name <your-synapse-workspace> --resource-group rg-hospital-charges
```

Notes: the linked-service JSONs use parameters (`<your-storage-account>`,
`<your-sql-password>`, …) — fill them in when deploying; nothing secret
is committed here. The pipeline does a full refresh (curated + summary
are truncated and rebuilt each run); that is a deliberate mini-project
choice, not a production incremental pattern.

## Files

```
README.md
data/source/standard_charges_sample.csv   # 247 real CMS rows
adf/linkedServices/LS_ADLSGen2.json
adf/linkedServices/LS_AzureSynapseAnalytics.json
adf/datasets/DS_SourceCSV.json
adf/datasets/DS_StagingTable.json
adf/pipelines/PL_HospitalCharges_Daily.json
synapse/01_create_staging_table.sql
synapse/02_create_curated_tables.sql
synapse/03_sp_transform_charges.sql
src/transform.py                          # same rules as the SQL proc
src/run_local.py                          # local validator (prints counts + checks)
tests/test_transform.py                   # 12 tests
requirements.txt
.gitignore
```
