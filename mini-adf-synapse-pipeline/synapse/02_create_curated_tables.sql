-- ============================================================================
-- 02_create_curated_tables.sql
-- Curated tables for the mini ADF + Synapse hospital-charges pipeline.
--
-- cur.standard_charges: one typed row per (hospital, CPT, payer, plan)
--   after the quality filter and dedupe in cur.sp_transform_charges.
-- cur.monthly_procedure_summary: one row per (CPT, procedure, price month)
--   with hospital count, charge count, average cash price, and the median /
--   min / max negotiated charge.
--
-- Target: Azure Synapse Analytics dedicated SQL pool.
-- ============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'cur')
    EXEC('CREATE SCHEMA cur');
GO

IF OBJECT_ID('cur.standard_charges', 'U') IS NOT NULL
    DROP TABLE cur.standard_charges;
GO

CREATE TABLE cur.standard_charges
(
    hospital_id       NVARCHAR(100)  NOT NULL,
    hospital_name     NVARCHAR(200)  NULL,
    npi               BIGINT         NULL,
    state             NVARCHAR(10)   NULL,
    city              NVARCHAR(100)  NULL,
    cpt_code          VARCHAR(10)    NOT NULL,
    procedure_name    NVARCHAR(200)  NULL,
    description       NVARCHAR(500)  NULL,
    setting           VARCHAR(20)    NULL,
    gross_charge      DECIMAL(12,2)  NULL,
    cash_price        DECIMAL(12,2)  NULL,
    negotiated_min    DECIMAL(12,2)  NULL,
    negotiated_max    DECIMAL(12,2)  NULL,
    payer_name        NVARCHAR(200)  NULL,
    plan_name         NVARCHAR(200)  NULL,
    negotiated_dollar DECIMAL(12,2)  NULL,
    median_amount     DECIMAL(12,2)  NULL,
    p10_amount        DECIMAL(12,2)  NULL,
    p90_amount        DECIMAL(12,2)  NULL,
    file_last_updated DATE           NULL,
    price_month       DATE           NULL,
    source_url        NVARCHAR(1000) NULL,
    template_version  NVARCHAR(20)   NULL,
    load_date         DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);
GO

IF OBJECT_ID('cur.monthly_procedure_summary', 'U') IS NOT NULL
    DROP TABLE cur.monthly_procedure_summary;
GO

CREATE TABLE cur.monthly_procedure_summary
(
    cpt_code          VARCHAR(10)   NOT NULL,
    procedure_name    NVARCHAR(200) NULL,
    price_month       DATE          NOT NULL,
    hospital_count    INT           NOT NULL,
    charge_count      INT           NOT NULL,
    avg_cash_price    DECIMAL(12,2) NULL,
    median_negotiated DECIMAL(12,2) NULL,
    min_negotiated    DECIMAL(12,2) NULL,
    max_negotiated    DECIMAL(12,2) NULL,
    run_date          DATE          NOT NULL
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);
GO
