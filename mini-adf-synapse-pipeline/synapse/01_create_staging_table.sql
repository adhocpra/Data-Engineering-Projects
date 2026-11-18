-- ============================================================================
-- 01_create_staging_table.sql
-- Staging table for the mini ADF + Synapse hospital-charges pipeline.
--
-- The ADF Copy activity lands the source CSV here as raw text on purpose:
-- CMS machine-readable files mix date formats (ISO and M/D/YYYY) and leave
-- money fields blank, so typing happens in the transform procedure
-- (cur.sp_transform_charges), not at load time. The Copy activity runs
-- TRUNCATE before each load, so staging always holds exactly one file.
--
-- Target: Azure Synapse Analytics dedicated SQL pool.
-- Run once: sqlcmd / Azure Data Studio / Synapse Studio SQL script.
-- ============================================================================

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'stg')
    EXEC('CREATE SCHEMA stg');
GO

IF OBJECT_ID('stg.standard_charges', 'U') IS NOT NULL
    DROP TABLE stg.standard_charges;
GO

CREATE TABLE stg.standard_charges
(
    row_id            INT IDENTITY(1,1) NOT NULL,
    hospital_id       NVARCHAR(100)  NULL,
    hospital_name     NVARCHAR(200)  NULL,
    npi               NVARCHAR(20)   NULL,
    state             NVARCHAR(10)   NULL,
    city              NVARCHAR(100)  NULL,
    cpt_code          NVARCHAR(10)   NULL,
    procedure_name    NVARCHAR(200)  NULL,
    description       NVARCHAR(500)  NULL,
    setting           NVARCHAR(20)   NULL,
    gross_charge      NVARCHAR(50)   NULL,
    cash_price        NVARCHAR(50)   NULL,
    negotiated_min    NVARCHAR(50)   NULL,
    negotiated_max    NVARCHAR(50)   NULL,
    payer_name        NVARCHAR(200)  NULL,
    plan_name         NVARCHAR(200)  NULL,
    negotiated_dollar NVARCHAR(50)   NULL,
    median_amount     NVARCHAR(50)   NULL,
    p10_amount        NVARCHAR(50)   NULL,
    p90_amount        NVARCHAR(50)   NULL,
    file_last_updated NVARCHAR(50)   NULL,
    source_url        NVARCHAR(1000) NULL,
    template_version  NVARCHAR(20)   NULL,
    format            NVARCHAR(20)   NULL,
    load_date         DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
)
WITH (HEAP, DISTRIBUTION = ROUND_ROBIN);
GO
