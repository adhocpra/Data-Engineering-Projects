-- ============================================================================
-- 03_sp_transform_charges.sql
-- Stored procedure for the mini ADF + Synapse hospital-charges pipeline.
--
-- Runs the SAME transform as the local Python validator
-- (src/transform.py), so the logic is proven locally before it ever runs
-- on Azure:
--   1. Parse staging text into typed values (money -> DECIMAL(12,2),
--      NPI -> BIGINT, file_last_updated -> DATE accepting ISO YYYY-MM-DD
--      and US M/D/YYYY).
--   2. Quality filter — a row is rejected when:
--        a. hospital_id or cpt_code is missing;
--        b. any money amount is negative;
--        c. gross charge, cash price AND negotiated charge are all NULL;
--        d. a negotiated charge exists but payer_name is missing.
--   3. Dedupe — one row per (hospital_id, cpt_code, payer_name,
--      plan_name), keeping the highest negotiated_dollar (ties: highest
--      gross_charge, then highest cash_price, then earliest row).
--      Rows with no payer are cash/gross-only reference rows; each is
--      unique per (hospital, CPT) in this dataset and passes through.
--   4. Monthly aggregate — per (cpt_code, procedure_name, price_month):
--      hospital count, charge count, average cash price, and the
--      median / min / max negotiated charge.
--
-- This is a full refresh: curated and summary tables are truncated and
-- rebuilt on every run. That is a deliberate mini-project choice, not a
-- production incremental pattern.
--
-- Target: Azure Synapse Analytics dedicated SQL pool.
-- Invoked by the ADF pipeline PL_HospitalCharges_Daily.
-- ============================================================================

IF OBJECT_ID('cur.sp_transform_charges', 'P') IS NOT NULL
    DROP PROCEDURE cur.sp_transform_charges;
GO

CREATE PROCEDURE cur.sp_transform_charges
    @RunDate DATE
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StagedCount   INT;
    DECLARE @RejectedCount INT;
    DECLARE @CuratedCount  INT;
    DECLARE @SummaryCount  INT;

    SELECT @StagedCount = COUNT(*) FROM stg.standard_charges;

    -- 1. Parse raw text into typed values. TRY_CAST / TRY_CONVERT return
    --    NULL instead of failing the load on a bad value.
    ;WITH Typed AS (
        SELECT
            row_id,
            NULLIF(LTRIM(RTRIM(hospital_id)), '')       AS hospital_id,
            NULLIF(LTRIM(RTRIM(hospital_name)), '')     AS hospital_name,
            TRY_CAST(NULLIF(LTRIM(RTRIM(npi)), '') AS BIGINT) AS npi,
            NULLIF(LTRIM(RTRIM(state)), '')             AS state,
            NULLIF(LTRIM(RTRIM(city)), '')              AS city,
            NULLIF(LTRIM(RTRIM(cpt_code)), '')          AS cpt_code,
            NULLIF(LTRIM(RTRIM(procedure_name)), '')    AS procedure_name,
            NULLIF(LTRIM(RTRIM(description)), '')       AS description,
            NULLIF(LTRIM(RTRIM(setting)), '')           AS setting,
            TRY_CAST(NULLIF(LTRIM(RTRIM(gross_charge)), '') AS DECIMAL(12,2))      AS gross_charge,
            TRY_CAST(NULLIF(LTRIM(RTRIM(cash_price)), '') AS DECIMAL(12,2))        AS cash_price,
            TRY_CAST(NULLIF(LTRIM(RTRIM(negotiated_min)), '') AS DECIMAL(12,2))    AS negotiated_min,
            TRY_CAST(NULLIF(LTRIM(RTRIM(negotiated_max)), '') AS DECIMAL(12,2))    AS negotiated_max,
            NULLIF(LTRIM(RTRIM(payer_name)), '')         AS payer_name,
            NULLIF(LTRIM(RTRIM(plan_name)), '')          AS plan_name,
            TRY_CAST(NULLIF(LTRIM(RTRIM(negotiated_dollar)), '') AS DECIMAL(12,2)) AS negotiated_dollar,
            TRY_CAST(NULLIF(LTRIM(RTRIM(median_amount)), '') AS DECIMAL(12,2))     AS median_amount,
            TRY_CAST(NULLIF(LTRIM(RTRIM(p10_amount)), '') AS DECIMAL(12,2))        AS p10_amount,
            TRY_CAST(NULLIF(LTRIM(RTRIM(p90_amount)), '') AS DECIMAL(12,2))        AS p90_amount,
            COALESCE(
                TRY_CONVERT(DATE, NULLIF(LTRIM(RTRIM(file_last_updated)), ''), 23),
                TRY_CONVERT(DATE, NULLIF(LTRIM(RTRIM(file_last_updated)), ''), 101)
            ) AS file_last_updated,
            NULLIF(LTRIM(RTRIM(source_url)), '')        AS source_url,
            NULLIF(LTRIM(RTRIM(template_version)), '')  AS template_version
        FROM stg.standard_charges
    )
    -- 2. Quality filter (see header for the four rules).
    SELECT *
    INTO #filtered
    FROM Typed
    WHERE hospital_id IS NOT NULL
      AND cpt_code IS NOT NULL
      AND NOT (
            (gross_charge      IS NOT NULL AND gross_charge      < 0)
         OR (cash_price        IS NOT NULL AND cash_price        < 0)
         OR (negotiated_min    IS NOT NULL AND negotiated_min    < 0)
         OR (negotiated_max    IS NOT NULL AND negotiated_max    < 0)
         OR (negotiated_dollar IS NOT NULL AND negotiated_dollar < 0)
         OR (median_amount     IS NOT NULL AND median_amount     < 0)
         OR (p10_amount        IS NOT NULL AND p10_amount        < 0)
         OR (p90_amount        IS NOT NULL AND p90_amount        < 0)
      )
      AND NOT (
            gross_charge IS NULL
        AND cash_price IS NULL
        AND negotiated_dollar IS NULL
      )
      AND NOT (
            negotiated_dollar IS NOT NULL
        AND payer_name IS NULL
      );

    SELECT @RejectedCount = @StagedCount - COUNT(*) FROM #filtered;

    -- 3. Dedupe + insert curated (full refresh).
    DELETE FROM cur.standard_charges;

    ;WITH Ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY hospital_id, cpt_code, payer_name, plan_name
                ORDER BY negotiated_dollar DESC,
                         gross_charge DESC,
                         cash_price DESC,
                         row_id ASC
            ) AS rn
        FROM #filtered
    )
    INSERT INTO cur.standard_charges
    (
        hospital_id, hospital_name, npi, state, city, cpt_code,
        procedure_name, description, setting,
        gross_charge, cash_price, negotiated_min, negotiated_max,
        payer_name, plan_name, negotiated_dollar,
        median_amount, p10_amount, p90_amount,
        file_last_updated, price_month, source_url, template_version
    )
    SELECT
        hospital_id, hospital_name, npi, state, city, cpt_code,
        procedure_name, description, setting,
        gross_charge, cash_price, negotiated_min, negotiated_max,
        payer_name, plan_name, negotiated_dollar,
        median_amount, p10_amount, p90_amount,
        file_last_updated,
        CASE WHEN file_last_updated IS NOT NULL
             THEN DATEFROMPARTS(YEAR(file_last_updated), MONTH(file_last_updated), 1)
        END AS price_month,
        source_url, template_version
    FROM Ranked
    WHERE rn = 1;

    SELECT @CuratedCount = COUNT(*) FROM cur.standard_charges;

    -- 4. Monthly aggregate. Dedicated SQL pool has no PERCENTILE_CONT,
    --    so the median is computed with the standard ROW_NUMBER method:
    --    average the middle one (odd count) or two (even count) values.
    DELETE FROM cur.monthly_procedure_summary;

    ;WITH RankedNeg AS (
        SELECT
            cpt_code, procedure_name, price_month, negotiated_dollar,
            ROW_NUMBER() OVER (
                PARTITION BY cpt_code, procedure_name, price_month
                ORDER BY negotiated_dollar
            ) AS rn,
            COUNT(*) OVER (
                PARTITION BY cpt_code, procedure_name, price_month
            ) AS cnt
        FROM cur.standard_charges
        WHERE negotiated_dollar IS NOT NULL
          AND price_month IS NOT NULL
    ),
    MedianNeg AS (
        SELECT
            cpt_code, procedure_name, price_month,
            AVG(CAST(negotiated_dollar AS DECIMAL(12,2))) AS median_negotiated
        FROM RankedNeg
        WHERE rn IN ((cnt + 1) / 2, (cnt + 2) / 2)
        GROUP BY cpt_code, procedure_name, price_month
    )
    INSERT INTO cur.monthly_procedure_summary
    (
        cpt_code, procedure_name, price_month,
        hospital_count, charge_count, avg_cash_price,
        median_negotiated, min_negotiated, max_negotiated, run_date
    )
    SELECT
        c.cpt_code,
        c.procedure_name,
        c.price_month,
        COUNT(DISTINCT c.hospital_id) AS hospital_count,
        COUNT(*) AS charge_count,
        AVG(CAST(c.cash_price AS DECIMAL(12,2))) AS avg_cash_price,
        MAX(m.median_negotiated) AS median_negotiated,
        MIN(c.negotiated_dollar) AS min_negotiated,
        MAX(c.negotiated_dollar) AS max_negotiated,
        @RunDate AS run_date
    FROM cur.standard_charges c
    LEFT JOIN MedianNeg m
        ON m.cpt_code = c.cpt_code
       AND m.procedure_name = c.procedure_name
       AND m.price_month = c.price_month
    WHERE c.price_month IS NOT NULL
    GROUP BY c.cpt_code, c.procedure_name, c.price_month;

    SELECT @SummaryCount = COUNT(*) FROM cur.monthly_procedure_summary;

    DROP TABLE #filtered;

    -- Report the run counts so the ADF run shows them in its output.
    SELECT
        @RunDate        AS run_date,
        @StagedCount    AS rows_staged,
        @RejectedCount  AS rows_rejected,
        @CuratedCount   AS rows_curated,
        @SummaryCount   AS monthly_summary_rows;
END;
GO
