-- ============================================================
-- STEP 8: ROW ACCESS POLICIES ON SALES DATA
-- Run as: ACCOUNTADMIN
-- The policy allows all authenticated users to see all rows.
-- BIKE_ROLE and SNOW_ROLE have been removed; the default role
-- of the running user is used throughout.
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E;
USE SCHEMA PUBLIC;

-- ============================================================
-- Row Access Policy definition
-- All roles are allowed full access to FACT_SALES.
-- ============================================================
CREATE OR REPLACE ROW ACCESS POLICY sales_product_rap
AS (article_id NUMBER) RETURNS BOOLEAN ->
    TRUE;

-- ============================================================
-- Attach the policy to FACT_SALES on the ARTICLE_ID column
-- ============================================================
ALTER TABLE FACT_SALES
    ADD ROW ACCESS POLICY sales_product_rap ON (ARTICLE_ID);

-- ============================================================
-- Verification
-- ============================================================
SHOW ROW ACCESS POLICIES IN SCHEMA CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC;

-- All rows visible for all roles
SELECT
    da.ARTICLE_CATEGORY,
    da.ARTICLE_NAME,
    COUNT(*)            AS sale_rows,
    SUM(fs.TOTAL_PRICE) AS total_revenue
FROM FACT_SALES fs
JOIN DIM_ARTICLE da ON fs.ARTICLE_ID = da.ARTICLE_ID
GROUP BY da.ARTICLE_CATEGORY, da.ARTICLE_NAME
ORDER BY da.ARTICLE_CATEGORY, da.ARTICLE_NAME;
