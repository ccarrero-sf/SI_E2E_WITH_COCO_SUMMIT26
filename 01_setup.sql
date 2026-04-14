-- ============================================================
-- STEP 1: SETUP - Database
-- Run as: ACCOUNTADMIN
-- NOTE: Database is DROPPED and RECREATED on every run.
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- Drop and recreate the database fresh on every run
CREATE OR REPLACE DATABASE CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E
    COMMENT = 'Snowflake Intelligence End-to-End Lab database';

USE DATABASE CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E;
USE SCHEMA PUBLIC;

-- ---- Verification ----
SHOW DATABASES LIKE 'CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E';
