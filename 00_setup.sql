-- ============================================================
-- ALL STEPS - CONSOLIDATED SCRIPT
-- Combines: 01_setup.sql → 02_git_integration.sql →
--           03_stages_and_ingestion.sql → 04_data_processing.sql →
--           06_tables_from_csv.sql → 07_cortex_search.sql
-- Run as: ACCOUNTADMIN
-- ============================================================


-- ============================================================
-- SOURCE: 01_setup.sql
-- ============================================================

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


-- ============================================================
-- SOURCE: 02_git_integration.sql
-- ============================================================

-- ============================================================
-- STEP 2: GIT INTEGRATION
-- Run as: ACCOUNTADMIN
-- Creates an account-level API integration for GitHub and a
-- database-level Git repository pointing to the source files.
-- The API integration persists across database re-creates.
-- Repo is public - no authentication secrets needed.
-- ============================================================

--USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E;
USE SCHEMA PUBLIC;

-- ---- Account-level API integration for GitHub ----
CREATE OR REPLACE API INTEGRATION GITHUB_SI_E2E_INTEGRATION
    API_PROVIDER         = GIT_HTTPS_API
    API_ALLOWED_PREFIXES = ('https://github.com/ccarrero-sf/')
    ENABLED              = TRUE
    COMMENT              = 'API integration for ccarrero-sf GitHub organization (public repos)';

-- ---- Git repository (database-level object) ----
CREATE OR REPLACE GIT REPOSITORY SI_E2E_FILES_REPO
    API_INTEGRATION = GITHUB_SI_E2E_INTEGRATION
    ORIGIN          = 'https://github.com/ccarrero-sf/SI_E2E_WITH_COCO_SUMMIT26'
    COMMENT         = 'Source files for Snowflake Intelligence E2E lab';

-- Fetch latest content from remote
ALTER GIT REPOSITORY SI_E2E_FILES_REPO FETCH;

-- ---- Verification: list files in both folders ----
LS @SI_E2E_FILES_REPO/branches/main/csv/;
LS @SI_E2E_FILES_REPO/branches/main/docs/;


-- ============================================================
-- SOURCE: 03_stages_and_ingestion.sql
-- ============================================================

-- ============================================================
-- STEP 3: STAGES AND DATA INGESTION
-- Run as: ACCOUNTADMIN
-- Creates two internal stages (all with Snowflake SSE),
-- then copies files from the Git repository into the stages.
--
-- Stage layout:
--   CSV_STAGE  - structured CSV data
--   DOCS_STAGE - all product PDFs and images (bike + snow)
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E;
USE SCHEMA PUBLIC;

-- ---- Internal stages with Snowflake SSE encryption ----

CREATE OR REPLACE STAGE CSV_STAGE
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT    = 'Structured CSV data files';

CREATE OR REPLACE STAGE DOCS_STAGE
    DIRECTORY  = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT    = 'All product PDFs and images (bike + snow)';

-- ============================================================
-- Copy CSV files from Git repository
-- ============================================================
COPY FILES INTO @CSV_STAGE
    FROM @SI_E2E_FILES_REPO/branches/main/csv/
    FILES = (
        'DIM_ARTICLE.csv',
        'DIM_CUSTOMER.csv',
        'DIM_SHOP.csv',
        'eval_dataset.csv',
        'customer_experience_comments.csv_0_0_0.csv.gz',
        'fact_sales.csv'
    );

-- ============================================================
-- Copy all PDF and image documents from Git repository
-- ============================================================
COPY FILES INTO @DOCS_STAGE
    FROM @SI_E2E_FILES_REPO/branches/main/docs/
    PATTERN = '.*\.(pdf|PDF|jpeg|JPEG|jpg|JPG)$';

-- ---- Verification: list stage contents ----
LS @CSV_STAGE;    -- expect 6 files
LS @DOCS_STAGE;   -- expect 20 files (13 bike + 7 snow)


-- ============================================================
-- SOURCE: 04_data_processing.sql
-- ============================================================

-- ============================================================
-- STEP 4: AI DATA PROCESSING - PDF & IMAGE PIPELINE
-- Run as: ACCOUNTADMIN
-- Processes PDFs and images from the doc stage into a single
-- DOCS_CHUNKS_TABLE table, then runs sanity checks.
--
-- Pipeline:
--   PDF  -> AI_PARSE_DOCUMENT (LAYOUT) -> SPLIT_TEXT_RECURSIVE_CHARACTER
--        -> AI_CLASSIFY (filename + first 500 chars) -> DOCS_CHUNKS_TABLE
--   IMG  -> AI_COMPLETE claude-3-7-sonnet (image description)
--        -> AI_CLASSIFY (filename) -> DOCS_CHUNKS_TABLE (one row/image)
--
-- Chunk settings: chunk_size=1500, overlap=100, format='markdown'
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E;
USE SCHEMA PUBLIC;

-- ============================================================
-- PART A: Refresh directory metadata on DOCS_STAGE
-- (Required for DIRECTORY() table function and AI image functions)
-- ============================================================

-- Refresh directory metadata so newly copied files are visible
ALTER STAGE DOCS_STAGE REFRESH;

-- Verify directory is populated
SELECT 'DOCS_STAGE' AS stage, COUNT(*) AS file_count FROM DIRECTORY(@DOCS_STAGE);

-- ============================================================
-- PART B: Create destination table
-- ============================================================
CREATE OR REPLACE TABLE DOCS_CHUNKS_TABLE (
    CHUNK_ID         NUMBER AUTOINCREMENT PRIMARY KEY,
    SOURCE_FILE      VARCHAR NOT NULL,
    FILE_TYPE        VARCHAR NOT NULL,   -- 'PDF' | 'IMAGE'
    PRODUCT_CATEGORY VARCHAR NOT NULL,   -- 'BIKE' | 'SNOW'  (AI-classified)
    STAGE_NAME       VARCHAR NOT NULL,   -- 'DOCS_STAGE'
    CHUNK_TEXT       VARCHAR NOT NULL,   -- text chunk (PDF) or image description (IMAGE)
    CHUNK_INDEX      NUMBER  NOT NULL    -- 0-based position within source file
);

-- ============================================================
-- PART C: Process PDF files from DOCS_STAGE
-- ============================================================

-- Step C1: Parse all PDFs (AI_PARSE_DOCUMENT extracts full text as markdown)
CREATE OR REPLACE TEMP TABLE DOCS_PDF_PARSED AS
SELECT
    RELATIVE_PATH,
    AI_PARSE_DOCUMENT(
        TO_FILE('@DOCS_STAGE', RELATIVE_PATH),
        {'mode': 'LAYOUT'}
    ):content::VARCHAR AS doc_text
FROM DIRECTORY(@DOCS_STAGE)
WHERE RELATIVE_PATH ILIKE '%.pdf';

-- Step C2: Classify each PDF once (filename + first 500 chars of text)
CREATE OR REPLACE TEMP TABLE DOCS_PDF_CLASSIFIED AS
SELECT
    RELATIVE_PATH,
    doc_text,
    AI_CLASSIFY(
        RELATIVE_PATH || '. ' || LEFT(doc_text, 500),
        ['BIKE', 'SNOW']
    ):labels[0]::VARCHAR AS product_category
FROM DOCS_PDF_PARSED
WHERE doc_text IS NOT NULL;

-- Step C3: Chunk and insert PDF content
INSERT INTO DOCS_CHUNKS_TABLE (SOURCE_FILE, FILE_TYPE, PRODUCT_CATEGORY, STAGE_NAME, CHUNK_TEXT, CHUNK_INDEX)
SELECT
    RELATIVE_PATH          AS source_file,
    'PDF'                  AS file_type,
    product_category,
    'DOCS_STAGE'           AS stage_name,
    c.value::VARCHAR       AS chunk_text,
    c.index::NUMBER        AS chunk_index
FROM DOCS_PDF_CLASSIFIED,
LATERAL FLATTEN(
    input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(doc_text, 'markdown', 1500, 100)
) c
WHERE c.value::VARCHAR IS NOT NULL
  AND LENGTH(c.value::VARCHAR) > 0;

-- ============================================================
-- PART D: Process IMAGE files from DOCS_STAGE
-- AI_COMPLETE generates a rich description; AI_CLASSIFY assigns category
-- NOTE: If claude-3-7-sonnet is unavailable for images, use claude-3-5-sonnet
-- ============================================================
INSERT INTO DOCS_CHUNKS_TABLE (SOURCE_FILE, FILE_TYPE, PRODUCT_CATEGORY, STAGE_NAME, CHUNK_TEXT, CHUNK_INDEX)
SELECT
    RELATIVE_PATH AS source_file,
    'IMAGE'       AS file_type,
    AI_CLASSIFY(
        RELATIVE_PATH,
        ['BIKE', 'SNOW']
    ):labels[0]::VARCHAR AS product_category,
    'DOCS_STAGE'  AS stage_name,
    AI_COMPLETE(
        'claude-3-7-sonnet',
        PROMPT('Provide a detailed description of this product image {0}. Focus on the product shown, its visible features, design characteristics, colors, components, and any relevant details useful for a customer considering this product.',
            TO_FILE('@DOCS_STAGE', RELATIVE_PATH))
    ) AS chunk_text,
    0 AS chunk_index
FROM DIRECTORY(@DOCS_STAGE)
WHERE RELATIVE_PATH ILIKE '%.jpeg'
   OR RELATIVE_PATH ILIKE '%.jpg'
   OR RELATIVE_PATH ILIKE '%.png';

-- ============================================================
-- PART G: POST-INSERT SANITY CHECKS
-- ============================================================

-- Check 1: Overall row counts by file type and category
SELECT
    FILE_TYPE,
    PRODUCT_CATEGORY,
    COUNT(*)              AS chunk_count,
    COUNT(DISTINCT SOURCE_FILE) AS file_count
FROM DOCS_CHUNKS_TABLE
GROUP BY FILE_TYPE, PRODUCT_CATEGORY
ORDER BY FILE_TYPE, PRODUCT_CATEGORY;

-- Check 2: NULL values (expect 0 for both)
SELECT
    COUNT_IF(CHUNK_TEXT IS NULL)       AS null_chunk_text,
    COUNT_IF(PRODUCT_CATEGORY IS NULL) AS null_category,
    COUNT_IF(LENGTH(CHUNK_TEXT) = 0)   AS empty_chunk_text
FROM DOCS_CHUNKS_TABLE;

-- Check 3: Per-file chunk counts (PDFs should have multiple chunks; images = 1)
SELECT
    SOURCE_FILE,
    FILE_TYPE,
    PRODUCT_CATEGORY,
    COUNT(*) AS chunks
FROM DOCS_CHUNKS_TABLE
GROUP BY SOURCE_FILE, FILE_TYPE, PRODUCT_CATEGORY
ORDER BY FILE_TYPE, SOURCE_FILE;

-- Check 4: Category distribution matches expectations
-- BIKE files: Mondracer, Premium_Bicycle, Xtreme, Ultimate_Downhill -> BIKE
-- SNOW files: Carver, OutPiste, RacingFast, Ski_Boots, Outpiste -> SNOW
SELECT
    SOURCE_FILE,
    PRODUCT_CATEGORY,
    CASE
        WHEN SOURCE_FILE ILIKE '%bike%'
          OR SOURCE_FILE ILIKE '%bicycle%'
          OR SOURCE_FILE ILIKE '%mondracer%'
          OR SOURCE_FILE ILIKE '%downhill%'
          OR SOURCE_FILE ILIKE '%xtreme%'
          OR SOURCE_FILE ILIKE '%road%'
        THEN 'BIKE'
        ELSE 'SNOW'
    END AS expected_category,
    IFF(PRODUCT_CATEGORY = CASE
        WHEN SOURCE_FILE ILIKE '%bike%'
          OR SOURCE_FILE ILIKE '%bicycle%'
          OR SOURCE_FILE ILIKE '%mondracer%'
          OR SOURCE_FILE ILIKE '%downhill%'
          OR SOURCE_FILE ILIKE '%xtreme%'
          OR SOURCE_FILE ILIKE '%road%'
        THEN 'BIKE'
        ELSE 'SNOW'
    END, 'CORRECT', 'MISMATCH') AS classification_result
FROM DOCS_CHUNKS_TABLE
GROUP BY SOURCE_FILE, PRODUCT_CATEGORY
ORDER BY SOURCE_FILE;


-- ============================================================
-- SOURCE: 06_tables_from_csv.sql
-- ============================================================

-- ============================================================
-- STEP 6: CREATE STRUCTURED TABLES FROM CSV FILES
-- Run as: ACCOUNTADMIN
-- Loads all CSV files from CSV_STAGE into Snowflake tables:
--   DIM_ARTICLE               - Product catalogue (8 articles)
--   DIM_CUSTOMER              - Customer master data
--   DIM_SHOP                  - Shop locations (12 shops across USA, Austria, Spain)
--   FACT_SALES                - Sales transactions (with SHOP_ID)
--   CUSTOMER_EXPERIENCE_COMMENTS - Product reviews / feedback
--
-- NOTE: eval_dataset.csv is kept as a stage file only (it is a
--       test/evaluation artefact, not operational data).
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E;
USE SCHEMA PUBLIC;

-- ============================================================
-- Shared file format (used for all COPY INTO commands)
-- ============================================================
CREATE OR REPLACE FILE FORMAT CSV_FMT
    TYPE                        = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF                     = ('NULL', 'null', 'None', '')
    EMPTY_FIELD_AS_NULL         = TRUE
    TRIM_SPACE                  = TRUE;

-- ============================================================
-- DIM_ARTICLE  (header row present)
-- ============================================================
CREATE OR REPLACE TABLE DIM_ARTICLE (
    ARTICLE_ID       NUMBER        NOT NULL PRIMARY KEY,
    ARTICLE_NAME     VARCHAR(255)  NOT NULL,
    ARTICLE_CATEGORY VARCHAR(100)  NOT NULL,   -- 'Bike' | 'Skis' | 'Ski Boots'
    ARTICLE_BRAND    VARCHAR(100),
    ARTICLE_COLOR    VARCHAR(100),
    ARTICLE_PRICE    NUMBER(10, 2) NOT NULL
);

COPY INTO DIM_ARTICLE (ARTICLE_ID, ARTICLE_NAME, ARTICLE_CATEGORY,
                        ARTICLE_BRAND, ARTICLE_COLOR, ARTICLE_PRICE)
FROM (
    SELECT $1::NUMBER, $2::VARCHAR, $3::VARCHAR,
           $4::VARCHAR, $5::VARCHAR, $6::NUMBER
    FROM @CSV_STAGE/DIM_ARTICLE.csv
)
FILE_FORMAT = (FORMAT_NAME = CSV_FMT, SKIP_HEADER = 1)
ON_ERROR    = ABORT_STATEMENT;

-- ============================================================
-- DIM_CUSTOMER  (header row present)
-- ============================================================
CREATE OR REPLACE TABLE DIM_CUSTOMER (
    CUSTOMER_ID      NUMBER       NOT NULL PRIMARY KEY,
    CUSTOMER_NAME    VARCHAR(255),
    CUSTOMER_REGION  VARCHAR(100),
    CUSTOMER_AGE     NUMBER(3),
    CUSTOMER_GENDER  VARCHAR(50),
    CUSTOMER_SEGMENT VARCHAR(100)   -- e.g. 'Premium' | 'Regular'
);

COPY INTO DIM_CUSTOMER (CUSTOMER_ID, CUSTOMER_NAME, CUSTOMER_REGION,
                         CUSTOMER_AGE, CUSTOMER_GENDER, CUSTOMER_SEGMENT)
FROM (
    SELECT $1::NUMBER, $2::VARCHAR, $3::VARCHAR,
           $4::NUMBER, $5::VARCHAR, $6::VARCHAR
    FROM @CSV_STAGE/DIM_CUSTOMER.csv
)
FILE_FORMAT = (FORMAT_NAME = CSV_FMT, SKIP_HEADER = 1)
ON_ERROR    = ABORT_STATEMENT;

-- ============================================================
-- DIM_SHOP  (header row present)
-- 5 shops in USA, 3 in Austria, 4 in Spain
-- IS_MOUNTAIN = TRUE  → mountain location (snow + downhill bias)
-- IS_MOUNTAIN = FALSE → urban location (race + bike bias)
-- ============================================================
CREATE OR REPLACE TABLE DIM_SHOP (
    SHOP_ID      NUMBER       NOT NULL PRIMARY KEY,
    SHOP_NAME    VARCHAR(255) NOT NULL,
    SHOP_CITY    VARCHAR(100) NOT NULL,
    SHOP_COUNTRY VARCHAR(100) NOT NULL,
    IS_MOUNTAIN  BOOLEAN      NOT NULL
);

COPY INTO DIM_SHOP (SHOP_ID, SHOP_NAME, SHOP_CITY, SHOP_COUNTRY, IS_MOUNTAIN)
FROM (
    SELECT $1::NUMBER, $2::VARCHAR, $3::VARCHAR,
           $4::VARCHAR, $5::BOOLEAN
    FROM @CSV_STAGE/DIM_SHOP.csv
)
FILE_FORMAT = (FORMAT_NAME = CSV_FMT, SKIP_HEADER = 1)
ON_ERROR    = ABORT_STATEMENT;

-- ============================================================
-- FACT_SALES  (no header row)
-- SHOP_ID is included in the CSV data.
-- ============================================================
CREATE OR REPLACE TABLE FACT_SALES (
    SALE_ID        NUMBER        NOT NULL PRIMARY KEY,
    ARTICLE_ID     NUMBER        NOT NULL REFERENCES DIM_ARTICLE(ARTICLE_ID),
    DATE_SALES     DATE          NOT NULL,
    CUSTOMER_ID    NUMBER        NOT NULL REFERENCES DIM_CUSTOMER(CUSTOMER_ID),
    QUANTITY_SOLD  NUMBER        NOT NULL,
    TOTAL_PRICE    NUMBER(12, 2) NOT NULL,
    SALES_CHANNEL  VARCHAR(100),             -- 'Online' | 'Partner'
    IS_RETURN      BOOLEAN       DEFAULT FALSE,
    SHOP_ID        NUMBER        REFERENCES DIM_SHOP(SHOP_ID)
);

COPY INTO FACT_SALES (SALE_ID, ARTICLE_ID, DATE_SALES, CUSTOMER_ID,
                       QUANTITY_SOLD, TOTAL_PRICE, SALES_CHANNEL, IS_RETURN, SHOP_ID)
FROM (
    SELECT $1::NUMBER, $2::NUMBER, $3::DATE,
           $4::NUMBER, $5::NUMBER, $6::NUMBER(12,2),
           $7::VARCHAR, $8::BOOLEAN, $9::NUMBER
    FROM @CSV_STAGE/fact_sales.csv
)
FILE_FORMAT = (FORMAT_NAME = CSV_FMT, SKIP_HEADER = 0)
ON_ERROR    = ABORT_STATEMENT;

-- ============================================================
-- CUSTOMER_EXPERIENCE_COMMENTS  (no header row; gzip compressed)
-- ============================================================
CREATE OR REPLACE TABLE CUSTOMER_EXPERIENCE_COMMENTS (
    COMMENT_ID    NUMBER        NOT NULL PRIMARY KEY,
    COMMENT_DATE  DATE,
    ARTICLE_ID    NUMBER        REFERENCES DIM_ARTICLE(ARTICLE_ID),
    ARTICLE_NAME  VARCHAR(255),
    COMMENT_TEXT  VARCHAR(4000) NOT NULL
);

COPY INTO CUSTOMER_EXPERIENCE_COMMENTS
         (COMMENT_ID, COMMENT_DATE, ARTICLE_ID, ARTICLE_NAME, COMMENT_TEXT)
FROM (
    SELECT $1::NUMBER, $2::DATE, $3::NUMBER,
           $4::VARCHAR, $5::VARCHAR
    FROM @CSV_STAGE/customer_experience_comments.csv_0_0_0.csv.gz
)
FILE_FORMAT = (FORMAT_NAME = CSV_FMT, SKIP_HEADER = 0)
ON_ERROR    = ABORT_STATEMENT;

-- ============================================================
-- Verification
-- ============================================================
SELECT 'DIM_ARTICLE'                  AS table_name, COUNT(*) AS row_count FROM DIM_ARTICLE
UNION ALL
SELECT 'DIM_CUSTOMER',                               COUNT(*) FROM DIM_CUSTOMER
UNION ALL
SELECT 'DIM_SHOP',                                   COUNT(*) FROM DIM_SHOP
UNION ALL
SELECT 'FACT_SALES',                                 COUNT(*) FROM FACT_SALES
UNION ALL
SELECT 'CUSTOMER_EXPERIENCE_COMMENTS',               COUNT(*) FROM CUSTOMER_EXPERIENCE_COMMENTS
ORDER BY 1;

-- Verify SHOP_ID distribution by article category
SELECT
    da.ARTICLE_CATEGORY,
    da.ARTICLE_NAME,
    ds.SHOP_CITY,
    ds.SHOP_COUNTRY,
    ds.IS_MOUNTAIN,
    COUNT(*) AS sale_count
FROM FACT_SALES fs
JOIN DIM_ARTICLE da ON fs.ARTICLE_ID = da.ARTICLE_ID
JOIN DIM_SHOP    ds ON fs.SHOP_ID    = ds.SHOP_ID
GROUP BY da.ARTICLE_CATEGORY, da.ARTICLE_NAME, ds.SHOP_CITY, ds.SHOP_COUNTRY, ds.IS_MOUNTAIN
ORDER BY da.ARTICLE_CATEGORY, da.ARTICLE_NAME, sale_count DESC;


-- ============================================================
-- SOURCE: 07_cortex_search.sql
-- ============================================================

-- ============================================================
-- STEP 7: CORTEX SEARCH SERVICES
-- Run as: ACCOUNTADMIN
-- Creates both Cortex Search Services in a single script.
-- Must run AFTER step 04 (DOCS_CHUNKS_TABLE) and
-- step 06 (CUSTOMER_EXPERIENCE_COMMENTS).
--
-- Services:
--   DOCUMENTATION_TOOL       - PDF chunks + image descriptions
--     Source : DOCS_CHUNKS_TABLE
--     Search : CHUNK_TEXT
--     Filters: SOURCE_FILE, FILE_TYPE, PRODUCT_CATEGORY, CHUNK_INDEX, STAGE_NAME
--
--   CUSTOMER_FEEDBACK_TOOL   - Product reviews and feedback
--     Source : CUSTOMER_EXPERIENCE_COMMENTS
--     Search : COMMENT_TEXT
--     Filters: COMMENT_ID, COMMENT_DATE, ARTICLE_ID, ARTICLE_NAME
--
--   Embedding model: snowflake-arctic-embed-l-v2.0
--   Refresh lag    : 1 day
-- ============================================================

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E;
USE SCHEMA PUBLIC;

-- ============================================================
-- DOCUMENTATION_TOOL
-- Covers all PDF chunks and image descriptions from DOCS_CHUNKS_TABLE
-- ============================================================
CREATE OR REPLACE CORTEX SEARCH SERVICE DOCUMENTATION_TOOL
    ON CHUNK_TEXT
    ATTRIBUTES SOURCE_FILE, FILE_TYPE, PRODUCT_CATEGORY, CHUNK_INDEX, STAGE_NAME
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '1 day'
    EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
AS (
    SELECT
        CHUNK_TEXT,
        SOURCE_FILE,
        FILE_TYPE,
        PRODUCT_CATEGORY,
        CHUNK_INDEX,
        STAGE_NAME
    FROM DOCS_CHUNKS_TABLE
);

-- ============================================================
-- CUSTOMER_FEEDBACK_TOOL
-- Covers product reviews and feedback from CUSTOMER_EXPERIENCE_COMMENTS
-- ============================================================
CREATE OR REPLACE CORTEX SEARCH SERVICE CUSTOMER_FEEDBACK_TOOL
    ON COMMENT_TEXT
    ATTRIBUTES COMMENT_ID, COMMENT_DATE, ARTICLE_ID, ARTICLE_NAME
    WAREHOUSE = COMPUTE_WH
    TARGET_LAG = '1 day'
    EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
AS (
    SELECT
        COMMENT_ID,
        COMMENT_DATE,
        ARTICLE_ID,
        ARTICLE_NAME,
        COMMENT_TEXT
    FROM CUSTOMER_EXPERIENCE_COMMENTS
);

-- ============================================================
-- Verification
-- ============================================================
SHOW CORTEX SEARCH SERVICES IN SCHEMA CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC;

-- Preview: documentation - bike query
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC.DOCUMENTATION_TOOL',
        '{
            "query": "bike features and specifications",
            "columns": ["CHUNK_TEXT", "SOURCE_FILE", "FILE_TYPE", "PRODUCT_CATEGORY"],
            "limit": 3
        }'
    )
)['results'] AS bike_doc_results;

-- Preview: documentation - snow query
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC.DOCUMENTATION_TOOL',
        '{
            "query": "ski specifications and performance",
            "columns": ["CHUNK_TEXT", "SOURCE_FILE", "FILE_TYPE", "PRODUCT_CATEGORY"],
            "limit": 3
        }'
    )
)['results'] AS snow_doc_results;

-- Preview: customer feedback - bike query
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC.CUSTOMER_FEEDBACK_TOOL',
        '{
            "query": "bike frame quality and warranty issues",
            "columns": ["COMMENT_TEXT", "ARTICLE_NAME", "COMMENT_DATE"],
            "limit": 3
        }'
    )
)['results'] AS bike_feedback_results;

-- Preview: customer feedback - snow query
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'CC_CoCo_SNOWFLAKE_INTELLIGENCE_E2E.PUBLIC.CUSTOMER_FEEDBACK_TOOL',
        '{
            "query": "ski performance and quality",
            "columns": ["COMMENT_TEXT", "ARTICLE_NAME", "COMMENT_DATE"],
            "limit": 3
        }'
    )
)['results'] AS ski_feedback_results;
