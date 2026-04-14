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
