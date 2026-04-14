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
