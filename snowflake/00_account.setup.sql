-- ============================================================
-- 1. RESOURCE MONITOR
-- ============================================================

USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS TRIAL_MONITOR
WITH
    CREDIT_QUOTA = 100
    FREQUENCY = NEVER
    START_TIMESTAMP = IMMEDIATELY
TRIGGERS
    ON 50 PERCENT DO NOTIFY
    ON 80 PERCENT DO NOTIFY
    ON 100 PERCENT DO SUSPEND_IMMEDIATE;

-- Apply the monitor across account virtual warehouses.
ALTER ACCOUNT SET RESOURCE_MONITOR = TRIAL_MONITOR;


-- ============================================================
-- 2. WAREHOUSES AND DATABASES
-- ============================================================

USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS LOADING_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Loading NYC TLC raw data';


CREATE WAREHOUSE IF NOT EXISTS TRANSFORMING_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'dbt transformations and testing';


CREATE WAREHOUSE IF NOT EXISTS REPORTING_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Tableau and MCP analytical queries';


CREATE DATABASE IF NOT EXISTS RAW
    COMMENT = 'NYC TLC landed source data';

CREATE DATABASE IF NOT EXISTS ANALYTICS
    COMMENT = 'dbt models and analytical marts';


-- ============================================================
-- 3. ROLES AND PERMISSIONS
-- ============================================================

USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS LOADER
    COMMENT = 'Loads and manages RAW data';

CREATE ROLE IF NOT EXISTS TRANSFORMER
    COMMENT = 'Runs dbt and manages ANALYTICS';

CREATE ROLE IF NOT EXISTS REPORTER
    COMMENT = 'Tableau read-only consumer';

CREATE ROLE IF NOT EXISTS ASSISTANT
    COMMENT = 'MCP read-only consumer';

-- Add custom roles to the SYSADMIN hierarchy.
GRANT ROLE LOADER TO ROLE SYSADMIN;
GRANT ROLE TRANSFORMER TO ROLE SYSADMIN;
GRANT ROLE REPORTER TO ROLE SYSADMIN;
GRANT ROLE ASSISTANT TO ROLE SYSADMIN;

-- Allow the current user to assume these roles.
SET MY_USER = CURRENT_USER();

GRANT ROLE LOADER TO USER IDENTIFIER($MY_USER);
GRANT ROLE TRANSFORMER TO USER IDENTIFIER($MY_USER);
GRANT ROLE REPORTER TO USER IDENTIFIER($MY_USER);
GRANT ROLE ASSISTANT TO USER IDENTIFIER($MY_USER);

-- Warehouse access.
GRANT USAGE ON WAREHOUSE LOADING_WH
    TO ROLE LOADER;

GRANT USAGE ON WAREHOUSE TRANSFORMING_WH
    TO ROLE TRANSFORMER;

GRANT USAGE ON WAREHOUSE REPORTING_WH
    TO ROLE REPORTER;

GRANT USAGE ON WAREHOUSE REPORTING_WH
    TO ROLE ASSISTANT;

-- Transfer database ownership.
-- Run ownership transfers once during initial setup.
GRANT OWNERSHIP ON DATABASE RAW
    TO ROLE LOADER
    COPY CURRENT GRANTS;

GRANT OWNERSHIP ON DATABASE ANALYTICS
    TO ROLE TRANSFORMER
    COPY CURRENT GRANTS;

-- dbt can read RAW but not modify its existing objects.
GRANT USAGE ON DATABASE RAW
    TO ROLE TRANSFORMER;

GRANT USAGE ON ALL SCHEMAS IN DATABASE RAW
    TO ROLE TRANSFORMER;

GRANT USAGE ON FUTURE SCHEMAS IN DATABASE RAW
    TO ROLE TRANSFORMER;

GRANT SELECT ON ALL TABLES IN DATABASE RAW
    TO ROLE TRANSFORMER;

GRANT SELECT ON FUTURE TABLES IN DATABASE RAW
    TO ROLE TRANSFORMER;

GRANT SELECT ON ALL VIEWS IN DATABASE RAW
    TO ROLE TRANSFORMER;

GRANT SELECT ON FUTURE VIEWS IN DATABASE RAW
    TO ROLE TRANSFORMER;

-- Reporting consumers receive database visibility only.
-- Schema and table access comes later through dbt's grants config.
GRANT USAGE ON DATABASE ANALYTICS
    TO ROLE REPORTER;

GRANT USAGE ON DATABASE ANALYTICS
    TO ROLE ASSISTANT;


-- ============================================================
-- 4. RAW SCHEMAS
-- ============================================================

USE ROLE LOADER;

CREATE SCHEMA IF NOT EXISTS RAW.TLC
    COMMENT = 'Taxi trip data from NYC TLC';

CREATE SCHEMA IF NOT EXISTS RAW.REFERENCE
    COMMENT = 'Reference and controlled test data';

-- ============================================================
-- 5. PERMISSION SMOKE TEST
-- ============================================================

-- Verify LOADER can create a raw table.
USE ROLE LOADER;

CREATE TABLE IF NOT EXISTS RAW.TLC._SETUP_PERMISSION_TEST (
    ID NUMBER
);

-- Verify TRANSFORMER can read RAW.
USE ROLE TRANSFORMER;
USE WAREHOUSE TRANSFORMING_WH;

SELECT COUNT(*)
FROM RAW.TLC._SETUP_PERMISSION_TEST;

-- Verify dbt role can create its own development schema.
CREATE SCHEMA IF NOT EXISTS ANALYTICS.DBT_DEV;

-- Remove temporary test table.
USE ROLE LOADER;

DROP TABLE IF EXISTS RAW.TLC._SETUP_PERMISSION_TEST;


-- ============================================================
-- 6. FINAL VERIFICATION
-- ============================================================

USE ROLE ACCOUNTADMIN;

SHOW RESOURCE MONITORS;

SHOW GRANTS TO ROLE TRANSFORMER;

SHOW GRANTS TO ROLE LOADER;

SHOW FUTURE GRANTS IN DATABASE RAW;

SHOW SCHEMAS IN DATABASE RAW;

SHOW SCHEMAS IN DATABASE ANALYTICS;

-- ============================================================
-- SETUP COMPLETE
-- ============================================================
