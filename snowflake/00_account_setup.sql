/*
===============================================================================
File:        00_setup.sql
Project:     Ethereum Blockchain Analytics
Layer:       Infrastructure
Purpose:     Configure the Snowflake environment required for the project.

Objectives:
    1. Establish credit consumption controls.
    2. Create warehouses for loading, transformation and reporting.
    3. Create databases for raw and transformed data.
    4. Configure role-based access and permissions.
    5. Create the required source schemas.
    6. Validate permissions and infrastructure configuration.

Note:
    Run this script before configuring the external stage or loading data.
    Database ownership transfers are intended for initial setup only.
    This script does not ingest any blockchain data.
===============================================================================
*/


-- =============================================================================
-- 01. RESOURCE MONITOR
-- =============================================================================
-- Protect the Snowflake trial account from excessive warehouse credit usage.
--
-- Credit quota: 100 credits
-- Frequency: NEVER (the quota does not automatically reset)
--
-- Thresholds:
--     50%  : Send a notification.
--     80%  : Send a second notification.
--     100% : Suspend warehouses immediately, cancelling running queries.
--
-- ACCOUNTADMIN is required to configure an account-level resource monitor.

use role accountadmin;

create resource monitor if not exists trial_monitor
with
    credit_quota = 100
    frequency = never
    start_timestamp = immediately
triggers
    on 50 percent do notify
    on 80 percent do notify
    on 100 percent do suspend_immediate;


-- Apply the resource monitor across all virtual warehouses in the account.
-- This includes warehouses created later in the project.

alter account set resource_monitor = trial_monitor;


-- =============================================================================
-- 02. WAREHOUSES AND DATABASES
-- =============================================================================
-- Separate compute resources by workload to isolate data ingestion,
-- dbt transformations and analytical queries.
--
-- All warehouses start with an XSMALL size to minimise credit consumption.
-- AUTO_SUSPEND stops idle warehouses after 60 seconds.
-- AUTO_RESUME automatically starts a warehouse when a query requires it.

use role sysadmin;


-- -----------------------------------------------------------------------------
-- 02.1 Loading warehouse
-- -----------------------------------------------------------------------------
-- Used to inspect and ingest Ethereum source data from AWS S3.

create warehouse if not exists loading_wh
    warehouse_size = 'XSMALL'
    auto_suspend = 60
    auto_resume = true
    initially_suspended = true
    comment = 'Ethereum source data inspection and ingestion';


-- -----------------------------------------------------------------------------
-- 02.2 Transformation warehouse
-- -----------------------------------------------------------------------------
-- Used by dbt to build, test and transform analytical models.

create warehouse if not exists transforming_wh
    warehouse_size = 'XSMALL'
    auto_suspend = 60
    auto_resume = true
    initially_suspended = true
    comment = 'dbt transformations and testing';


-- -----------------------------------------------------------------------------
-- 02.3 Reporting warehouse
-- -----------------------------------------------------------------------------
-- Used for downstream analytical queries from Tableau and MCP consumers.

create warehouse if not exists reporting_wh
    warehouse_size = 'XSMALL'
    auto_suspend = 60
    auto_resume = true
    initially_suspended = true
    comment = 'Tableau and MCP analytical queries';


-- -----------------------------------------------------------------------------
-- 02.4 Databases
-- -----------------------------------------------------------------------------
-- Separate landed source data from transformed analytical models.
--
-- RAW       : Stores source data before dbt transformations.
-- ANALYTICS : Stores dbt staging, intermediate and mart models.

create database if not exists raw
    comment = 'Raw Ethereum blockchain source data';

create database if not exists analytics
    comment = 'dbt models and analytical marts';


-- =============================================================================
-- 03. ROLES AND PERMISSIONS
-- =============================================================================
-- Implement role-based access control (RBAC) to separate responsibilities
-- and grant each role only the permissions required for its workload.
--
-- LOADER      : Manages raw data ingestion.
-- TRANSFORMER : Reads raw data and manages dbt models.
-- REPORTER    : Consumes approved analytical models through Tableau.
-- ASSISTANT   : Consumes approved analytical models through MCP.
--
-- SECURITYADMIN manages role creation and privilege assignments.

use role securityadmin;


-- -----------------------------------------------------------------------------
-- 03.1 Create custom roles
-- -----------------------------------------------------------------------------

create role if not exists loader
    comment = 'Loads and manages raw Ethereum source data';

create role if not exists transformer
    comment = 'Runs dbt transformations and manages analytical models';

create role if not exists reporter
    comment = 'Read-only analytical access for Tableau';

create role if not exists assistant
    comment = 'Read-only analytical access for MCP';


-- -----------------------------------------------------------------------------
-- 03.2 Establish role hierarchy
-- -----------------------------------------------------------------------------
-- Grant custom roles to SYSADMIN so that their privileges are inherited
-- by the system administration role.

grant role loader to role sysadmin;
grant role transformer to role sysadmin;
grant role reporter to role sysadmin;
grant role assistant to role sysadmin;


-- -----------------------------------------------------------------------------
-- 03.3 Assign roles to the current user
-- -----------------------------------------------------------------------------
-- Allow the current project developer to switch between custom roles
-- when performing ingestion, transformation and reporting tasks.
--
-- CURRENT_USER avoids hardcoding a specific Snowflake username.

set my_user = current_user();

grant role loader to user identifier($my_user);
grant role transformer to user identifier($my_user);
grant role reporter to user identifier($my_user);
grant role assistant to user identifier($my_user);


-- -----------------------------------------------------------------------------
-- 03.4 Grant warehouse access
-- -----------------------------------------------------------------------------
-- Each role receives access to the warehouse required for its workload.
-- REPORTER and ASSISTANT share the reporting warehouse.

grant usage on warehouse loading_wh
    to role loader;

grant usage on warehouse transforming_wh
    to role transformer;

grant usage on warehouse reporting_wh
    to role reporter;

grant usage on warehouse reporting_wh
    to role assistant;


-- -----------------------------------------------------------------------------
-- 03.5 Assign database ownership
-- -----------------------------------------------------------------------------
-- LOADER owns the RAW database.
-- TRANSFORMER owns the ANALYTICS database.
--
-- COPY CURRENT GRANTS preserves existing database privileges during
-- the ownership transfer.
--
-- IMPORTANT: Run these ownership transfers once during initial setup.

grant ownership on database raw
    to role loader
    copy current grants;

grant ownership on database analytics
    to role transformer
    copy current grants;


-- -----------------------------------------------------------------------------
-- 03.6 Grant TRANSFORMER read access to RAW
-- -----------------------------------------------------------------------------
-- dbt requires access to raw source data to build analytical models.
-- The TRANSFORMER role receives read access but is not granted permissions
-- to insert, update, delete or create objects in RAW.
--
-- Existing grants cover objects that already exist.
-- Future grants automatically cover newly created schemas, tables and views.

grant usage on database raw
    to role transformer;


-- Schema access

grant usage on all schemas in database raw
    to role transformer;

grant usage on future schemas in database raw
    to role transformer;


-- Table access

grant select on all tables in database raw
    to role transformer;

grant select on future tables in database raw
    to role transformer;


-- View access

grant select on all views in database raw
    to role transformer;

grant select on future views in database raw
    to role transformer;


-- -----------------------------------------------------------------------------
-- 03.7 Grant reporting consumers database access
-- -----------------------------------------------------------------------------
-- REPORTER and ASSISTANT receive visibility of the ANALYTICS database only.
--
-- Access to specific schemas and analytical models will be configured
-- later when the dbt project is developed.
--
-- This prevents reporting consumers from automatically accessing every
-- staging and intermediate model in the database.

grant usage on database analytics
    to role reporter;

grant usage on database analytics
    to role assistant;


-- =============================================================================
-- 04. RAW SCHEMA CREATION
-- =============================================================================
-- Organise raw data into schemas based on its source and purpose.
--
-- ETHEREUM  : Ethereum blockchain source tables and external stage objects.
-- REFERENCE : Supporting reference datasets and controlled test data.

use role loader;

create schema if not exists raw.ethereum
    comment = 'Ethereum blockchain source data and external stage objects';

create schema if not exists raw.reference
    comment = 'Reference datasets and controlled test data';


-- =============================================================================
-- 05. PERMISSION SMOKE TESTS
-- =============================================================================
-- Perform basic tests to verify that the custom roles have the permissions
-- required to perform their intended responsibilities.
--
-- Disable secondary roles so each test uses only the active primary role.
-- Run these statements in order and stop if any test fails.


-- -----------------------------------------------------------------------------
-- 05.1 Verify LOADER can create a raw table
-- -----------------------------------------------------------------------------
-- Create a disposable test table to confirm that LOADER can manage
-- objects within the RAW.ETHEREUM schema.

use role loader;
use secondary roles none;
use warehouse loading_wh;

create table raw.ethereum._setup_permission_test (
    id number
);


-- -----------------------------------------------------------------------------
-- 05.2 Verify TRANSFORMER can read raw data
-- -----------------------------------------------------------------------------
-- Switch to TRANSFORMER and query the table created by LOADER.
-- This also verifies that the future table SELECT grant is working.
--
-- Expected result: 0 rows, since the test table is empty.

use role transformer;
use warehouse transforming_wh;

select count(*) as row_count
from raw.ethereum._setup_permission_test;


-- -----------------------------------------------------------------------------
-- 05.3 Verify TRANSFORMER can create analytical schemas
-- -----------------------------------------------------------------------------
-- Confirm that the dbt role can create its own development schema
-- within the ANALYTICS database.

create schema if not exists analytics.dbt_dev
    comment = 'dbt development environment';


-- -----------------------------------------------------------------------------
-- 05.4 Clean up permission test objects
-- -----------------------------------------------------------------------------
-- Remove the disposable test table.
-- Retain ANALYTICS.DBT_DEV for subsequent dbt development.

use role loader;
use warehouse loading_wh;

drop table if exists raw.ethereum._setup_permission_test;


-- =============================================================================
-- 06. FINAL VERIFICATION
-- =============================================================================
-- Review the configured infrastructure, role privileges and schemas
-- before proceeding to the external stage configuration.

use role accountadmin;


-- 06.1 Verify resource monitor configuration

show resource monitors;


-- 06.2 Verify warehouse configuration

show warehouses;


-- 06.3 Verify custom role permissions

show grants to role loader;

show grants to role transformer;

show grants to role reporter;

show grants to role assistant;


-- 06.4 Verify future grants on the RAW database

show future grants in database raw;


-- 06.5 Verify database schemas

show schemas in database raw;

show schemas in database analytics;


-- =============================================================================
-- SETUP COMPLETE
-- =============================================================================
-- Next step: 01_ethereum_stage.sql
-- Configure the Parquet file format and external AWS S3 stage.
-- =============================================================================