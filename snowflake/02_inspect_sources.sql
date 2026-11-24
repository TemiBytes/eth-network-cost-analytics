/*
===============================================================================
File:        02_inspect_sources.sql
Project:     Ethereum Blockchain Analytics
Layer:       Raw
Purpose:     Inspect and understand the Ethereum source data before ingestion.

Objectives:
    1. Verify access to the external S3 stage.
    2. Assess historical data availability and file sizes.
    3. Discover the schema of each source dataset.
    4. Preview raw records to understand the data structure.
    5. Explore important transaction fields and edge cases.

Source:      AWS Public Blockchain Dataset (Ethereum)
Format:      Parquet
Data window: 2026-10-01 to 2026-11-15 (inclusive)

Note:
    This script is for data exploration only.
    It does not create tables or load data into Snowflake.
===============================================================================
*/


-- =============================================================================
-- 01. SESSION SETUP
-- =============================================================================
-- Set the role, warehouse, database and schema used for source inspection.

use role loader;
use warehouse loading_wh;
use database raw;
use schema ethereum;


-- =============================================================================
-- 02. STAGE VERIFICATION
-- =============================================================================
-- Confirm that the external stage is accessible and contains source files.
-- Use a single date to avoid listing unnecessary historical files.

list @raw.ethereum.eth_stage/transactions/date=2026-10-01/;


-- Summarise the number and compressed size of files for the selected date.
-- RESULT_SCAN allows us to query the output of the preceding LIST command.

select
    count(*) as file_count,
    sum("size") as total_bytes,
    round(sum("size") / 1000000, 2) as total_mb
from table(result_scan(last_query_id()));


-- =============================================================================
-- 03. HISTORICAL DATA ASSESSMENT
-- =============================================================================
-- Assess the availability and volume of data within the historical window
-- before loading it into Snowflake.
--
-- Historical window: October 1 to November 15, 2026
-- Expected number of days: 46
--
-- Metrics:
--     file_count     : Total number of source files
--     available_days : Number of distinct dates with available files
--     total_gb       : Total compressed file size in gigabytes
--
-- Run each LIST statement immediately before its corresponding summary query.


-- Define the historical date pattern once for all four datasets.
-- Includes all dates in October and November 1 through November 15.

set historical_pattern =
    '.*date=2026-(10-[0-9]{2}|11-(0[1-9]|1[0-5]))/.*';


-- -----------------------------------------------------------------------------
-- 03.1 Blocks
-- -----------------------------------------------------------------------------

list @raw.ethereum.eth_stage/blocks/
    pattern = $historical_pattern;

select
    'blocks' as source_name,
    count(*) as file_count,
    count(distinct regexp_substr(
        "name",
        'date=[0-9]{4}-[0-9]{2}-[0-9]{2}'
    )) as available_days,
    round(sum("size") / 1000000000, 2) as total_gb
from table(result_scan(last_query_id()));


-- -----------------------------------------------------------------------------
-- 03.2 Transactions
-- -----------------------------------------------------------------------------

list @raw.ethereum.eth_stage/transactions/
    pattern = $historical_pattern;

select
    'transactions' as source_name,
    count(*) as file_count,
    count(distinct regexp_substr(
        "name",
        'date=[0-9]{4}-[0-9]{2}-[0-9]{2}'
    )) as available_days,
    round(sum("size") / 1000000000, 2) as total_gb
from table(result_scan(last_query_id()));


-- -----------------------------------------------------------------------------
-- 03.3 Contracts
-- -----------------------------------------------------------------------------

list @raw.ethereum.eth_stage/contracts/
    pattern = $historical_pattern;

select
    'contracts' as source_name,
    count(*) as file_count,
    count(distinct regexp_substr(
        "name",
        'date=[0-9]{4}-[0-9]{2}-[0-9]{2}'
    )) as available_days,
    round(sum("size") / 1000000000, 2) as total_gb
from table(result_scan(last_query_id()));


-- -----------------------------------------------------------------------------
-- 03.4 Token transfers
-- -----------------------------------------------------------------------------

list @raw.ethereum.eth_stage/token_transfers/
    pattern = $historical_pattern;

select
    'token_transfers' as source_name,
    count(*) as file_count,
    count(distinct regexp_substr(
        "name",
        'date=[0-9]{4}-[0-9]{2}-[0-9]{2}'
    )) as available_days,
    round(sum("size") / 1000000000, 2) as total_gb
from table(result_scan(last_query_id()));


-- =============================================================================
-- 04. SOURCE SCHEMA DISCOVERY
-- =============================================================================
-- Use INFER_SCHEMA to identify the columns, data types and nullability
-- of each Parquet dataset before defining the raw source tables.
--
-- Inspect only one file per dataset to limit unnecessary processing.
-- This provides an initial understanding of the source schema, but does
-- not guarantee schema consistency across the entire historical window.


-- 04.1 Blocks

select column_name, type, nullable
from table(
    infer_schema(
        location => '@raw.ethereum.eth_stage/blocks/date=2026-10-01/',
        file_format => 'RAW.ETHEREUM.PARQUET_FORMAT',
        max_file_count => 1
    )
)
order by order_id;


-- 04.2 Transactions

select column_name, type, nullable
from table(
    infer_schema(
        location => '@raw.ethereum.eth_stage/transactions/date=2026-10-01/',
        file_format => 'RAW.ETHEREUM.PARQUET_FORMAT',
        max_file_count => 1
    )
)
order by order_id;


-- 04.3 Contracts

select column_name, type, nullable
from table(
    infer_schema(
        location => '@raw.ethereum.eth_stage/contracts/date=2026-10-01/',
        file_format => 'RAW.ETHEREUM.PARQUET_FORMAT',
        max_file_count => 1
    )
)
order by order_id;


-- 04.4 Token transfers

select column_name, type, nullable
from table(
    infer_schema(
        location => '@raw.ethereum.eth_stage/token_transfers/date=2026-10-01/',
        file_format => 'RAW.ETHEREUM.PARQUET_FORMAT',
        max_file_count => 1
    )
)
order by order_id;


-- =============================================================================
-- 05. RAW SOURCE DATA PREVIEW
-- =============================================================================
-- Preview five records from each source directly from the external stage.
-- For staged Parquet files, $1 represents the raw record as a VARIANT.
--
-- This helps us understand actual field values, nested structures,
-- data formats and potential data quality issues before ingestion.


-- 05.1 Blocks

select $1 as raw_record
from @raw.ethereum.eth_stage/blocks/date=2026-10-01/
limit 5;


-- 05.2 Transactions

select $1 as raw_record
from @raw.ethereum.eth_stage/transactions/date=2026-10-01/
limit 5;


-- 05.3 Contracts

select $1 as raw_record
from @raw.ethereum.eth_stage/contracts/date=2026-10-01/
limit 5;


-- 05.4 Token transfers

select $1 as raw_record
from @raw.ethereum.eth_stage/token_transfers/date=2026-10-01/
limit 5;


-- =============================================================================
-- 06. TRANSACTION DATA EXPLORATION
-- =============================================================================
-- Explore important transaction fields and behaviours that may influence
-- downstream dbt transformations, data quality tests and business logic.
--
-- Focus areas:
--     - Transaction identifiers and wallet addresses
--     - Transaction values and gas fees
--     - Transactions without input data
--     - Potential smart contract creation transactions
--     - Null and empty address handling


-- -----------------------------------------------------------------------------
-- 06.1 Inspect key transaction fields
-- -----------------------------------------------------------------------------
-- Extract individual fields from the raw Parquet record to understand
-- their values and representation before defining transformation logic.

select
    $1:hash::varchar as transaction_hash,
    $1:from_address::varchar as from_address,
    $1:to_address::varchar as to_address,
    $1:input::varchar as transaction_input,
    $1:gas_price as gas_price,
    $1:receipt_effective_gas_price as effective_gas_price,
    $1:receipt_gas_used as gas_used,
    $1:value as transaction_value,
    $1:date as transaction_date,
    $1:last_modified as last_modified
from @raw.ethereum.eth_stage/transactions/date=2026-10-01/
limit 10;


-- -----------------------------------------------------------------------------
-- 06.2 Inspect transactions with empty input data
-- -----------------------------------------------------------------------------
-- An input value of '0x' indicates that no input data was supplied.
-- These are commonly associated with native ETH transfers, although
-- empty input alone does not establish the transaction type.

select
    $1:hash::varchar as transaction_hash,
    $1:from_address::varchar as from_address,
    $1:to_address::varchar as to_address,
    $1:input::varchar as transaction_input,
    $1:value as transaction_value
from @raw.ethereum.eth_stage/transactions/date=2026-10-01/
where $1:input::varchar = '0x'
limit 5;


-- -----------------------------------------------------------------------------
-- 06.3 Inspect potential contract creation transactions
-- -----------------------------------------------------------------------------
-- Contract creation transactions do not have an existing recipient address.
-- Inspect missing or empty recipient addresses and the contract address
-- returned in the transaction receipt.
--
-- Check how missing addresses are represented:
--     - SQL NULL
--     - JSON null within a VARIANT
--     - Empty string
--
-- Missing recipient addresses are candidates for investigation, rather
-- than confirmation of successful contract creation.

select
    $1:hash::varchar as transaction_hash,
    $1:to_address::varchar as to_address,
    $1:receipt_contract_address::varchar as contract_address,
    left($1:input::varchar, 100) as input_preview,
    typeof($1:to_address) as address_type
from @raw.ethereum.eth_stage/transactions/date=2026-10-01/
where
    $1:to_address is null
    or is_null_value($1:to_address)
    or $1:to_address::varchar = ''
limit 5;