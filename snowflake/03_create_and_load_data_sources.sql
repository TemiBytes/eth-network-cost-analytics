/*
===============================================================================
File:        03_create_and_load_sources.sql
Project:     Ethereum Blockchain Analytics
Layer:       Raw
Purpose:     Create, populate and validate the raw Ethereum source tables.

Objectives:
    1. Create the four raw source tables using explicit column definitions.
    2. Define the historical data ingestion window.
    3. Load Ethereum Parquet files from AWS S3 into Snowflake.
    4. Capture ingestion metadata for data lineage and freshness tracking.
    5. Validate the completeness and volume of the loaded data.
    6. Confirm that the dbt transformation role can access the raw tables.

Source:      AWS Public Blockchain Dataset (Ethereum)
Format:      Parquet
Data window: 2026-10-01 to 2026-11-15 (inclusive)
Expected historical coverage: 46 days

Note:
    The raw layer preserves the source data without applying business logic.
    Data cleaning, unit conversions, deduplication and analytical
    transformations will be handled within the dbt project.
===============================================================================
*/


-- =============================================================================
-- 01. SESSION SETUP
-- =============================================================================
-- Establish the Snowflake execution context for raw data ingestion.

use role loader;
use warehouse loading_wh;
use database raw;
use schema ethereum;


-- =============================================================================
-- 02. CREATE RAW SOURCE TABLES
-- =============================================================================
-- Define the four raw tables based on the schemas discovered during
-- source inspection in 02_inspect_sources.sql.
--
-- Each table contains three additional ingestion metadata columns:
--
--     _source_file        : Path of the original Parquet file.
--     _file_last_modified : When the source file was last modified.
--     _loaded_at          : When Snowflake started scanning the file
--                           during ingestion.
--
-- These columns support data lineage, troubleshooting, freshness checks
-- and future incremental processing.
--
-- IF NOT EXISTS prevents existing tables from being replaced.


-- -----------------------------------------------------------------------------
-- 02.1 Blocks
-- -----------------------------------------------------------------------------
-- Contains information about Ethereum blocks, including block identifiers,
-- timestamps, gas consumption and transaction counts.
--
-- Expected grain: One record per blockchain block.

create table if not exists raw.ethereum.blocks (
    difficulty          float,
    hash                varchar,
    miner               varchar,
    nonce               varchar,
    number              number(38, 0),
    size                number(38, 0),
    timestamp           timestamp_ntz,
    base_fee_per_gas    number(38, 0),
    gas_limit           number(38, 0),
    gas_used            number(38, 0),
    extra_data          varchar,
    logs_bloom          varchar,
    parent_hash         varchar,
    state_root          varchar,
    receipts_root       varchar,
    transactions_root   varchar,
    sha3_uncles         varchar,
    transaction_count   number(38, 0),
    date                varchar,
    last_modified       timestamp_ntz,

    -- Ingestion metadata
    _source_file        varchar,
    _file_last_modified timestamp_ntz,
    _loaded_at          timestamp_ltz
);


-- -----------------------------------------------------------------------------
-- 02.2 Transactions
-- -----------------------------------------------------------------------------
-- Contains individual Ethereum transactions, including sender and recipient
-- addresses, transaction values, gas fees and execution status.
--
-- Expected grain: One record per transaction.

create table if not exists raw.ethereum.transactions (
    gas                         number(38, 0),
    hash                        varchar,
    input                       varchar,
    nonce                       number(38, 0),
    value                       float,
    block_number                number(38, 0),
    block_hash                  varchar,
    transaction_index           number(38, 0),
    from_address                varchar,
    to_address                  varchar,
    gas_price                   number(38, 0),
    receipt_cumulative_gas_used number(38, 0),
    receipt_gas_used            number(38, 0),
    receipt_contract_address    varchar,
    receipt_status              number(38, 0),
    receipt_effective_gas_price number(38, 0),
    transaction_type            number(38, 0),
    max_fee_per_gas             number(38, 0),
    max_priority_fee_per_gas    number(38, 0),
    block_timestamp             timestamp_ntz,
    date                        varchar,
    last_modified               timestamp_ntz,

    -- Ingestion metadata
    _source_file                varchar,
    _file_last_modified         timestamp_ntz,
    _loaded_at                  timestamp_ltz
);


-- -----------------------------------------------------------------------------
-- 02.3 Contracts
-- -----------------------------------------------------------------------------
-- Contains information about deployed Ethereum smart contracts and
-- the blocks associated with their creation.
--
-- Expected grain: One record per contract deployment.

create table if not exists raw.ethereum.contracts (
    address             varchar,
    block_number        number(38, 0),
    block_hash          varchar,
    block_timestamp     timestamp_ntz,
    date                varchar,
    last_modified       timestamp_ntz,

    -- Ingestion metadata
    _source_file        varchar,
    _file_last_modified timestamp_ntz,
    _loaded_at          timestamp_ltz
);


-- -----------------------------------------------------------------------------
-- 02.4 Token Transfers
-- -----------------------------------------------------------------------------
-- Contains token transfer events recorded on the Ethereum blockchain,
-- including token addresses, sender and recipient addresses, and values.
--
-- Expected grain: One record per token transfer event.

create table if not exists raw.ethereum.token_transfers (
    token_address       varchar,
    from_address        varchar,
    to_address          varchar,
    value               float,
    transaction_hash    varchar,
    log_index           number(38, 0),
    block_number        number(38, 0),
    block_hash          varchar,
    block_timestamp     timestamp_ntz,
    date                varchar,
    last_modified       timestamp_ntz,

    -- Ingestion metadata
    _source_file        varchar,
    _file_last_modified timestamp_ntz,
    _loaded_at          timestamp_ltz
);


-- -----------------------------------------------------------------------------
-- 02.5 Verify table creation
-- -----------------------------------------------------------------------------
-- Confirm that all four source tables exist with the expected columns
-- and data types before proceeding with ingestion.

show tables in schema raw.ethereum;

desc table raw.ethereum.blocks;
desc table raw.ethereum.transactions;
desc table raw.ethereum.contracts;
desc table raw.ethereum.token_transfers;


-- =============================================================================
-- 03. DEFINE HISTORICAL DATA WINDOW
-- =============================================================================
-- Restrict ingestion to files within the selected historical period.
--
-- Historical window: October 1 to November 15, 2026 (inclusive).
-- Expected coverage: 46 calendar days.
--
-- Pattern breakdown:
--     10-[0-9]{2}       : All October date partitions.
--     11-(0[1-9]|1[0-5]) : November 1 through November 15.
--
-- Define the pattern once and reuse it across all four datasets to
-- maintain a consistent ingestion window.

set historical_pattern =
    '.*date=2026-(10-[0-9]{2}|11-(0[1-9]|1[0-5]))/.*';


-- Verify the historical pattern before loading data.

select $historical_pattern as historical_pattern;


-- =============================================================================
-- 04. HISTORICAL DATA INGESTION
-- =============================================================================
-- Load the selected Parquet files from the external AWS S3 stage into
-- the corresponding Snowflake raw tables.
--
-- Explicit destination columns ensure that source fields are mapped
-- correctly, regardless of the physical column order in the table.
--
-- Snowflake generates the following metadata during ingestion:
--
--     METADATA$FILENAME           : Original source file path.
--     METADATA$FILE_LAST_MODIFIED : Source file modification timestamp.
--     METADATA$START_SCAN_TIME    : Start time of the file scan.
--
-- Copy options:
--     PATTERN  : Restricts ingestion to the historical window.
--     ON_ERROR : Aborts the current COPY statement if an error occurs.
--     FORCE    : Avoids reloading files recognised as already loaded.
--
-- Review the output of each COPY statement before proceeding.
-- Confirm that the expected files were loaded and no errors occurred.


-- -----------------------------------------------------------------------------
-- 04.1 Load Blocks
-- -----------------------------------------------------------------------------

copy into raw.ethereum.blocks (
    difficulty,
    hash,
    miner,
    nonce,
    number,
    size,
    timestamp,
    base_fee_per_gas,
    gas_limit,
    gas_used,
    extra_data,
    logs_bloom,
    parent_hash,
    state_root,
    receipts_root,
    transactions_root,
    sha3_uncles,
    transaction_count,
    date,
    last_modified,
    _source_file,
    _file_last_modified,
    _loaded_at
)
from (
    select
        t.$1:difficulty,
        t.$1:hash,
        t.$1:miner,
        t.$1:nonce,
        t.$1:number,
        t.$1:size,
        t.$1:timestamp,
        t.$1:base_fee_per_gas,
        t.$1:gas_limit,
        t.$1:gas_used,
        t.$1:extra_data,
        t.$1:logs_bloom,
        t.$1:parent_hash,
        t.$1:state_root,
        t.$1:receipts_root,
        t.$1:transactions_root,
        t.$1:sha3_uncles,
        t.$1:transaction_count,
        t.$1:date,
        t.$1:last_modified,

        -- Ingestion metadata
        metadata$filename,
        metadata$file_last_modified,
        metadata$start_scan_time
    from @raw.ethereum.eth_stage/blocks/ t
)
pattern = $historical_pattern
on_error = abort_statement
force = false;


-- -----------------------------------------------------------------------------
-- 04.2 Load Transactions
-- -----------------------------------------------------------------------------

copy into raw.ethereum.transactions (
    gas,
    hash,
    input,
    nonce,
    value,
    block_number,
    block_hash,
    transaction_index,
    from_address,
    to_address,
    gas_price,
    receipt_cumulative_gas_used,
    receipt_gas_used,
    receipt_contract_address,
    receipt_status,
    receipt_effective_gas_price,
    transaction_type,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    block_timestamp,
    date,
    last_modified,
    _source_file,
    _file_last_modified,
    _loaded_at
)
from (
    select
        t.$1:gas,
        t.$1:hash,
        t.$1:input,
        t.$1:nonce,
        t.$1:value,
        t.$1:block_number,
        t.$1:block_hash,
        t.$1:transaction_index,
        t.$1:from_address,
        t.$1:to_address,
        t.$1:gas_price,
        t.$1:receipt_cumulative_gas_used,
        t.$1:receipt_gas_used,
        t.$1:receipt_contract_address,
        t.$1:receipt_status,
        t.$1:receipt_effective_gas_price,
        t.$1:transaction_type,
        t.$1:max_fee_per_gas,
        t.$1:max_priority_fee_per_gas,
        t.$1:block_timestamp,
        t.$1:date,
        t.$1:last_modified,

        -- Ingestion metadata
        metadata$filename,
        metadata$file_last_modified,
        metadata$start_scan_time
    from @raw.ethereum.eth_stage/transactions/ t
)
pattern = $historical_pattern
on_error = abort_statement
force = false;


-- -----------------------------------------------------------------------------
-- 04.3 Load Contracts
-- -----------------------------------------------------------------------------

copy into raw.ethereum.contracts (
    address,
    block_number,
    block_hash,
    block_timestamp,
    date,
    last_modified,
    _source_file,
    _file_last_modified,
    _loaded_at
)
from (
    select
        t.$1:address,
        t.$1:block_number,
        t.$1:block_hash,
        t.$1:block_timestamp,
        t.$1:date,
        t.$1:last_modified,

        -- Ingestion metadata
        metadata$filename,
        metadata$file_last_modified,
        metadata$start_scan_time
    from @raw.ethereum.eth_stage/contracts/ t
)
pattern = $historical_pattern
on_error = abort_statement
force = false;


-- -----------------------------------------------------------------------------
-- 04.4 Load Token Transfers
-- -----------------------------------------------------------------------------

copy into raw.ethereum.token_transfers (
    token_address,
    from_address,
    to_address,
    value,
    transaction_hash,
    log_index,
    block_number,
    block_hash,
    block_timestamp,
    date,
    last_modified,
    _source_file,
    _file_last_modified,
    _loaded_at
)
from (
    select
        t.$1:token_address,
        t.$1:from_address,
        t.$1:to_address,
        t.$1:value,
        t.$1:transaction_hash,
        t.$1:log_index,
        t.$1:block_number,
        t.$1:block_hash,
        t.$1:block_timestamp,
        t.$1:date,
        t.$1:last_modified,

        -- Ingestion metadata
        metadata$filename,
        metadata$file_last_modified,
        metadata$start_scan_time
    from @raw.ethereum.eth_stage/token_transfers/ t
)
pattern = $historical_pattern
on_error = abort_statement
force = false;


-- =============================================================================
-- 05. POST-INGESTION VALIDATION
-- =============================================================================
-- Perform initial checks to confirm that the source data has been loaded
-- successfully and is available for downstream dbt development.
--
-- Detailed business-level data quality tests will be implemented in dbt.


-- -----------------------------------------------------------------------------
-- 05.1 Validate row counts and historical coverage
-- -----------------------------------------------------------------------------
-- Generate a consolidated summary of all four raw tables.
--
-- Review the following:
--     - Total number of records loaded.
--     - Number of distinct source dates represented in the table.
--     - Earliest and latest available dates.
--     - Number of distinct source files loaded.
--
-- Expected historical coverage: 46 days, assuming each source contains
-- records for every date in the selected period.
--
-- Compare the file counts against the source inspection results.

select
    'blocks' as table_name,
    count(*) as total_rows,
    count(distinct date) as available_days,
    min(date) as earliest_date,
    max(date) as latest_date,
    count(distinct _source_file) as files_loaded
from raw.ethereum.blocks

union all

select
    'transactions',
    count(*),
    count(distinct date),
    min(date),
    max(date),
    count(distinct _source_file)
from raw.ethereum.transactions

union all

select
    'contracts',
    count(*),
    count(distinct date),
    min(date),
    max(date),
    count(distinct _source_file)
from raw.ethereum.contracts

union all

select
    'token_transfers',
    count(*),
    count(distinct date),
    min(date),
    max(date),
    count(distinct _source_file)
from raw.ethereum.token_transfers;


-- -----------------------------------------------------------------------------
-- 05.2 Preview loaded records
-- -----------------------------------------------------------------------------
-- Inspect a small sample from each table to confirm that source fields
-- have been populated and mapped to the correct destination columns.

select * from raw.ethereum.blocks limit 5;

select * from raw.ethereum.transactions limit 5;

select * from raw.ethereum.contracts limit 5;

select * from raw.ethereum.token_transfers limit 5;


-- -----------------------------------------------------------------------------
-- 05.3 Inspect ingestion metadata
-- -----------------------------------------------------------------------------
-- Use transaction records to understand the difference between:
--
--     block_timestamp     : When the blockchain event occurred.
--     last_modified       : Source-provided modification timestamp.
--     _file_last_modified : When the original Parquet file was modified.
--     _loaded_at          : When Snowflake started scanning the file.
--
-- These timestamps will support source freshness monitoring and
-- incremental processing within dbt.

select
    hash,
    block_timestamp,
    date,
    last_modified,
    _source_file,
    _file_last_modified,
    _loaded_at
from raw.ethereum.transactions
limit 10;


-- -----------------------------------------------------------------------------
-- 05.4 Validate ingestion metadata completeness
-- -----------------------------------------------------------------------------
-- Confirm that the ingestion metadata has been populated for every
-- loaded record across all four raw tables.
--
-- Expected result: Zero records with missing ingestion metadata.

select
    'blocks' as table_name,
    count(*) as missing_metadata_rows
from raw.ethereum.blocks
where
    _source_file is null
    or _file_last_modified is null
    or _loaded_at is null

union all

select
    'transactions',
    count(*)
from raw.ethereum.transactions
where
    _source_file is null
    or _file_last_modified is null
    or _loaded_at is null

union all

select
    'contracts',
    count(*)
from raw.ethereum.contracts
where
    _source_file is null
    or _file_last_modified is null
    or _loaded_at is null

union all

select
    'token_transfers',
    count(*)
from raw.ethereum.token_transfers
where
    _source_file is null
    or _file_last_modified is null
    or _loaded_at is null;


-- -----------------------------------------------------------------------------
-- 05.5 Assess Snowflake table storage
-- -----------------------------------------------------------------------------
-- Review the row counts and reported table sizes after ingestion.
--
-- This provides visibility into the volume of data stored in Snowflake
-- and supports future decisions around compute and storage optimisation.
--
-- Storage is expressed in decimal gigabytes (GB).

select
    table_name,
    row_count,
    round(bytes / 1000000000, 2) as storage_gb
from raw.information_schema.tables
where table_schema = 'ETHEREUM'
    and table_name in (
        'BLOCKS',
        'TRANSACTIONS',
        'CONTRACTS',
        'TOKEN_TRANSFERS'
    )
order by storage_gb desc;


-- =============================================================================
-- 06. VERIFY DBT ACCESS TO RAW DATA
-- =============================================================================
-- Confirm that the TRANSFORMER role can read all four raw source tables
-- using the transformation warehouse.
--
-- Disable secondary roles to ensure that access is provided by the
-- TRANSFORMER role rather than inherited from another active user role.
--
-- Successful execution confirms that dbt has read access to the sources.


use role transformer;
use secondary roles none;
use warehouse transforming_wh;
use database raw;
use schema ethereum;


-- Verify read access to each source table.

select * from raw.ethereum.blocks limit 5;

select * from raw.ethereum.transactions limit 5;

select * from raw.ethereum.contracts limit 5;

select * from raw.ethereum.token_transfers limit 5;


-- =============================================================================
-- SOURCE INGESTION COMPLETE
-- =============================================================================
-- At this stage:
--     1. All four raw source tables should exist.
--     2. Historical Ethereum data should be loaded.
--     3. Ingestion metadata should be populated.
--     4. Historical coverage and storage volumes should be reviewed.
--     5. The TRANSFORMER role should have read access to all source tables.
--
-- Next steps:
--     1. Generate the Ethereum data dictionary.
--     2. Document source table relationships and grain.
--     3. Configure the dbt project and source definitions.
--     4. Begin staging model development.
-- =============================================================================