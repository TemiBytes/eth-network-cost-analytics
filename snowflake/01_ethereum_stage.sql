/*
===============================================================================
File:        01_ethereum_stage.sql
Project:     Ethereum Blockchain Analytics
Layer:       Raw
Purpose:     Configure Snowflake to access Ethereum blockchain data in AWS S3.

Objectives:
    1. Establish the Snowflake execution context.
    2. Create a named Parquet file format.
    3. Create an external stage pointing to the AWS public dataset.
    4. Verify the configuration and access to source files.

Source:      AWS Public Blockchain Dataset (Ethereum)
Location:    s3://aws-public-blockchain/v1.0/eth/
Format:      Parquet

Note:
    This script creates metadata objects in Snowflake.
    It does not create source tables or load any data.
===============================================================================
*/


-- =============================================================================
-- 01. SESSION SETUP
-- =============================================================================
-- Set the role, warehouse, database and schema used for source configuration.

use role loader;
use warehouse loading_wh;
use database raw;
use schema ethereum;


-- =============================================================================
-- 02. CREATE FILE FORMAT
-- =============================================================================
-- Define how Snowflake should interpret the source Parquet files.
-- A named file format allows us to reuse the same configuration across
-- multiple operations, including INFER_SCHEMA and COPY INTO.

create file format if not exists raw.ethereum.parquet_format
    type = parquet;


-- =============================================================================
-- 03. CREATE EXTERNAL STAGE
-- =============================================================================
-- Create a reference to the AWS S3 location containing the Ethereum datasets.
-- The bucket is publicly accessible, so AWS credentials and a storage
-- integration are not required for accessing these public files.
--
-- Associate the named Parquet file format with the stage so Snowflake
-- knows how to interpret the files when querying or loading data.

create stage if not exists raw.ethereum.eth_stage
    url = 's3://aws-public-blockchain/v1.0/eth/'
    file_format = (
        format_name = 'RAW.ETHEREUM.PARQUET_FORMAT'
    );


-- =============================================================================
-- 04. VERIFY OBJECT CONFIGURATION
-- =============================================================================
-- Confirm that the file format and external stage have been created
-- with the expected configuration.

desc file format raw.ethereum.parquet_format;

desc stage raw.ethereum.eth_stage;


-- =============================================================================
-- 05. VERIFY SOURCE ACCESS
-- =============================================================================
-- Confirm that Snowflake can successfully access files in the AWS S3 bucket.
-- Restrict the listing to a single date to avoid unnecessary file enumeration.
--
-- Expected result: One or more Parquet files with their names, sizes,
-- MD5 values and last modified timestamps.

list @raw.ethereum.eth_stage/blocks/date=2026-10-01/;