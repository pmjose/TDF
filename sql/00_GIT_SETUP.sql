-- ============================================================================
-- TDF DATA PLATFORM - GIT INTEGRATION SETUP
-- ============================================================================
-- This script sets up Snowflake Git integration for automated deployments
-- Run this ONCE with ACCOUNTADMIN role before using 00_MASTER_DEPLOY.sql
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- STEP 1: Create API Integration for GitHub
-- ============================================================================
-- Repository: https://github.com/pmjose/TDF

CREATE OR REPLACE API INTEGRATION tdf_git_integration
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/pmjose/TDF')
    ENABLED = TRUE
    COMMENT = 'Git integration for TDF Data Platform deployment';

-- Grant usage to SYSADMIN for ongoing management
GRANT USAGE ON INTEGRATION tdf_git_integration TO ROLE SYSADMIN;

-- ============================================================================
-- STEP 2: Create Database (if not exists) for Git Repository object
-- ============================================================================

CREATE DATABASE IF NOT EXISTS TDF_DATA_PLATFORM
    COMMENT = 'TDF Infrastructure Data Platform - Single Source of Truth';

USE DATABASE TDF_DATA_PLATFORM;

CREATE SCHEMA IF NOT EXISTS PUBLIC;

-- ============================================================================
-- STEP 3: Create Git Repository Object
-- ============================================================================
-- Repository: https://github.com/pmjose/TDF

CREATE OR REPLACE GIT REPOSITORY TDF_DATA_PLATFORM.PUBLIC.TDF_REPO
    API_INTEGRATION = tdf_git_integration
    ORIGIN = 'https://github.com/pmjose/TDF.git'
    COMMENT = 'TDF Data Platform SQL scripts repository';

-- ============================================================================
-- STEP 4: Fetch latest from Git
-- ============================================================================

ALTER GIT REPOSITORY TDF_DATA_PLATFORM.PUBLIC.TDF_REPO FETCH;

-- ============================================================================
-- STEP 5: Verify setup
-- ============================================================================

-- List available branches
SHOW GIT BRANCHES IN TDF_DATA_PLATFORM.PUBLIC.TDF_REPO;

-- List files in the repository
SELECT * FROM TABLE(TDF_DATA_PLATFORM.PUBLIC.TDF_REPO!LIST_FILES('main', '/sql/'));

-- ============================================================================
-- DEPLOYMENT INSTRUCTIONS
-- ============================================================================
-- After this setup is complete, run the master deployment:
--
--   ALTER GIT REPOSITORY TDF_DATA_PLATFORM.PUBLIC.TDF_REPO FETCH;
--   EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/00_MASTER_DEPLOY.sql;
--
-- ============================================================================

