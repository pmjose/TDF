-- ============================================================================
-- TDF DATA PLATFORM - DATABASE SETUP
-- ============================================================================
-- Creates the database, schemas, roles, and warehouse
-- Run this script first with ACCOUNTADMIN privileges
-- ============================================================================

-- ============================================================================
-- STEP 0: USE ACCOUNTADMIN AND CREATE DATABASE FIRST
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Drop and recreate database for clean setup (comment out if you want to preserve data)
-- DROP DATABASE IF EXISTS TDF_DATA_PLATFORM;

-- Create database as ACCOUNTADMIN
CREATE DATABASE IF NOT EXISTS TDF_DATA_PLATFORM
    COMMENT = 'TDF Infrastructure Data Platform - Single Source of Truth for Telecom Operations';

-- Grant full privileges to SYSADMIN and SECURITYADMIN
GRANT ALL PRIVILEGES ON DATABASE TDF_DATA_PLATFORM TO ROLE SYSADMIN;
GRANT CREATE SCHEMA ON DATABASE TDF_DATA_PLATFORM TO ROLE SYSADMIN;
GRANT USAGE ON DATABASE TDF_DATA_PLATFORM TO ROLE SECURITYADMIN;

-- ============================================================================
-- ROLES
-- ============================================================================

USE ROLE SECURITYADMIN;

-- Create functional roles
CREATE ROLE IF NOT EXISTS TDF_ADMIN COMMENT = 'TDF Platform Administrator';
CREATE ROLE IF NOT EXISTS TDF_ANALYST COMMENT = 'TDF Data Analyst - Read access';
CREATE ROLE IF NOT EXISTS TDF_ENGINEER COMMENT = 'TDF Data Engineer - Read/Write access';
CREATE ROLE IF NOT EXISTS TDF_EXECUTIVE COMMENT = 'TDF Executive - Dashboard access';

-- Grant roles to SYSADMIN for management
GRANT ROLE TDF_ADMIN TO ROLE SYSADMIN;
GRANT ROLE TDF_ANALYST TO ROLE TDF_ADMIN;
GRANT ROLE TDF_ENGINEER TO ROLE TDF_ADMIN;
GRANT ROLE TDF_EXECUTIVE TO ROLE TDF_ADMIN;

-- ============================================================================
-- WAREHOUSE
-- ============================================================================

USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS TDF_WH
    WAREHOUSE_SIZE = 'SMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 2
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'TDF Data Platform Warehouse';

-- Grant warehouse usage
GRANT USAGE ON WAREHOUSE TDF_WH TO ROLE TDF_ADMIN;
GRANT USAGE ON WAREHOUSE TDF_WH TO ROLE TDF_ANALYST;
GRANT USAGE ON WAREHOUSE TDF_WH TO ROLE TDF_ENGINEER;
GRANT USAGE ON WAREHOUSE TDF_WH TO ROLE TDF_EXECUTIVE;

-- ============================================================================
-- DATABASE (Already created above with ACCOUNTADMIN)
-- ============================================================================

USE DATABASE TDF_DATA_PLATFORM;

-- Grant database access
GRANT USAGE ON DATABASE TDF_DATA_PLATFORM TO ROLE TDF_ADMIN;
GRANT USAGE ON DATABASE TDF_DATA_PLATFORM TO ROLE TDF_ANALYST;
GRANT USAGE ON DATABASE TDF_DATA_PLATFORM TO ROLE TDF_ENGINEER;
GRANT USAGE ON DATABASE TDF_DATA_PLATFORM TO ROLE TDF_EXECUTIVE;

-- ============================================================================
-- SCHEMAS
-- ============================================================================

-- CORE: Master data (regions, departments, business units, operators)
CREATE SCHEMA IF NOT EXISTS CORE
    COMMENT = 'Master data: regions, departments, business units, operators';

-- HR: Workforce, skills, capacity planning, diversity metrics
CREATE SCHEMA IF NOT EXISTS HR
    COMMENT = 'Human Resources: employees, skills, capacity, diversity';

-- COMMERCIAL: Demand forecasting, projects, contracts, scenarios
CREATE SCHEMA IF NOT EXISTS COMMERCIAL
    COMMENT = 'Commercial: demand forecast, projects, investment scenarios';

-- OPERATIONS: Work orders, maintenance, resource allocation, broadcast coverage
CREATE SCHEMA IF NOT EXISTS OPERATIONS
    COMMENT = 'Operations: work orders, maintenance, broadcast coverage';

-- INFRASTRUCTURE: Sites, towers, rooftops, antennas, equipment
CREATE SCHEMA IF NOT EXISTS INFRASTRUCTURE
    COMMENT = 'Infrastructure: 8,785 sites, towers, rooftops, equipment';

-- FINANCE: CAPEX, budgets, accounting entries, revenue by segment
CREATE SCHEMA IF NOT EXISTS FINANCE
    COMMENT = 'Finance: CAPEX, budgets, accounting, revenue (EUR 799.1M)';

-- ENERGY: Consumption readings, carbon emissions
CREATE SCHEMA IF NOT EXISTS ENERGY
    COMMENT = 'Energy: consumption readings, carbon emissions';

-- ESG: Regulatory reports, audit trails, board scorecard
CREATE SCHEMA IF NOT EXISTS ESG
    COMMENT = 'ESG: regulatory reports, audit trails, compliance';

-- DIGITAL_TWIN: 3D models, discrepancies, validation rules
CREATE SCHEMA IF NOT EXISTS DIGITAL_TWIN
    COMMENT = 'Digital Twin: asset models, discrepancy detection';

-- ANALYTICS: Pre-built views for dashboards
CREATE SCHEMA IF NOT EXISTS ANALYTICS
    COMMENT = 'Analytics: operational and executive dashboard views';

-- ============================================================================
-- SCHEMA PERMISSIONS
-- ============================================================================

-- Grant schema access to roles
GRANT USAGE ON ALL SCHEMAS IN DATABASE TDF_DATA_PLATFORM TO ROLE TDF_ADMIN;
GRANT USAGE ON ALL SCHEMAS IN DATABASE TDF_DATA_PLATFORM TO ROLE TDF_ANALYST;
GRANT USAGE ON ALL SCHEMAS IN DATABASE TDF_DATA_PLATFORM TO ROLE TDF_ENGINEER;
GRANT USAGE ON SCHEMA TDF_DATA_PLATFORM.ANALYTICS TO ROLE TDF_EXECUTIVE;

-- Future grants for new objects
GRANT SELECT ON FUTURE TABLES IN DATABASE TDF_DATA_PLATFORM TO ROLE TDF_ANALYST;
GRANT SELECT ON FUTURE VIEWS IN DATABASE TDF_DATA_PLATFORM TO ROLE TDF_ANALYST;
GRANT SELECT ON FUTURE TABLES IN DATABASE TDF_DATA_PLATFORM TO ROLE TDF_EXECUTIVE;
GRANT SELECT ON FUTURE VIEWS IN DATABASE TDF_DATA_PLATFORM TO ROLE TDF_EXECUTIVE;

GRANT ALL ON FUTURE TABLES IN DATABASE TDF_DATA_PLATFORM TO ROLE TDF_ENGINEER;
GRANT ALL ON FUTURE VIEWS IN DATABASE TDF_DATA_PLATFORM TO ROLE TDF_ENGINEER;

-- ============================================================================
-- SET DEFAULT CONTEXT
-- ============================================================================

USE WAREHOUSE TDF_WH;
USE DATABASE TDF_DATA_PLATFORM;
USE SCHEMA CORE;

SELECT 'DATABASE SETUP COMPLETE' AS STATUS, CURRENT_TIMESTAMP() AS CREATED_AT;

