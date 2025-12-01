-- ============================================================================
-- TDF DATA PLATFORM - MASTER DEPLOYMENT SCRIPT
-- ============================================================================
-- This script orchestrates the complete deployment of the TDF Data Platform
-- Run via: EXECUTE IMMEDIATE FROM @TDF_REPO/branches/main/sql/00_MASTER_DEPLOY.sql
-- ============================================================================
-- Version: 1.0.0
-- Date: 2025
-- Data Period: June 1, 2025 - December 19, 2025
-- ============================================================================

-- ============================================================================
-- PRE-REQUISITE: CREATE DATABASE (Must exist before Git repo can be accessed)
-- ============================================================================

USE ROLE SYSADMIN;

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS TDF_DATA_PLATFORM
    COMMENT = 'TDF Infrastructure Data Platform - Single Source of Truth';

USE DATABASE TDF_DATA_PLATFORM;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS PUBLIC;
CREATE SCHEMA IF NOT EXISTS CORE COMMENT = 'Master data: regions, departments, business units, operators';
CREATE SCHEMA IF NOT EXISTS HR COMMENT = 'Human Resources: employees, skills, capacity, diversity';
CREATE SCHEMA IF NOT EXISTS COMMERCIAL COMMENT = 'Commercial: demand forecast, projects, investment scenarios';
CREATE SCHEMA IF NOT EXISTS OPERATIONS COMMENT = 'Operations: work orders, maintenance, broadcast coverage';
CREATE SCHEMA IF NOT EXISTS INFRASTRUCTURE COMMENT = 'Infrastructure: 8,785 sites, towers, rooftops, equipment';
CREATE SCHEMA IF NOT EXISTS FINANCE COMMENT = 'Finance: CAPEX, budgets, accounting, revenue (EUR 799.1M)';
CREATE SCHEMA IF NOT EXISTS ENERGY COMMENT = 'Energy: consumption readings, carbon emissions';
CREATE SCHEMA IF NOT EXISTS ESG COMMENT = 'ESG: regulatory reports, audit trails, compliance';
CREATE SCHEMA IF NOT EXISTS DIGITAL_TWIN COMMENT = 'Digital Twin: asset models, discrepancy detection';
CREATE SCHEMA IF NOT EXISTS ANALYTICS COMMENT = 'Analytics: operational and executive dashboard views';

-- Create warehouse if not exists
CREATE WAREHOUSE IF NOT EXISTS TDF_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 300
    AUTO_RESUME = TRUE;

USE WAREHOUSE TDF_WH;

-- ============================================================================
-- SECTION 1: DATABASE, SCHEMAS, ROLES, AND WAREHOUSE (Full setup)
-- ============================================================================
EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/ddl/01_database_setup.sql;

-- ============================================================================
-- SECTION 2: CORE TABLES (Regions, Departments, Business Units, Operators)
-- ============================================================================
EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/ddl/02_core_tables.sql;

-- ============================================================================
-- SECTION 3: INFRASTRUCTURE TABLES (Sites, Towers, Rooftops, Equipment)
-- ============================================================================
EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/ddl/03_infrastructure_tables.sql;

-- ============================================================================
-- SECTION 4: HR TABLES (Employees, Skills, Capacity, Diversity)
-- ============================================================================
EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/ddl/04_hr_tables.sql;

-- ============================================================================
-- SECTION 5: COMMERCIAL TABLES (Demand Forecast, Projects, Scenarios)
-- ============================================================================
EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/ddl/05_commercial_tables.sql;

-- ============================================================================
-- SECTION 6: OPERATIONS TABLES (Work Orders, Maintenance, Coverage)
-- ============================================================================
EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/ddl/06_operations_tables.sql;

-- ============================================================================
-- SECTION 7: FINANCE TABLES (CAPEX, Budgets, Accounting, Revenue)
-- ============================================================================
EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/ddl/07_finance_tables.sql;

-- ============================================================================
-- SECTION 8: ENERGY TABLES (Consumption, Carbon Emissions)
-- ============================================================================
EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/ddl/08_energy_tables.sql;

-- ============================================================================
-- SECTION 9: ESG TABLES (Regulatory Reports, Audit Trail, Board Scorecard)
-- ============================================================================
EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/ddl/09_esg_tables.sql;

-- ============================================================================
-- SECTION 10: DIGITAL TWIN TABLES (Asset Models, Discrepancies)
-- ============================================================================
EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/ddl/10_digital_twin_tables.sql;

-- ============================================================================
-- SECTION 11: ANALYTICS VIEWS (Operational Dashboards)
-- ============================================================================
EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/ddl/11_analytics_views.sql;

-- ============================================================================
-- SECTION 12: EXECUTIVE VIEWS (C-Level Dashboards)
-- ============================================================================
EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/ddl/12_executive_views.sql;

-- ============================================================================
-- SECTION 13: SEED DATA - FRANCE GEOGRAPHY
-- ============================================================================
EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/data/13_seed_france_geo.sql;

-- ============================================================================
-- SECTION 14: SEED DATA - OPERATORS (Orange, SFR, Bouygues, Free)
-- ============================================================================
EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/data/14_seed_operators.sql;

-- ============================================================================
-- SECTION 15: SEED DATA - INFRASTRUCTURE (8,785 Sites, 7,877 Towers)
-- ============================================================================
EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/data/15_seed_infrastructure.sql;

-- ============================================================================
-- SECTION 16: SEED DATA - HR (Employees, Skills)
-- ============================================================================
EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/data/16_seed_hr_data.sql;

-- ============================================================================
-- SECTION 17: SEED DATA - OPERATIONS (Work Orders, Maintenance)
-- ============================================================================
EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/data/17_seed_operations.sql;

-- ============================================================================
-- SECTION 18: SEED DATA - FINANCE (CAPEX, Revenue, Accounting)
-- ============================================================================
EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/data/18_seed_finance.sql;

-- ============================================================================
-- SECTION 19: SEED DATA - ENERGY & ESG
-- ============================================================================
EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/data/19_seed_energy_esg.sql;

-- ============================================================================
-- SECTION 20: SEED DATA - EXECUTIVE KPIs
-- ============================================================================
EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/data/20_seed_executive_kpis.sql;

-- ============================================================================
-- DEPLOYMENT VALIDATION
-- ============================================================================

-- Count records in key tables
SELECT 'DEPLOYMENT VALIDATION' AS STATUS;
SELECT 'CORE.REGIONS' AS TABLE_NAME, COUNT(*) AS RECORD_COUNT FROM TDF_DATA_PLATFORM.CORE.REGIONS
UNION ALL SELECT 'CORE.DEPARTMENTS', COUNT(*) FROM TDF_DATA_PLATFORM.CORE.DEPARTMENTS
UNION ALL SELECT 'INFRASTRUCTURE.SITES', COUNT(*) FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES
UNION ALL SELECT 'INFRASTRUCTURE.TOWERS', COUNT(*) FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.TOWERS
UNION ALL SELECT 'HR.EMPLOYEES', COUNT(*) FROM TDF_DATA_PLATFORM.HR.EMPLOYEES
UNION ALL SELECT 'FINANCE.REVENUE_BY_SEGMENT', COUNT(*) FROM TDF_DATA_PLATFORM.FINANCE.REVENUE_BY_SEGMENT;

SELECT 'TDF DATA PLATFORM DEPLOYMENT COMPLETE' AS STATUS, CURRENT_TIMESTAMP() AS DEPLOYED_AT;

