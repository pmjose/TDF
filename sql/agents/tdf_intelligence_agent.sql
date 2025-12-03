-- ============================================================================
-- TDF DATA PLATFORM - SNOWFLAKE INTELLIGENCE AGENT
-- ============================================================================
-- AI-powered conversational agent for TDF's 4 priority use cases:
-- P1: Resource & Capacity Planning (ACM)
-- P1: ESG Regulatory Reporting
-- P2: Infrastructure Data Mastery & Digital Twin
-- P2: CAPEX & Equipment Lifecycle Management
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- ============================================================================
-- 1. ENABLE SNOWFLAKE INTELLIGENCE
-- ============================================================================

CREATE DATABASE IF NOT EXISTS SNOWFLAKE_INTELLIGENCE;
CREATE SCHEMA IF NOT EXISTS SNOWFLAKE_INTELLIGENCE.AGENTS;

GRANT USAGE ON DATABASE SNOWFLAKE_INTELLIGENCE TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE PUBLIC;

-- ============================================================================
-- 2. CREATE TDF INTELLIGENCE ROLE
-- ============================================================================

CREATE OR REPLACE ROLE TDF_INTELLIGENCE_ROLE;

SET current_user_name = CURRENT_USER();
GRANT ROLE TDF_INTELLIGENCE_ROLE TO USER IDENTIFIER($current_user_name);

-- Warehouse access
GRANT USAGE ON WAREHOUSE TDF_WH TO ROLE TDF_INTELLIGENCE_ROLE;

-- Database and schema access
GRANT USAGE ON DATABASE TDF_DATA_PLATFORM TO ROLE TDF_INTELLIGENCE_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE TDF_DATA_PLATFORM TO ROLE TDF_INTELLIGENCE_ROLE;
GRANT SELECT ON ALL TABLES IN DATABASE TDF_DATA_PLATFORM TO ROLE TDF_INTELLIGENCE_ROLE;
GRANT SELECT ON ALL VIEWS IN DATABASE TDF_DATA_PLATFORM TO ROLE TDF_INTELLIGENCE_ROLE;

-- Critical: Grant CREATE SEMANTIC VIEW on the ANALYTICS schema
GRANT CREATE SEMANTIC VIEW ON SCHEMA TDF_DATA_PLATFORM.ANALYTICS TO ROLE TDF_INTELLIGENCE_ROLE;

-- Future grants to ensure new objects are accessible
GRANT SELECT ON FUTURE TABLES IN DATABASE TDF_DATA_PLATFORM TO ROLE TDF_INTELLIGENCE_ROLE;
GRANT SELECT ON FUTURE VIEWS IN DATABASE TDF_DATA_PLATFORM TO ROLE TDF_INTELLIGENCE_ROLE;

-- Snowflake Intelligence access
GRANT USAGE ON DATABASE SNOWFLAKE_INTELLIGENCE TO ROLE TDF_INTELLIGENCE_ROLE;
GRANT USAGE ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE TDF_INTELLIGENCE_ROLE;
GRANT CREATE AGENT ON SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS TO ROLE TDF_INTELLIGENCE_ROLE;

-- Switch to the role
USE ROLE TDF_INTELLIGENCE_ROLE;
USE DATABASE TDF_DATA_PLATFORM;
USE WAREHOUSE TDF_WH;
USE SCHEMA ANALYTICS;

-- ============================================================================
-- PREREQUISITE CHECK
-- ============================================================================
-- The following analytics views MUST exist before running this script:
-- Run these DDL scripts first if you haven't:
--   - sql/ddl/11_analytics_views.sql
--   - sql/ddl/12_executive_views.sql
--
-- Quick check: SELECT * FROM TDF_DATA_PLATFORM.ANALYTICS.VW_CAPACITY_VS_DEMAND LIMIT 1;
-- ============================================================================

-- ============================================================================
-- 3. VIEWS FOR CORTEX ANALYST (Simple Views with Comments)
-- ============================================================================
-- Note: Instead of semantic views (which have complex syntax requirements),
-- we'll use well-documented regular views that the Agent can query directly.
-- The Agent's semantic understanding comes from the YAML semantic model.

-- Resource & Capacity Planning View (already exists as VW_CAPACITY_VS_DEMAND)
COMMENT ON VIEW TDF_DATA_PLATFORM.ANALYTICS.VW_CAPACITY_VS_DEMAND IS 
    'Resource & Capacity Planning - 18 month workforce forecasting. 
    Columns: YEAR_MONTH (planning month), BU_NAME (business unit), REGION_NAME (French region), 
    SKILL_CATEGORY_NAME (skill type), HEADCOUNT (employees), FTE_AVAILABLE (capacity), 
    DEMAND_FTE (required FTE), FTE_GAP (capacity minus demand), CAPACITY_STATUS (SUFFICIENT/TIGHT/SHORTAGE), 
    UTILIZATION_PCT (utilization percentage)';

-- ESG Reporting View (already exists as VW_ESG_DASHBOARD)
COMMENT ON VIEW TDF_DATA_PLATFORM.ANALYTICS.VW_ESG_DASHBOARD IS 
    'ESG Regulatory Reporting - CSRD, Bilan GES, Index Égalité compliance.
    Columns: FISCAL_YEAR, CARBON_EMISSIONS_TONNES, CARBON_INTENSITY_KG_EUR, RENEWABLE_ENERGY_PCT,
    TOTAL_EMPLOYEES, FEMALE_EMPLOYEES_PCT, EQUALITY_INDEX_SCORE (target ≥75), TRAINING_HOURS_PER_EMPLOYEE,
    ENVIRONMENTAL_STATUS, SOCIAL_STATUS, OVERALL_ESG_STATUS';

-- Infrastructure Health View (already exists as VW_INFRASTRUCTURE_HEALTH)
COMMENT ON VIEW TDF_DATA_PLATFORM.ANALYTICS.VW_INFRASTRUCTURE_HEALTH IS 
    'Digital Twin & Infrastructure - 2,000+ pylons across France.
    Columns: SITE_TYPE (TOWER/ROOFTOP/INDOOR), STATUS, DEPARTMENT_NAME, REGION_NAME,
    SITE_COUNT, AVG_TENANTS, AVG_COLOCATION_RATE, AVG_RISK_SCORE, DT_SYNCED_COUNT, DT_DISCREPANCY_COUNT';

-- Equipment Lifecycle View (already exists as VW_EQUIPMENT_LIFECYCLE)
COMMENT ON VIEW TDF_DATA_PLATFORM.ANALYTICS.VW_EQUIPMENT_LIFECYCLE IS 
    'CAPEX & Lifecycle Management - 7-10 year equipment lifecycles.
    Columns: LIFECYCLE_STATUS (ACTIVE/AGING/END_OF_LIFE), EQUIPMENT_CATEGORY, EQUIPMENT_TYPE_NAME,
    EQUIPMENT_COUNT, AVG_AGE_YEARS, AVG_CONDITION_SCORE, AVG_RISK_SCORE, TOTAL_REPLACEMENT_COST, PAST_END_OF_LIFE_COUNT';


-- ============================================================================
-- 4. VERIFY VIEWS ARE AVAILABLE
-- ============================================================================

-- These views should already exist from the DDL scripts
SELECT 'VW_CAPACITY_VS_DEMAND' AS VIEW_NAME, COUNT(*) AS ROW_COUNT FROM TDF_DATA_PLATFORM.ANALYTICS.VW_CAPACITY_VS_DEMAND
UNION ALL
SELECT 'VW_ESG_DASHBOARD', COUNT(*) FROM TDF_DATA_PLATFORM.ANALYTICS.VW_ESG_DASHBOARD
UNION ALL
SELECT 'VW_INFRASTRUCTURE_HEALTH', COUNT(*) FROM TDF_DATA_PLATFORM.ANALYTICS.VW_INFRASTRUCTURE_HEALTH
UNION ALL  
SELECT 'VW_EQUIPMENT_LIFECYCLE', COUNT(*) FROM TDF_DATA_PLATFORM.ANALYTICS.VW_EQUIPMENT_LIFECYCLE;


-- ============================================================================
-- 5. CREATE THE TDF INTELLIGENCE AGENT
-- ============================================================================
-- Note: Snowflake Intelligence uses Cortex Analyst which requires a semantic model
-- defined as a YAML file stored in a stage. This creates a simpler agent setup
-- that uses SQL_EXEC for direct view access.

USE ROLE ACCOUNTADMIN;

-- Grant necessary permissions for the agent
GRANT USAGE ON DATABASE TDF_DATA_PLATFORM TO ROLE TDF_INTELLIGENCE_ROLE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE TDF_DATA_PLATFORM TO ROLE TDF_INTELLIGENCE_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA TDF_DATA_PLATFORM.ANALYTICS TO ROLE TDF_INTELLIGENCE_ROLE;

CREATE OR REPLACE CORTEX SEARCH SERVICE TDF_DATA_PLATFORM.ANALYTICS.TDF_SEARCH_SERVICE
  ON SEARCH_COLUMN
  WAREHOUSE = TDF_WH
  TARGET_LAG = '1 hour'
AS (
  SELECT 
    'Capacity Planning' AS CATEGORY,
    YEAR_MONTH || ' - ' || BU_NAME || ' - ' || REGION_NAME AS SEARCH_COLUMN,
    YEAR_MONTH, BU_NAME, REGION_NAME, HEADCOUNT, FTE_AVAILABLE, DEMAND_FTE, FTE_GAP, CAPACITY_STATUS
  FROM TDF_DATA_PLATFORM.ANALYTICS.VW_CAPACITY_VS_DEMAND
);

SELECT 'SETUP COMPLETE - Views documented and ready for Cortex Analyst' AS STATUS;


-- ============================================================================
-- 6. SAMPLE QUESTIONS FOR CORTEX ANALYST / SNOWFLAKE COPILOT
-- ============================================================================
-- These questions can be asked in Snowflake Copilot or Cortex Analyst
-- They work against the documented analytics views

/*
=== P1: Resource & Capacity Planning ===
These questions address: 18-month forecasting, dynamic scenario modeling, staffing based on commercial contribution

1. "What is our total workforce capacity vs demand forecast for the next 18 months?"
2. "Show me capacity utilization by region for Q1 2026"
3. "Which skill categories have the biggest capacity gaps?"
4. "What is the revenue forecast based on commercial contribution for Île-de-France?"
5. "How many work orders are overdue by priority level?"
6. "What are the hiring needs by business unit for the next 6 months?"


=== P1: ESG Regulatory Reporting ===
These questions address: CSRD compliance, audit trail, data lineage, Index Égalité

1. "What are our total carbon emissions by scope (Scope 1, 2, 3) for fiscal year 2025?"
2. "What is our current Index Égalité score and are we above the 75 threshold?"
3. "Show me the renewable energy percentage trend over the last 12 months"
4. "Which ESG compliance requirements are overdue or at risk?"
5. "What is the data lineage for our CSRD carbon emissions reporting?"
6. "Compare our female representation in management vs company-wide"
7. "What is our carbon intensity (kg CO2 per EUR revenue)?"


=== P2: Digital Twin & Infrastructure ===
These questions address: 2,000+ pylons, data harmonization, discrepancy detection

1. "How many active towers do we have and what is the average colocation rate?"
2. "Show me Digital Twin discrepancies by severity and status"
3. "What is our data quality score for infrastructure assets?"
4. "Which sites have the highest number of tenants?"
5. "List the top 10 towers by load utilization percentage"
6. "What is the total revenue by operator across all sites?"
7. "How many discrepancies are affecting billing?"


=== P2: CAPEX & Lifecycle Management ===
These questions address: 7-10 year lifecycles, predictive renewal, installation dates

1. "What equipment is due for renewal in the next 3 years?"
2. "Show me equipment by lifecycle status and average age"
3. "What is our CAPEX budget vs actual spending by category?"
4. "Which equipment has the highest failure risk score?"
5. "What is the total replacement cost of end-of-life equipment?"
6. "List equipment past its expected end of life date"
7. "What is the 7-year renewal forecast by equipment category?"
*/

