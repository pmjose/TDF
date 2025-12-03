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
-- 3. SEMANTIC VIEWS FOR CORTEX ANALYST
-- ============================================================================
-- Following exact syntax from example.sql

-- SEMANTIC VIEW 1: Resource & Capacity Planning
CREATE OR REPLACE SEMANTIC VIEW TDF_DATA_PLATFORM.ANALYTICS.SV_RESOURCE_CAPACITY
    TABLES (
        CAPACITY AS TDF_DATA_PLATFORM.ANALYTICS.VW_CAPACITY_VS_DEMAND 
            WITH SYNONYMS=('capacity','demand','workforce','staffing','FTE') 
            COMMENT='Capacity vs demand analysis - 18 month forecasting'
    )
    FACTS (
        CAPACITY.HEADCOUNT AS headcount COMMENT='Number of employees',
        CAPACITY.FTE_AVAILABLE AS fte_available COMMENT='Full-time equivalent capacity',
        CAPACITY.DEMAND_FTE AS demand_fte COMMENT='FTE required by demand',
        CAPACITY.FTE_GAP AS fte_gap COMMENT='Gap between capacity and demand',
        CAPACITY.UTILIZATION_PCT AS utilization_pct COMMENT='Utilization percentage',
        CAPACITY.RECORD_COUNT AS 1 COMMENT='Count of records'
    )
    DIMENSIONS (
        CAPACITY.YEAR_MONTH AS year_month WITH SYNONYMS=('month','date','planning month') COMMENT='Planning month',
        CAPACITY.BU_NAME AS bu_name WITH SYNONYMS=('BU','division','business unit') COMMENT='Business unit',
        CAPACITY.REGION_NAME AS region_name WITH SYNONYMS=('region','territory') COMMENT='French region',
        CAPACITY.SKILL_CATEGORY_NAME AS skill_category_name WITH SYNONYMS=('skill','competency','skill category') COMMENT='Skill category',
        CAPACITY.CAPACITY_STATUS AS capacity_status WITH SYNONYMS=('status') COMMENT='Status (SUFFICIENT/TIGHT/SHORTAGE)'
    )
    METRICS (
        CAPACITY.TOTAL_HEADCOUNT AS SUM(capacity.headcount) COMMENT='Total headcount',
        CAPACITY.TOTAL_FTE AS SUM(capacity.fte_available) COMMENT='Total FTE',
        CAPACITY.TOTAL_GAP AS SUM(capacity.fte_gap) COMMENT='Total FTE gap',
        CAPACITY.AVG_UTILIZATION AS AVG(capacity.utilization_pct) COMMENT='Average utilization'
    )
    COMMENT='Resource & Capacity Planning - 18 month forecasting';


-- SEMANTIC VIEW 2: ESG Reporting
CREATE OR REPLACE SEMANTIC VIEW TDF_DATA_PLATFORM.ANALYTICS.SV_ESG_REPORTING
    TABLES (
        ESG AS TDF_DATA_PLATFORM.ANALYTICS.VW_ESG_DASHBOARD 
            WITH SYNONYMS=('sustainability','carbon','emissions','Index Egalite') 
            COMMENT='ESG dashboard for regulatory reporting'
    )
    FACTS (
        ESG.CARBON_EMISSIONS_TONNES AS carbon_tonnes COMMENT='Carbon emissions in tonnes',
        ESG.RENEWABLE_ENERGY_PCT AS renewable_pct COMMENT='Renewable energy percentage',
        ESG.EQUALITY_INDEX_SCORE AS egalite_score COMMENT='Index Egalite score (target >=75)',
        ESG.TOTAL_EMPLOYEES AS total_employees COMMENT='Total employees',
        ESG.FEMALE_EMPLOYEES_PCT AS female_pct COMMENT='Female employees percentage',
        ESG.RECORD_COUNT AS 1 COMMENT='Count of records'
    )
    DIMENSIONS (
        ESG.FISCAL_YEAR AS fiscal_year WITH SYNONYMS=('year') COMMENT='Fiscal year',
        ESG.ENVIRONMENTAL_STATUS AS environmental_status WITH SYNONYMS=('env status') COMMENT='Environmental status',
        ESG.SOCIAL_STATUS AS social_status COMMENT='Social status',
        ESG.OVERALL_ESG_STATUS AS overall_esg_status WITH SYNONYMS=('status','esg status') COMMENT='Overall ESG status'
    )
    METRICS (
        ESG.TOTAL_EMISSIONS AS SUM(esg.carbon_tonnes) COMMENT='Total carbon emissions',
        ESG.AVG_EGALITE AS AVG(esg.egalite_score) COMMENT='Average Index Egalite'
    )
    COMMENT='ESG Regulatory Reporting - CSRD, Bilan GES, Index Egalite';


-- SEMANTIC VIEW 3: Digital Twin Infrastructure
CREATE OR REPLACE SEMANTIC VIEW TDF_DATA_PLATFORM.ANALYTICS.SV_DIGITAL_TWIN
    TABLES (
        INFRA AS TDF_DATA_PLATFORM.ANALYTICS.VW_INFRASTRUCTURE_HEALTH 
            WITH SYNONYMS=('infrastructure','sites','towers','pylons','digital twin') 
            COMMENT='Infrastructure health - 2,000+ pylons'
    )
    FACTS (
        INFRA.SITE_COUNT AS site_count COMMENT='Number of sites',
        INFRA.AVG_TENANTS AS avg_tenants COMMENT='Average tenants per site',
        INFRA.AVG_COLOCATION_RATE AS colocation_rate COMMENT='Colocation rate',
        INFRA.DT_SYNCED_COUNT AS synced_count COMMENT='Sites synced with Digital Twin',
        INFRA.DT_DISCREPANCY_COUNT AS discrepancy_count COMMENT='Sites with discrepancies',
        INFRA.RECORD_COUNT AS 1 COMMENT='Count of records'
    )
    DIMENSIONS (
        INFRA.SITE_TYPE AS site_type WITH SYNONYMS=('type') COMMENT='Site type (TOWER/ROOFTOP/INDOOR)',
        INFRA.STATUS AS infra_status WITH SYNONYMS=('status','site status') COMMENT='Site status',
        INFRA.DEPARTMENT_NAME AS department_name WITH SYNONYMS=('department') COMMENT='Department',
        INFRA.REGION_NAME AS region_name WITH SYNONYMS=('region','territory') COMMENT='French region'
    )
    METRICS (
        INFRA.TOTAL_SITES AS SUM(infra.site_count) COMMENT='Total sites',
        INFRA.TOTAL_DISCREPANCIES AS SUM(infra.discrepancy_count) COMMENT='Total discrepancies'
    )
    COMMENT='Digital Twin & Infrastructure - 2,000+ pylons';


-- SEMANTIC VIEW 4: CAPEX & Lifecycle
CREATE OR REPLACE SEMANTIC VIEW TDF_DATA_PLATFORM.ANALYTICS.SV_CAPEX_LIFECYCLE
    TABLES (
        EQUIP AS TDF_DATA_PLATFORM.ANALYTICS.VW_EQUIPMENT_LIFECYCLE 
            WITH SYNONYMS=('equipment','lifecycle','assets','CAPEX') 
            COMMENT='Equipment lifecycle - 7-10 year lifespans'
    )
    FACTS (
        EQUIP.EQUIPMENT_COUNT AS equipment_count COMMENT='Number of equipment items',
        EQUIP.AVG_AGE_YEARS AS avg_age COMMENT='Average age in years',
        EQUIP.AVG_CONDITION_SCORE AS condition_score COMMENT='Average condition score',
        EQUIP.TOTAL_REPLACEMENT_COST AS replacement_cost COMMENT='Replacement cost',
        EQUIP.PAST_END_OF_LIFE_COUNT AS past_eol_count COMMENT='Equipment past end of life',
        EQUIP.RECORD_COUNT AS 1 COMMENT='Count of records'
    )
    DIMENSIONS (
        EQUIP.LIFECYCLE_STATUS AS lifecycle_status WITH SYNONYMS=('status') COMMENT='Lifecycle status',
        EQUIP.EQUIPMENT_CATEGORY AS equipment_category WITH SYNONYMS=('category') COMMENT='Equipment category',
        EQUIP.EQUIPMENT_TYPE_NAME AS equipment_type_name WITH SYNONYMS=('equipment type') COMMENT='Equipment type'
    )
    METRICS (
        EQUIP.TOTAL_EQUIPMENT AS SUM(equip.equipment_count) COMMENT='Total equipment',
        EQUIP.TOTAL_COST AS SUM(equip.replacement_cost) COMMENT='Total replacement cost'
    )
    COMMENT='CAPEX & Lifecycle - 7-10 year equipment lifecycles';


-- Show created semantic views
SHOW SEMANTIC VIEWS IN SCHEMA TDF_DATA_PLATFORM.ANALYTICS;


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

-- ============================================================================
-- 6. CREATE THE TDF DATA PLATFORM AGENT
-- ============================================================================
-- Following the exact syntax from example.sql

USE ROLE TDF_INTELLIGENCE_ROLE;

CREATE OR REPLACE AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.TDF_DATA_PLATFORM_AGENT
WITH PROFILE='{ "display_name": "TDF Data Platform Agent" }'
    COMMENT=$$ TDF Infrastructure Data Platform AI Assistant. Covers Resource Planning, ESG Reporting, Digital Twin, and CAPEX Lifecycle for French telecom infrastructure. $$
FROM SPECIFICATION $$
{
  "models": {
    "orchestration": ""
  },
  "instructions": {
    "response": "You are the TDF Data Platform Assistant. TDF is the leading French telecom infrastructure company with EUR 799M revenue, 2,000+ towers across France, and BBB- credit rating. Help users analyze 4 priority use cases: (1) Resource & Capacity Planning - 18-month workforce forecasts, (2) ESG Reporting - CSRD compliance and Index Egalite scores (target >=75), (3) Digital Twin - 2,000+ pylons infrastructure data, (4) CAPEX & Lifecycle - 7-10 year equipment lifecycles. Always provide clear business context and actionable insights.",
    "orchestration": "Select the appropriate datamart based on the question:\n- Resource & Capacity: workforce, hiring, demand forecasting, utilization (VW_CAPACITY_VS_DEMAND)\n- ESG Reporting: carbon emissions, energy, diversity, Index Egalite, compliance (VW_ESG_DASHBOARD)\n- Digital Twin: sites, towers, equipment inventory, data quality, discrepancies (VW_INFRASTRUCTURE_HEALTH)\n- CAPEX & Lifecycle: equipment age, renewal forecast, budget vs actuals (VW_EQUIPMENT_LIFECYCLE)\n\nFor capacity planning questions, the forecast horizon is 18 months.\nFor ESG questions, emphasize audit trail and data lineage.\nFor Digital Twin questions, note we have 2,000+ pylons.\nFor CAPEX questions, equipment lifecycles are typically 7-10 years.",
    "sample_questions": [
      {
        "question": "What is our workforce capacity vs demand for the next 18 months?"
      },
      {
        "question": "What is our current Index Egalite score and are we compliant?"
      },
      {
        "question": "How many sites have Digital Twin discrepancies?"
      },
      {
        "question": "What equipment is due for renewal in the next 3 years?"
      }
    ]
  },
  "tools": [
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "Query Resource & Capacity",
        "description": "Query workforce capacity, demand forecasting, utilization. Use for questions about hiring needs, capacity gaps, 18-month forecasts."
      }
    },
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "Query ESG Reporting",
        "description": "Query ESG data including carbon emissions, energy, diversity metrics, Index Egalite. Use for CSRD compliance and regulatory reporting."
      }
    },
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "Query Digital Twin",
        "description": "Query infrastructure data including 2,000+ pylons/towers, sites, equipment inventory, data quality scores."
      }
    },
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "Query CAPEX Lifecycle",
        "description": "Query equipment lifecycle data, renewal forecasts, CAPEX budgets. Equipment typically has 7-10 year lifecycles."
      }
    }
  ],
  "tool_resources": {
    "Query Resource & Capacity": {
      "semantic_view": "TDF_DATA_PLATFORM.ANALYTICS.SV_RESOURCE_CAPACITY"
    },
    "Query ESG Reporting": {
      "semantic_view": "TDF_DATA_PLATFORM.ANALYTICS.SV_ESG_REPORTING"
    },
    "Query Digital Twin": {
      "semantic_view": "TDF_DATA_PLATFORM.ANALYTICS.SV_DIGITAL_TWIN"
    },
    "Query CAPEX Lifecycle": {
      "semantic_view": "TDF_DATA_PLATFORM.ANALYTICS.SV_CAPEX_LIFECYCLE"
    }
  }
}
$$;

-- Grant access to the agent
GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.TDF_DATA_PLATFORM_AGENT TO ROLE PUBLIC;

SELECT 'TDF DATA PLATFORM AGENT CREATED SUCCESSFULLY' AS STATUS;


-- ============================================================================
-- 7. SAMPLE QUESTIONS FOR THE AGENT
-- ============================================================================
-- These questions can be asked to the TDF Data Platform Agent

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

