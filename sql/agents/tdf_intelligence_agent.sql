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
create or replace semantic view TDF_DATA_PLATFORM.ANALYTICS.SV_RESOURCE_CAPACITY
    tables (
        CAPACITY as TDF_DATA_PLATFORM.ANALYTICS.VW_CAPACITY_VS_DEMAND 
            with synonyms=('capacity','demand','workforce','staffing','FTE') 
            comment='Capacity vs demand analysis - 18 month forecasting'
    )
    facts (
        CAPACITY.HEADCOUNT as headcount comment='Number of employees',
        CAPACITY.FTE_AVAILABLE as fte_available comment='Full-time equivalent capacity',
        CAPACITY.DEMAND_FTE as demand_fte comment='FTE required by demand',
        CAPACITY.FTE_GAP as fte_gap comment='Gap between capacity and demand',
        CAPACITY.UTILIZATION_PCT as utilization_pct comment='Utilization percentage'
    )
    dimensions (
        CAPACITY.YEAR_MONTH as year_month with synonyms=('month','date','planning month') comment='Planning month',
        CAPACITY.BU_NAME as bu_name with synonyms=('BU','division','business unit') comment='Business unit',
        CAPACITY.REGION_NAME as region_name with synonyms=('region','territory') comment='French region',
        CAPACITY.SKILL_CATEGORY_NAME as skill_category_name with synonyms=('skill','competency') comment='Skill category',
        CAPACITY.CAPACITY_STATUS as capacity_status with synonyms=('status') comment='Status (SUFFICIENT/TIGHT/SHORTAGE)'
    )
    metrics (
        CAPACITY.TOTAL_HEADCOUNT as SUM(CAPACITY.HEADCOUNT) comment='Total headcount',
        CAPACITY.TOTAL_FTE as SUM(CAPACITY.FTE_AVAILABLE) comment='Total FTE',
        CAPACITY.TOTAL_GAP as SUM(CAPACITY.FTE_GAP) comment='Total FTE gap',
        CAPACITY.AVG_UTILIZATION as AVG(CAPACITY.UTILIZATION_PCT) comment='Average utilization'
    )
    comment='Resource & Capacity Planning - 18 month forecasting';


-- SEMANTIC VIEW 2: ESG Reporting
create or replace semantic view TDF_DATA_PLATFORM.ANALYTICS.SV_ESG_REPORTING
    tables (
        ESG as TDF_DATA_PLATFORM.ANALYTICS.VW_ESG_DASHBOARD 
            with synonyms=('sustainability','carbon','emissions','Index Egalite') 
            comment='ESG dashboard for regulatory reporting'
    )
    facts (
        ESG.CARBON_EMISSIONS_TONNES as carbon_emissions comment='Carbon emissions in tonnes',
        ESG.RENEWABLE_ENERGY_PCT as renewable_pct comment='Renewable energy percentage',
        ESG.EQUALITY_INDEX_SCORE as equality_score comment='Index Egalite score (target >=75)',
        ESG.TOTAL_EMPLOYEES as total_employees comment='Total employees',
        ESG.FEMALE_EMPLOYEES_PCT as female_pct comment='Female employees percentage'
    )
    dimensions (
        ESG.FISCAL_YEAR as fiscal_year with synonyms=('year') comment='Fiscal year',
        ESG.ENVIRONMENTAL_STATUS as environmental_status with synonyms=('env status') comment='Environmental status',
        ESG.SOCIAL_STATUS as social_status comment='Social status',
        ESG.OVERALL_ESG_STATUS as overall_esg_status with synonyms=('status','esg status') comment='Overall ESG status'
    )
    metrics (
        ESG.TOTAL_EMISSIONS as SUM(ESG.CARBON_EMISSIONS_TONNES) comment='Total carbon emissions',
        ESG.AVG_EGALITE as AVG(ESG.EQUALITY_INDEX_SCORE) comment='Average Index Egalite'
    )
    comment='ESG Regulatory Reporting - CSRD, Bilan GES, Index Egalite';


-- SEMANTIC VIEW 3: Digital Twin Infrastructure
create or replace semantic view TDF_DATA_PLATFORM.ANALYTICS.SV_DIGITAL_TWIN
    tables (
        INFRA as TDF_DATA_PLATFORM.ANALYTICS.VW_INFRASTRUCTURE_HEALTH 
            with synonyms=('infrastructure','sites','towers','pylons','digital twin') 
            comment='Infrastructure health - 2,000+ pylons'
    )
    facts (
        INFRA.SITE_COUNT as site_count comment='Number of sites',
        INFRA.AVG_TENANTS as avg_tenants comment='Average tenants per site',
        INFRA.AVG_COLOCATION_RATE as colocation_rate comment='Colocation rate',
        INFRA.DT_SYNCED_COUNT as synced_count comment='Sites synced with Digital Twin',
        INFRA.DT_DISCREPANCY_COUNT as discrepancy_count comment='Sites with discrepancies'
    )
    dimensions (
        INFRA.SITE_TYPE as site_type with synonyms=('type') comment='Site type (TOWER/ROOFTOP/INDOOR)',
        INFRA.STATUS as infra_status with synonyms=('status','site status') comment='Site status',
        INFRA.DEPARTMENT_NAME as department_name with synonyms=('department') comment='Department',
        INFRA.REGION_NAME as region_name with synonyms=('region','territory') comment='French region'
    )
    metrics (
        INFRA.TOTAL_SITES as SUM(INFRA.SITE_COUNT) comment='Total sites',
        INFRA.TOTAL_DISCREPANCIES as SUM(INFRA.DT_DISCREPANCY_COUNT) comment='Total discrepancies'
    )
    comment='Digital Twin & Infrastructure - 2,000+ pylons';


-- SEMANTIC VIEW 4: CAPEX & Lifecycle
create or replace semantic view TDF_DATA_PLATFORM.ANALYTICS.SV_CAPEX_LIFECYCLE
    tables (
        EQUIP as TDF_DATA_PLATFORM.ANALYTICS.VW_EQUIPMENT_LIFECYCLE 
            with synonyms=('equipment','lifecycle','assets','CAPEX') 
            comment='Equipment lifecycle - 7-10 year lifespans'
    )
    facts (
        EQUIP.EQUIPMENT_COUNT as equipment_count comment='Number of equipment items',
        EQUIP.AVG_AGE_YEARS as avg_age comment='Average age in years',
        EQUIP.AVG_CONDITION_SCORE as condition_score comment='Average condition score',
        EQUIP.TOTAL_REPLACEMENT_COST as replacement_cost comment='Replacement cost',
        EQUIP.PAST_END_OF_LIFE_COUNT as past_eol_count comment='Equipment past end of life'
    )
    dimensions (
        EQUIP.LIFECYCLE_STATUS as lifecycle_status with synonyms=('status') comment='Lifecycle status',
        EQUIP.EQUIPMENT_CATEGORY as equipment_category with synonyms=('category') comment='Equipment category',
        EQUIP.EQUIPMENT_TYPE_NAME as equipment_type_name with synonyms=('equipment type') comment='Equipment type'
    )
    metrics (
        EQUIP.TOTAL_EQUIPMENT as SUM(EQUIP.EQUIPMENT_COUNT) comment='Total equipment',
        EQUIP.TOTAL_COST as SUM(EQUIP.TOTAL_REPLACEMENT_COST) comment='Total replacement cost'
    )
    comment='CAPEX & Lifecycle - 7-10 year equipment lifecycles';


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

