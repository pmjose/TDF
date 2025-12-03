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
-- Following exact syntax from example.sql with primary keys

-- SEMANTIC VIEW 1: Resource & Capacity Planning
create or replace semantic view TDF_DATA_PLATFORM.ANALYTICS.SV_RESOURCE_CAPACITY
  tables (
    WORKFORCE as TDF_DATA_PLATFORM.HR.WORKFORCE_CAPACITY primary key (CAPACITY_ID) 
      with synonyms=('capacity','workforce','staffing','FTE') 
      comment='Workforce capacity data - 18 month forecasting',
    DEMAND as TDF_DATA_PLATFORM.COMMERCIAL.DEMAND_FORECAST primary key (FORECAST_ID) 
      with synonyms=('demand','forecast') 
      comment='Commercial demand forecast',
    BU as TDF_DATA_PLATFORM.CORE.BUSINESS_UNITS primary key (BU_ID) 
      with synonyms=('business unit','division') 
      comment='Business units',
    REGIONS as TDF_DATA_PLATFORM.CORE.REGIONS primary key (REGION_ID) 
      with synonyms=('region','territory') 
      comment='French regions',
    SKILLS as TDF_DATA_PLATFORM.CORE.SKILL_CATEGORIES primary key (SKILL_CATEGORY_ID) 
      with synonyms=('skill','competency') 
      comment='Skill categories'
  )
  relationships (
    WORKFORCE_TO_BU as WORKFORCE(BU_ID) references BU(BU_ID),
    WORKFORCE_TO_REGION as WORKFORCE(REGION_ID) references REGIONS(REGION_ID),
    WORKFORCE_TO_SKILL as WORKFORCE(SKILL_CATEGORY_ID) references SKILLS(SKILL_CATEGORY_ID),
    DEMAND_TO_BU as DEMAND(BU_ID) references BU(BU_ID),
    DEMAND_TO_REGION as DEMAND(REGION_ID) references REGIONS(REGION_ID)
  )
  facts (
    WORKFORCE.HEADCOUNT as headcount comment='Number of employees',
    WORKFORCE.FTE_AVAILABLE as fte_available comment='Full-time equivalent available',
    WORKFORCE.FTE_ALLOCATED as fte_allocated comment='FTE allocated to projects',
    WORKFORCE.FTE_REMAINING as fte_remaining comment='FTE remaining for new work',
    DEMAND.DEMAND_FTE as demand_fte comment='FTE required by demand',
    DEMAND.REVENUE_FORECAST_EUR as revenue_forecast comment='Revenue forecast in EUR',
    WORKFORCE.RECORD as 1 comment='Record count'
  )
  dimensions (
    WORKFORCE.YEAR_MONTH as year_month with synonyms=('month','date','planning month') comment='Planning month',
    BU.BU_NAME as bu_name with synonyms=('business unit','division') comment='Business unit name',
    REGIONS.REGION_NAME as region_name with synonyms=('region','territory') comment='French region',
    SKILLS.SKILL_CATEGORY_NAME as skill_category_name with synonyms=('skill','competency') comment='Skill category'
  )
  metrics (
    WORKFORCE.TOTAL_HEADCOUNT as SUM(workforce.headcount) comment='Total headcount',
    WORKFORCE.TOTAL_FTE as SUM(workforce.fte_available) comment='Total FTE available',
    WORKFORCE.AVG_UTILIZATION as AVG(workforce.fte_allocated / NULLIF(workforce.fte_available, 0) * 100) comment='Average utilization'
  )
  comment='Resource & Capacity Planning - 18 month workforce forecasting';


-- SEMANTIC VIEW 2: ESG Reporting  
create or replace semantic view TDF_DATA_PLATFORM.ANALYTICS.SV_ESG_REPORTING
  tables (
    SCORECARD as TDF_DATA_PLATFORM.ESG.BOARD_SCORECARD primary key (SCORECARD_ID)
      with synonyms=('ESG','sustainability','carbon','emissions') 
      comment='ESG Board Scorecard with carbon emissions and Index Egalite'
  )
  facts (
    SCORECARD.CARBON_EMISSIONS_TONNES as carbon_tonnes comment='Carbon emissions in tonnes',
    SCORECARD.CARBON_INTENSITY_KG_EUR as carbon_intensity comment='Carbon intensity kg per EUR',
    SCORECARD.RENEWABLE_ENERGY_PCT as renewable_pct comment='Renewable energy percentage',
    SCORECARD.TOTAL_EMPLOYEES as employees comment='Total employees',
    SCORECARD.FEMALE_EMPLOYEES_PCT as female_pct comment='Female employees percentage',
    SCORECARD.EQUALITY_INDEX_SCORE as egalite_score comment='Index Egalite score (target >=75)',
    SCORECARD.TRAINING_HOURS_PER_EMPLOYEE as training_hours comment='Training hours per employee',
    SCORECARD.RECORD as 1 comment='Record count'
  )
  dimensions (
    SCORECARD.FISCAL_YEAR as fiscal_year with synonyms=('year') comment='Fiscal year',
    SCORECARD.REPORTING_DATE as reporting_date with synonyms=('date') comment='Reporting date',
    SCORECARD.ENVIRONMENTAL_STATUS as env_status with synonyms=('environmental status') comment='Environmental compliance status',
    SCORECARD.SOCIAL_STATUS as social_status comment='Social compliance status',
    SCORECARD.OVERALL_ESG_STATUS as esg_status with synonyms=('status','ESG status') comment='Overall ESG status'
  )
  metrics (
    SCORECARD.TOTAL_EMISSIONS as SUM(scorecard.carbon_tonnes) comment='Total carbon emissions',
    SCORECARD.AVG_EGALITE as AVG(scorecard.egalite_score) comment='Average Index Egalite score'
  )
  comment='ESG Regulatory Reporting - CSRD, Bilan GES, Index Egalite';


-- SEMANTIC VIEW 3: Digital Twin Infrastructure
create or replace semantic view TDF_DATA_PLATFORM.ANALYTICS.SV_DIGITAL_TWIN
  tables (
    SITES as TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES primary key (SITE_ID)
      with synonyms=('infrastructure','towers','pylons','sites') 
      comment='Infrastructure sites - 2,000+ pylons across France',
    REGIONS as TDF_DATA_PLATFORM.CORE.REGIONS primary key (REGION_ID)
      with synonyms=('region','territory') 
      comment='French regions',
    DEPTS as TDF_DATA_PLATFORM.CORE.DEPARTMENTS primary key (DEPARTMENT_ID)
      with synonyms=('department') 
      comment='Departments'
  )
  relationships (
    SITES_TO_DEPT as SITES(DEPARTMENT_ID) references DEPTS(DEPARTMENT_ID),
    DEPT_TO_REGION as DEPTS(REGION_ID) references REGIONS(REGION_ID)
  )
  facts (
    SITES.CURRENT_TENANTS as tenants comment='Current number of tenants',
    SITES.MAX_TENANTS as max_tenants comment='Maximum tenant capacity',
    SITES.COLOCATION_RATE as colocation_rate comment='Colocation rate',
    SITES.RISK_SCORE as risk_score comment='Site risk score',
    SITES.RECORD as 1 comment='Site count'
  )
  dimensions (
    SITES.SITE_TYPE as site_type with synonyms=('type') comment='Site type (TOWER/ROOFTOP/INDOOR)',
    SITES.STATUS as site_status with synonyms=('status') comment='Site operational status',
    SITES.DIGITAL_TWIN_STATUS as dt_status with synonyms=('digital twin','sync status') comment='Digital Twin sync status',
    DEPTS.DEPARTMENT_NAME as department with synonyms=('dept') comment='Department name',
    REGIONS.REGION_NAME as region with synonyms=('territory') comment='French region'
  )
  metrics (
    SITES.TOTAL_SITES as COUNT(sites.record) comment='Total number of sites',
    SITES.AVG_TENANTS as AVG(sites.tenants) comment='Average tenants per site',
    SITES.DISCREPANCY_COUNT as COUNT(CASE WHEN sites.dt_status = 'DISCREPANCY' THEN 1 END) comment='Sites with discrepancies'
  )
  comment='Digital Twin & Infrastructure - 2,000+ pylons harmonized';


-- SEMANTIC VIEW 4: CAPEX & Lifecycle
create or replace semantic view TDF_DATA_PLATFORM.ANALYTICS.SV_CAPEX_LIFECYCLE
  tables (
    EQUIPMENT as TDF_DATA_PLATFORM.INFRASTRUCTURE.EQUIPMENT primary key (EQUIPMENT_ID)
      with synonyms=('equipment','assets','CAPEX') 
      comment='Equipment with 7-10 year lifecycles',
    EQUIP_TYPES as TDF_DATA_PLATFORM.CORE.EQUIPMENT_TYPES primary key (EQUIPMENT_TYPE_ID)
      with synonyms=('equipment type') 
      comment='Equipment type reference'
  )
  relationships (
    EQUIPMENT_TO_TYPE as EQUIPMENT(EQUIPMENT_TYPE_ID) references EQUIP_TYPES(EQUIPMENT_TYPE_ID)
  )
  facts (
    EQUIPMENT.AGE_YEARS as age_years comment='Equipment age in years',
    EQUIPMENT.CONDITION_SCORE as condition_score comment='Condition score 1-10',
    EQUIPMENT.FAILURE_RISK_SCORE as risk_score comment='Failure risk score',
    EQUIPMENT.REPLACEMENT_COST_EUR as replacement_cost comment='Replacement cost in EUR',
    EQUIPMENT.RECORD as 1 comment='Equipment count'
  )
  dimensions (
    EQUIPMENT.LIFECYCLE_STATUS as lifecycle_status with synonyms=('status','life status') comment='Lifecycle status (ACTIVE/AGING/END_OF_LIFE)',
    EQUIPMENT.EQUIPMENT_CATEGORY as category with synonyms=('equipment category') comment='Equipment category',
    EQUIP_TYPES.EQUIPMENT_TYPE_NAME as equipment_type with synonyms=('type') comment='Equipment type name',
    EQUIPMENT.INSTALLATION_DATE as install_date with synonyms=('installed') comment='Installation date',
    EQUIPMENT.EXPECTED_END_OF_LIFE as end_of_life with synonyms=('EOL') comment='Expected end of life date'
  )
  metrics (
    EQUIPMENT.TOTAL_EQUIPMENT as COUNT(equipment.record) comment='Total equipment count',
    EQUIPMENT.AVG_AGE as AVG(equipment.age_years) comment='Average equipment age',
    EQUIPMENT.TOTAL_REPLACEMENT_COST as SUM(equipment.replacement_cost) comment='Total replacement cost'
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

