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
-- 3. SEMANTIC VIEW: RESOURCE & CAPACITY PLANNING (UC1)
-- ============================================================================
-- Using the pre-built VW_CAPACITY_VS_DEMAND view which already joins the tables

CREATE OR REPLACE SEMANTIC VIEW TDF_DATA_PLATFORM.ANALYTICS.SV_RESOURCE_CAPACITY
    TABLES (
        CAPACITY_DEMAND AS TDF_DATA_PLATFORM.ANALYTICS.VW_CAPACITY_VS_DEMAND 
            WITH SYNONYMS=('capacity','demand','workforce','staffing','FTE') 
            COMMENT='Capacity vs demand analysis - 18 month forecasting horizon'
    )
    FACTS (
        CAPACITY_DEMAND.HEADCOUNT AS HEADCOUNT COMMENT='Number of employees',
        CAPACITY_DEMAND.FTE_AVAILABLE AS FTE_AVAILABLE COMMENT='Full-time equivalent capacity',
        CAPACITY_DEMAND.FTE_ALLOCATED AS FTE_ALLOCATED COMMENT='FTE allocated to projects',
        CAPACITY_DEMAND.FTE_REMAINING AS FTE_REMAINING COMMENT='FTE available for new work',
        CAPACITY_DEMAND.DEMAND_FTE AS DEMAND_FTE COMMENT='FTE required by demand',
        CAPACITY_DEMAND.REVENUE_FORECAST_EUR AS REVENUE_EUR COMMENT='Revenue forecast in EUR',
        CAPACITY_DEMAND.FTE_GAP AS FTE_GAP COMMENT='Gap between capacity and demand',
        CAPACITY_DEMAND.UTILIZATION_PCT AS UTILIZATION_PCT COMMENT='Utilization percentage'
    )
    DIMENSIONS (
        CAPACITY_DEMAND.YEAR_MONTH AS YEAR_MONTH WITH SYNONYMS=('month','period','date') COMMENT='Planning month',
        CAPACITY_DEMAND.BU_NAME AS BU_NAME WITH SYNONYMS=('business unit','division') COMMENT='Business unit name',
        CAPACITY_DEMAND.REGION_NAME AS REGION_NAME WITH SYNONYMS=('region','territory') COMMENT='French region',
        CAPACITY_DEMAND.SKILL_CATEGORY_NAME AS SKILL_NAME WITH SYNONYMS=('skill','competency') COMMENT='Skill category',
        CAPACITY_DEMAND.CAPACITY_STATUS AS CAPACITY_STATUS WITH SYNONYMS=('status') COMMENT='Capacity status (SUFFICIENT, TIGHT, SHORTAGE)'
    )
    METRICS (
        CAPACITY_DEMAND.TOTAL_HEADCOUNT AS SUM(CAPACITY_DEMAND.HEADCOUNT) COMMENT='Total headcount',
        CAPACITY_DEMAND.TOTAL_FTE AS SUM(CAPACITY_DEMAND.FTE_AVAILABLE) COMMENT='Total FTE available',
        CAPACITY_DEMAND.TOTAL_DEMAND AS SUM(CAPACITY_DEMAND.DEMAND_FTE) COMMENT='Total demand FTE',
        CAPACITY_DEMAND.TOTAL_GAP AS SUM(CAPACITY_DEMAND.FTE_GAP) COMMENT='Total FTE gap',
        CAPACITY_DEMAND.AVG_UTILIZATION AS AVG(CAPACITY_DEMAND.UTILIZATION_PCT) COMMENT='Average utilization'
    )
    COMMENT='Resource & Capacity Planning - 18 month forecasting, staffing based on commercial contribution';


-- ============================================================================
-- 4. SEMANTIC VIEW: ESG REGULATORY REPORTING (UC2)
-- ============================================================================
-- Using the pre-built ESG views for compliance tracking

CREATE OR REPLACE SEMANTIC VIEW TDF_DATA_PLATFORM.ANALYTICS.SV_ESG_REPORTING
    TABLES (
        ESG_DASHBOARD AS TDF_DATA_PLATFORM.ANALYTICS.VW_ESG_DASHBOARD 
            WITH SYNONYMS=('ESG','sustainability','carbon','emissions') 
            COMMENT='ESG dashboard metrics for regulatory reporting',
        CARBON AS TDF_DATA_PLATFORM.ANALYTICS.VW_CARBON_BY_REGION 
            WITH SYNONYMS=('carbon emissions','CO2','Bilan GES','greenhouse gas') 
            COMMENT='Carbon emissions by region and scope'
    )
    FACTS (
        ESG_DASHBOARD.CARBON_EMISSIONS_TONNES AS TOTAL_EMISSIONS COMMENT='Total carbon emissions in tonnes',
        ESG_DASHBOARD.CARBON_INTENSITY_KG_EUR AS CARBON_INTENSITY COMMENT='Carbon intensity kg/EUR',
        ESG_DASHBOARD.RENEWABLE_ENERGY_PCT AS RENEWABLE_PCT COMMENT='Renewable energy percentage',
        ESG_DASHBOARD.TOTAL_EMPLOYEES AS EMPLOYEE_COUNT COMMENT='Total employees',
        ESG_DASHBOARD.FEMALE_EMPLOYEES_PCT AS FEMALE_PCT COMMENT='Female employees percentage',
        ESG_DASHBOARD.FEMALE_MANAGEMENT_PCT AS FEMALE_MGMT_PCT COMMENT='Female management percentage',
        ESG_DASHBOARD.EQUALITY_INDEX_SCORE AS EGALITE_INDEX COMMENT='Index Égalité score (target ≥75)',
        ESG_DASHBOARD.TRAINING_HOURS_PER_EMPLOYEE AS TRAINING_HOURS COMMENT='Training hours per employee',
        ESG_DASHBOARD.BOARD_INDEPENDENCE_PCT AS BOARD_INDEPENDENCE COMMENT='Board independence percentage',
        ESG_DASHBOARD.BOARD_FEMALE_PCT AS BOARD_FEMALE_PCT COMMENT='Board female percentage',
        CARBON.TOTAL_EMISSIONS_TONNES AS REGIONAL_EMISSIONS COMMENT='Regional emissions in tonnes',
        CARBON.AVG_CARBON_INTENSITY AS AVG_INTENSITY COMMENT='Average carbon intensity'
    )
    DIMENSIONS (
        ESG_DASHBOARD.FISCAL_YEAR AS FISCAL_YEAR WITH SYNONYMS=('year') COMMENT='Fiscal year',
        ESG_DASHBOARD.ENVIRONMENTAL_STATUS AS ENV_STATUS COMMENT='Environmental status (GREEN/AMBER/RED)',
        ESG_DASHBOARD.SOCIAL_STATUS AS SOCIAL_STATUS COMMENT='Social status',
        ESG_DASHBOARD.GOVERNANCE_STATUS AS GOV_STATUS COMMENT='Governance status',
        ESG_DASHBOARD.OVERALL_ESG_STATUS AS ESG_STATUS WITH SYNONYMS=('status') COMMENT='Overall ESG status',
        CARBON.REGION_NAME AS REGION_NAME WITH SYNONYMS=('region') COMMENT='Region name',
        CARBON.EMISSION_SCOPE AS EMISSION_SCOPE WITH SYNONYMS=('scope','scope 1','scope 2','scope 3') COMMENT='GHG Protocol scope'
    )
    METRICS (
        ESG_DASHBOARD.AVG_EGALITE AS AVG(ESG_DASHBOARD.EQUALITY_INDEX_SCORE) COMMENT='Average Index Égalité',
        CARBON.TOTAL_CARBON AS SUM(CARBON.TOTAL_EMISSIONS_TONNES) COMMENT='Total carbon emissions'
    )
    COMMENT='ESG Regulatory Reporting - CSRD, Bilan GES, Index Égalité with audit trail';


-- ============================================================================
-- 5. SEMANTIC VIEW: DIGITAL TWIN & INFRASTRUCTURE (UC3)
-- ============================================================================
-- Using pre-built infrastructure views

CREATE OR REPLACE SEMANTIC VIEW TDF_DATA_PLATFORM.ANALYTICS.SV_DIGITAL_TWIN
    TABLES (
        INFRASTRUCTURE AS TDF_DATA_PLATFORM.ANALYTICS.VW_INFRASTRUCTURE_HEALTH 
            WITH SYNONYMS=('infrastructure','sites','towers','pylons') 
            COMMENT='Infrastructure health - 2,000+ pylons across France',
        COLOCATION AS TDF_DATA_PLATFORM.ANALYTICS.VW_COLOCATION_ANALYSIS 
            WITH SYNONYMS=('colocation','tenants','clients') 
            COMMENT='Colocation analysis by site type and region',
        DATA_QUALITY AS TDF_DATA_PLATFORM.ANALYTICS.VW_DIGITAL_TWIN_QUALITY 
            WITH SYNONYMS=('data quality','DQ score') 
            COMMENT='Digital Twin data quality scores',
        DISCREPANCIES AS TDF_DATA_PLATFORM.ANALYTICS.VW_DISCREPANCY_SUMMARY 
            WITH SYNONYMS=('discrepancy','mismatch','issue') 
            COMMENT='Discrepancy log for data harmonization'
    )
    FACTS (
        INFRASTRUCTURE.SITE_COUNT AS SITE_COUNT COMMENT='Number of sites',
        INFRASTRUCTURE.AVG_TENANTS AS AVG_TENANTS COMMENT='Average tenants per site',
        INFRASTRUCTURE.AVG_COLOCATION_RATE AS AVG_COLOCATION COMMENT='Average colocation rate',
        INFRASTRUCTURE.AVG_RISK_SCORE AS AVG_RISK COMMENT='Average site risk score',
        INFRASTRUCTURE.DT_SYNCED_COUNT AS SYNCED_COUNT COMMENT='Sites synced with Digital Twin',
        INFRASTRUCTURE.DT_DISCREPANCY_COUNT AS DISCREPANCY_COUNT COMMENT='Sites with discrepancies',
        COLOCATION.TOTAL_SITES AS TOTAL_SITES COMMENT='Total sites',
        COLOCATION.TOTAL_TENANTS AS TOTAL_TENANTS COMMENT='Total tenants',
        COLOCATION.COLOCATION_RATE AS COLOCATION_RATE COMMENT='Colocation rate',
        COLOCATION.TOTAL_ANNUAL_REVENUE AS ANNUAL_REVENUE COMMENT='Annual revenue from colocation',
        DATA_QUALITY.OVERALL_SCORE AS DQ_SCORE COMMENT='Data quality score (0-100)',
        DATA_QUALITY.COMPLETENESS_SCORE AS COMPLETENESS COMMENT='Data completeness score',
        DATA_QUALITY.ACCURACY_SCORE AS ACCURACY COMMENT='Data accuracy score',
        DATA_QUALITY.OPEN_DISCREPANCIES AS OPEN_ISSUES COMMENT='Open discrepancies count',
        DISCREPANCIES.DISCREPANCY_COUNT AS ISSUES_COUNT COMMENT='Number of discrepancies',
        DISCREPANCIES.AVG_DAYS_OPEN AS AVG_DAYS_OPEN COMMENT='Average days issues are open'
    )
    DIMENSIONS (
        INFRASTRUCTURE.SITE_TYPE AS SITE_TYPE WITH SYNONYMS=('type') COMMENT='Site type (TOWER, ROOFTOP, INDOOR)',
        INFRASTRUCTURE.STATUS AS SITE_STATUS WITH SYNONYMS=('status') COMMENT='Site operational status',
        INFRASTRUCTURE.DEPARTMENT_NAME AS DEPARTMENT COMMENT='Department name',
        INFRASTRUCTURE.REGION_NAME AS REGION_NAME WITH SYNONYMS=('region') COMMENT='French region',
        COLOCATION.SITE_TYPE AS COLO_SITE_TYPE COMMENT='Site type for colocation',
        COLOCATION.REGION_NAME AS COLO_REGION COMMENT='Region for colocation',
        DATA_QUALITY.ENTITY_TYPE AS ENTITY_TYPE COMMENT='Entity type scored',
        DATA_QUALITY.SCORE_LABEL AS QUALITY_LABEL COMMENT='Quality label (EXCELLENT, GOOD, etc.)',
        DISCREPANCIES.STATUS AS DISC_STATUS COMMENT='Discrepancy status (OPEN, RESOLVED)',
        DISCREPANCIES.SEVERITY AS DISC_SEVERITY WITH SYNONYMS=('severity') COMMENT='Discrepancy severity',
        DISCREPANCIES.DISCREPANCY_TYPE AS DISC_TYPE COMMENT='Type of discrepancy'
    )
    METRICS (
        INFRASTRUCTURE.TOTAL_SITES AS SUM(INFRASTRUCTURE.SITE_COUNT) COMMENT='Total sites',
        DATA_QUALITY.AVG_QUALITY AS AVG(DATA_QUALITY.OVERALL_SCORE) COMMENT='Average data quality'
    )
    COMMENT='Digital Twin & Infrastructure - 2,000+ pylons harmonized, discrepancy detection';


-- ============================================================================
-- 6. SEMANTIC VIEW: CAPEX & LIFECYCLE MANAGEMENT (UC4)
-- ============================================================================
-- Using pre-built CAPEX and lifecycle views

CREATE OR REPLACE SEMANTIC VIEW TDF_DATA_PLATFORM.ANALYTICS.SV_CAPEX_LIFECYCLE
    TABLES (
        LIFECYCLE AS TDF_DATA_PLATFORM.ANALYTICS.VW_EQUIPMENT_LIFECYCLE 
            WITH SYNONYMS=('equipment','lifecycle','assets') 
            COMMENT='Equipment lifecycle - 7-10 year lifespans',
        CAPEX AS TDF_DATA_PLATFORM.ANALYTICS.VW_CAPEX_BUDGET_VS_ACTUAL 
            WITH SYNONYMS=('CAPEX','budget','capital') 
            COMMENT='CAPEX budget vs actual spending',
        RENEWAL AS TDF_DATA_PLATFORM.ANALYTICS.VW_RENEWAL_FORECAST_SUMMARY 
            WITH SYNONYMS=('renewal','replacement','forecast') 
            COMMENT='Predictive renewal model',
        AT_RISK AS TDF_DATA_PLATFORM.ANALYTICS.VW_EQUIPMENT_AT_RISK 
            WITH SYNONYMS=('at risk','failure','critical') 
            COMMENT='Equipment at risk of failure'
    )
    FACTS (
        LIFECYCLE.EQUIPMENT_COUNT AS EQUIPMENT_COUNT COMMENT='Number of equipment items',
        LIFECYCLE.AVG_AGE_YEARS AS AVG_AGE COMMENT='Average equipment age (7-10 year lifecycle)',
        LIFECYCLE.AVG_CONDITION_SCORE AS AVG_CONDITION COMMENT='Average condition score',
        LIFECYCLE.AVG_RISK_SCORE AS AVG_RISK COMMENT='Average failure risk',
        LIFECYCLE.TOTAL_REPLACEMENT_COST AS REPLACEMENT_COST COMMENT='Total replacement cost',
        LIFECYCLE.PAST_END_OF_LIFE_COUNT AS PAST_EOL_COUNT COMMENT='Equipment past end of life',
        CAPEX.BUDGET_EUR AS BUDGET_EUR COMMENT='CAPEX budget',
        CAPEX.ACTUAL_YTD_EUR AS ACTUAL_EUR COMMENT='Actual CAPEX YTD',
        CAPEX.REMAINING_EUR AS REMAINING_EUR COMMENT='Remaining budget',
        CAPEX.UTILIZED_PCT AS UTILIZED_PCT COMMENT='Budget utilized percentage',
        RENEWAL.EQUIPMENT_COUNT AS RENEWAL_COUNT COMMENT='Equipment due for renewal',
        RENEWAL.TOTAL_REPLACEMENT_COST AS RENEWAL_COST COMMENT='Total renewal cost',
        RENEWAL.AVG_AGE_YEARS AS RENEWAL_AVG_AGE COMMENT='Average age of renewal items',
        RENEWAL.BUDGETED_COST AS BUDGETED_RENEWAL COMMENT='Budgeted renewal cost',
        RENEWAL.UNBUDGETED_COST AS UNBUDGETED_RENEWAL COMMENT='Unbudgeted renewal cost'
    )
    DIMENSIONS (
        LIFECYCLE.LIFECYCLE_STATUS AS LIFECYCLE_STATUS WITH SYNONYMS=('status','life status') COMMENT='Lifecycle status (ACTIVE, AGING, END_OF_LIFE)',
        LIFECYCLE.EQUIPMENT_CATEGORY AS EQUIPMENT_CATEGORY WITH SYNONYMS=('category') COMMENT='Equipment category',
        LIFECYCLE.EQUIPMENT_TYPE_NAME AS EQUIPMENT_TYPE COMMENT='Equipment type',
        CAPEX.FISCAL_YEAR AS FISCAL_YEAR WITH SYNONYMS=('year') COMMENT='Fiscal year',
        CAPEX.CAPEX_CATEGORY AS CAPEX_CATEGORY WITH SYNONYMS=('CAPEX type') COMMENT='CAPEX category (MAINTENANCE, GROWTH)',
        CAPEX.CAPEX_SUBCATEGORY AS CAPEX_SUBCATEGORY COMMENT='CAPEX subcategory',
        CAPEX.BU_NAME AS BU_NAME WITH SYNONYMS=('business unit') COMMENT='Business unit',
        CAPEX.BUDGET_STATUS AS BUDGET_STATUS WITH SYNONYMS=('status') COMMENT='Budget status (ON_TRACK, OVER_BUDGET)',
        RENEWAL.REPLACEMENT_YEAR AS REPLACEMENT_YEAR WITH SYNONYMS=('renewal year') COMMENT='Year for replacement',
        RENEWAL.REPLACEMENT_PRIORITY AS RENEWAL_PRIORITY WITH SYNONYMS=('priority') COMMENT='Replacement priority'
    )
    METRICS (
        LIFECYCLE.TOTAL_EQUIPMENT AS SUM(LIFECYCLE.EQUIPMENT_COUNT) COMMENT='Total equipment',
        CAPEX.TOTAL_BUDGET AS SUM(CAPEX.BUDGET_EUR) COMMENT='Total CAPEX budget',
        CAPEX.TOTAL_ACTUAL AS SUM(CAPEX.ACTUAL_YTD_EUR) COMMENT='Total CAPEX actual',
        RENEWAL.TOTAL_RENEWAL AS SUM(RENEWAL.TOTAL_REPLACEMENT_COST) COMMENT='Total renewal cost'
    )
    COMMENT='CAPEX & Lifecycle - 7-10 year equipment lifecycles, predictive renewal model';


-- ============================================================================
-- 7. SHOW CREATED SEMANTIC VIEWS
-- ============================================================================

SHOW SEMANTIC VIEWS IN SCHEMA TDF_DATA_PLATFORM.ANALYTICS;


-- ============================================================================
-- 8. CREATE THE TDF INTELLIGENCE AGENT
-- ============================================================================

USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.TDF_DATA_PLATFORM_AGENT
WITH PROFILE='{ "display_name": "TDF Data Platform Assistant" }'
    COMMENT=$$ 
    TDF Infrastructure Data Platform AI Assistant.
    Addresses 4 priority use cases:
    - P1: Resource & Capacity Planning (18-month forecasting, scenario modeling)
    - P1: ESG Regulatory Reporting (CSRD, Bilan GES, Index Égalité with audit trail)
    - P2: Digital Twin & Infrastructure (2,000+ pylons, data quality control)
    - P2: CAPEX & Lifecycle Management (7-10 year equipment lifecycles)
    $$
FROM SPECIFICATION $$
{
  "models": {
    "orchestration": "claude-3-5-sonnet"
  },
  "instructions": {
    "response": "You are the TDF Data Platform Assistant, helping users analyze infrastructure, workforce, ESG, and financial data for TDF, the leading French telecom infrastructure company. Always provide clear, actionable insights. When showing data, include context about TDF's business: €799M revenue, 2,000+ towers, BBB- credit rating. Use visualizations when helpful.",
    "orchestration": "Select the appropriate datamart based on the question:\n- Resource & Capacity: workforce, hiring, demand forecasting, utilization\n- ESG Reporting: carbon emissions, energy, diversity, Index Égalité, compliance\n- Digital Twin: sites, towers, equipment inventory, data quality, discrepancies\n- CAPEX & Lifecycle: equipment age, renewal forecast, budget vs actuals\n\nFor capacity planning questions, the forecast horizon is 18 months.\nFor ESG questions, emphasize audit trail and data lineage.\nFor Digital Twin questions, note we have 2,000+ pylons.\nFor CAPEX questions, equipment lifecycles are typically 7-10 years.",
    "sample_questions": [
      {
        "question": "What is our workforce capacity vs demand for the next 18 months?"
      },
      {
        "question": "Show me our carbon emissions by scope for the current year"
      },
      {
        "question": "What is our current Index Égalité score and are we compliant?"
      },
      {
        "question": "How many sites have Digital Twin discrepancies?"
      },
      {
        "question": "What equipment is due for renewal in the next 3 years?"
      },
      {
        "question": "What is our CAPEX budget vs actual for this fiscal year?"
      }
    ]
  },
  "tools": [
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "Query Resource & Capacity Planning",
        "description": "Query workforce capacity, demand forecasting, utilization, and work orders. Use for questions about hiring needs, capacity gaps, 18-month forecasts, and staffing based on commercial contribution. This replaces the 3-day Excel process."
      }
    },
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "Query ESG Regulatory Reporting",
        "description": "Query ESG data including carbon emissions (Bilan GES), energy consumption, renewable energy, diversity metrics, Index Égalité (French gender equality), and compliance status. Provides full data lineage for external audit requirements. Covers CSRD, DPEF, and EU Taxonomy."
      }
    },
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "Query Digital Twin & Infrastructure",
        "description": "Query infrastructure data including 2,000+ pylons/towers, sites, equipment inventory, client installations, data quality scores, and discrepancy detection. Use for Digital Twin coherence validation and infrastructure data harmonization questions."
      }
    },
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "Query CAPEX & Lifecycle Management",
        "description": "Query equipment lifecycle data, renewal forecasts, CAPEX budgets and actuals. Equipment typically has 7-10 year lifecycles. Use for predictive renewal modeling, installation dates, life status tracking, and CAPEX planning questions."
      }
    }
  ],
  "tool_resources": {
    "Query Resource & Capacity Planning": {
      "semantic_view": "TDF_DATA_PLATFORM.ANALYTICS.SV_RESOURCE_CAPACITY"
    },
    "Query ESG Regulatory Reporting": {
      "semantic_view": "TDF_DATA_PLATFORM.ANALYTICS.SV_ESG_REPORTING"
    },
    "Query Digital Twin & Infrastructure": {
      "semantic_view": "TDF_DATA_PLATFORM.ANALYTICS.SV_DIGITAL_TWIN"
    },
    "Query CAPEX & Lifecycle Management": {
      "semantic_view": "TDF_DATA_PLATFORM.ANALYTICS.SV_CAPEX_LIFECYCLE"
    }
  }
}
$$;

-- ============================================================================
-- 9. GRANT ACCESS TO THE AGENT
-- ============================================================================

GRANT USAGE ON AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.TDF_DATA_PLATFORM_AGENT TO ROLE PUBLIC;

SELECT 'TDF INTELLIGENCE AGENT CREATED SUCCESSFULLY' AS STATUS;

-- ============================================================================
-- 10. SAMPLE QUESTIONS TO TEST THE AGENT
-- ============================================================================

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

