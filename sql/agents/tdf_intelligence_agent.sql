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
-- 3. SEMANTIC VIEW: RESOURCE & CAPACITY PLANNING (UC1)
-- ============================================================================
-- Source: VW_CAPACITY_VS_DEMAND view

CREATE OR REPLACE SEMANTIC VIEW TDF_DATA_PLATFORM.ANALYTICS.SV_RESOURCE_CAPACITY
    TABLES (
        CAP AS TDF_DATA_PLATFORM.ANALYTICS.VW_CAPACITY_VS_DEMAND 
            WITH SYNONYMS=('capacity','demand','workforce','staffing','FTE') 
            COMMENT='Capacity vs demand analysis - 18 month forecasting horizon'
    )
    FACTS (
        CAP.HEADCOUNT AS headcount COMMENT='Number of employees',
        CAP.FTE_AVAILABLE AS fte_available COMMENT='Full-time equivalent capacity',
        CAP.FTE_REMAINING AS fte_remaining COMMENT='FTE available for new work',
        CAP.DEMAND_FTE AS demand_fte COMMENT='FTE required by demand',
        CAP.FTE_GAP AS fte_gap COMMENT='Gap between capacity and demand',
        CAP.UTILIZATION_PCT AS utilization COMMENT='Utilization percentage'
    )
    DIMENSIONS (
        CAP.YEAR_MONTH AS period WITH SYNONYMS=('month','date','year month') COMMENT='Planning month',
        CAP.BU_NAME AS business_unit WITH SYNONYMS=('BU','division') COMMENT='Business unit name',
        CAP.REGION_NAME AS region WITH SYNONYMS=('territory') COMMENT='French region',
        CAP.SKILL_CATEGORY_NAME AS skill WITH SYNONYMS=('competency') COMMENT='Skill category',
        CAP.CAPACITY_STATUS AS status COMMENT='Capacity status (SUFFICIENT, TIGHT, SHORTAGE)'
    )
    METRICS (
        CAP.total_headcount AS SUM(CAP.headcount) COMMENT='Total headcount',
        CAP.total_fte AS SUM(CAP.fte_available) COMMENT='Total FTE available',
        CAP.total_demand AS SUM(CAP.demand_fte) COMMENT='Total demand FTE',
        CAP.total_gap AS SUM(CAP.fte_gap) COMMENT='Total FTE gap',
        CAP.avg_utilization AS AVG(CAP.utilization) COMMENT='Average utilization'
    )
    COMMENT='Resource & Capacity Planning - 18 month forecasting';


-- ============================================================================
-- 4. SEMANTIC VIEW: ESG REGULATORY REPORTING (UC2)
-- ============================================================================
-- Source: VW_ESG_DASHBOARD view

CREATE OR REPLACE SEMANTIC VIEW TDF_DATA_PLATFORM.ANALYTICS.SV_ESG_REPORTING
    TABLES (
        ESG AS TDF_DATA_PLATFORM.ANALYTICS.VW_ESG_DASHBOARD 
            WITH SYNONYMS=('sustainability','carbon','emissions','Index Egalite') 
            COMMENT='ESG dashboard metrics for regulatory reporting'
    )
    FACTS (
        ESG.CARBON_EMISSIONS_TONNES AS carbon_tonnes COMMENT='Carbon emissions in tonnes',
        ESG.CARBON_INTENSITY_KG_EUR AS carbon_intensity COMMENT='Carbon intensity kg/EUR',
        ESG.RENEWABLE_ENERGY_PCT AS renewable_pct COMMENT='Renewable energy percentage',
        ESG.TOTAL_EMPLOYEES AS employees COMMENT='Total employees',
        ESG.FEMALE_EMPLOYEES_PCT AS female_pct COMMENT='Female employees percentage',
        ESG.EQUALITY_INDEX_SCORE AS egalite_index COMMENT='Index Égalité score (target ≥75)',
        ESG.TRAINING_HOURS_PER_EMPLOYEE AS training_hours COMMENT='Training hours per employee'
    )
    DIMENSIONS (
        ESG.FISCAL_YEAR AS fiscal_year WITH SYNONYMS=('year') COMMENT='Fiscal year',
        ESG.ENVIRONMENTAL_STATUS AS env_status WITH SYNONYMS=('environmental') COMMENT='Environmental status',
        ESG.SOCIAL_STATUS AS social_status COMMENT='Social status',
        ESG.OVERALL_ESG_STATUS AS esg_status WITH SYNONYMS=('status') COMMENT='Overall ESG status'
    )
    METRICS (
        ESG.total_emissions AS SUM(ESG.carbon_tonnes) COMMENT='Total carbon emissions',
        ESG.avg_egalite AS AVG(ESG.egalite_index) COMMENT='Average Index Égalité'
    )
    COMMENT='ESG Regulatory Reporting - CSRD, Bilan GES, Index Égalité';


-- ============================================================================
-- 5. SEMANTIC VIEW: DIGITAL TWIN & INFRASTRUCTURE (UC3)
-- ============================================================================
-- Source: VW_INFRASTRUCTURE_HEALTH view

CREATE OR REPLACE SEMANTIC VIEW TDF_DATA_PLATFORM.ANALYTICS.SV_DIGITAL_TWIN
    TABLES (
        INFRA AS TDF_DATA_PLATFORM.ANALYTICS.VW_INFRASTRUCTURE_HEALTH 
            WITH SYNONYMS=('infrastructure','sites','towers','pylons','digital twin') 
            COMMENT='Infrastructure health - 2,000+ pylons across France'
    )
    FACTS (
        INFRA.SITE_COUNT AS site_count COMMENT='Number of sites',
        INFRA.AVG_TENANTS AS avg_tenants COMMENT='Average tenants per site',
        INFRA.AVG_COLOCATION_RATE AS colocation_rate COMMENT='Average colocation rate',
        INFRA.AVG_RISK_SCORE AS risk_score COMMENT='Average site risk score',
        INFRA.DT_SYNCED_COUNT AS synced_count COMMENT='Sites synced with Digital Twin',
        INFRA.DT_DISCREPANCY_COUNT AS discrepancy_count COMMENT='Sites with discrepancies'
    )
    DIMENSIONS (
        INFRA.SITE_TYPE AS site_type WITH SYNONYMS=('type') COMMENT='Site type (TOWER, ROOFTOP, INDOOR)',
        INFRA.STATUS AS site_status WITH SYNONYMS=('status') COMMENT='Site operational status',
        INFRA.DEPARTMENT_NAME AS department COMMENT='Department name',
        INFRA.REGION_NAME AS region WITH SYNONYMS=('territory') COMMENT='French region'
    )
    METRICS (
        INFRA.total_sites AS SUM(INFRA.site_count) COMMENT='Total sites',
        INFRA.total_discrepancies AS SUM(INFRA.discrepancy_count) COMMENT='Total discrepancies'
    )
    COMMENT='Digital Twin & Infrastructure - 2,000+ pylons harmonized';


-- ============================================================================
-- 6. SEMANTIC VIEW: CAPEX & LIFECYCLE MANAGEMENT (UC4)
-- ============================================================================
-- Source: VW_EQUIPMENT_LIFECYCLE view

CREATE OR REPLACE SEMANTIC VIEW TDF_DATA_PLATFORM.ANALYTICS.SV_CAPEX_LIFECYCLE
    TABLES (
        EQUIP AS TDF_DATA_PLATFORM.ANALYTICS.VW_EQUIPMENT_LIFECYCLE 
            WITH SYNONYMS=('equipment','lifecycle','assets','CAPEX') 
            COMMENT='Equipment lifecycle - 7-10 year lifespans'
    )
    FACTS (
        EQUIP.EQUIPMENT_COUNT AS equipment_count COMMENT='Number of equipment items',
        EQUIP.AVG_AGE_YEARS AS avg_age COMMENT='Average equipment age (7-10 year lifecycle)',
        EQUIP.AVG_CONDITION_SCORE AS condition_score COMMENT='Average condition score',
        EQUIP.AVG_RISK_SCORE AS risk_score COMMENT='Average failure risk',
        EQUIP.TOTAL_REPLACEMENT_COST AS replacement_cost COMMENT='Total replacement cost',
        EQUIP.PAST_END_OF_LIFE_COUNT AS past_eol COMMENT='Equipment past end of life'
    )
    DIMENSIONS (
        EQUIP.LIFECYCLE_STATUS AS lifecycle_status WITH SYNONYMS=('status','life status') COMMENT='Lifecycle status (ACTIVE, AGING, END_OF_LIFE)',
        EQUIP.EQUIPMENT_CATEGORY AS category COMMENT='Equipment category',
        EQUIP.EQUIPMENT_TYPE_NAME AS equipment_type COMMENT='Equipment type'
    )
    METRICS (
        EQUIP.total_equipment AS SUM(EQUIP.equipment_count) COMMENT='Total equipment',
        EQUIP.total_cost AS SUM(EQUIP.replacement_cost) COMMENT='Total replacement cost'
    )
    COMMENT='CAPEX & Lifecycle - 7-10 year equipment lifecycles';


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

