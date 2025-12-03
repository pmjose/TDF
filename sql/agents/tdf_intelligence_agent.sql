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
-- Addresses: 18-month forecasting, dynamic scenario modeling, 
-- staffing based on commercial contribution

CREATE OR REPLACE SEMANTIC VIEW TDF_DATA_PLATFORM.ANALYTICS.SV_RESOURCE_CAPACITY
    TABLES (
        CAPACITY AS HR.WORKFORCE_CAPACITY PRIMARY KEY (CAPACITY_ID) 
            WITH SYNONYMS=('workforce capacity','staffing','headcount','FTE') 
            COMMENT='Workforce capacity by region, skill, and month - 18 month horizon',
        DEMAND AS COMMERCIAL.DEMAND_FORECAST PRIMARY KEY (FORECAST_ID) 
            WITH SYNONYMS=('demand forecast','project demand','workload') 
            COMMENT='Commercial demand forecast by skill and region',
        WORK_ORDERS AS OPERATIONS.WORK_ORDERS PRIMARY KEY (WORK_ORDER_ID) 
            WITH SYNONYMS=('work orders','WO','tasks','jobs') 
            COMMENT='Operational work orders and backlog',
        BUSINESS_UNITS AS CORE.BUSINESS_UNITS PRIMARY KEY (BU_ID) 
            WITH SYNONYMS=('BU','business unit','division') 
            COMMENT='TDF business units',
        REGIONS AS CORE.REGIONS PRIMARY KEY (REGION_ID) 
            WITH SYNONYMS=('region','territory','area') 
            COMMENT='French regions',
        SKILL_CATEGORIES AS CORE.SKILL_CATEGORIES PRIMARY KEY (SKILL_CATEGORY_ID) 
            WITH SYNONYMS=('skill','competency','expertise') 
            COMMENT='Technical skill categories'
    )
    RELATIONSHIPS (
        CAPACITY_TO_BU AS CAPACITY(BU_ID) REFERENCES BUSINESS_UNITS(BU_ID),
        CAPACITY_TO_REGION AS CAPACITY(REGION_ID) REFERENCES REGIONS(REGION_ID),
        CAPACITY_TO_SKILL AS CAPACITY(SKILL_CATEGORY_ID) REFERENCES SKILL_CATEGORIES(SKILL_CATEGORY_ID),
        DEMAND_TO_BU AS DEMAND(BU_ID) REFERENCES BUSINESS_UNITS(BU_ID),
        DEMAND_TO_REGION AS DEMAND(REGION_ID) REFERENCES REGIONS(REGION_ID),
        DEMAND_TO_SKILL AS DEMAND(SKILL_CATEGORY_ID) REFERENCES SKILL_CATEGORIES(SKILL_CATEGORY_ID),
        WO_TO_BU AS WORK_ORDERS(BU_ID) REFERENCES BUSINESS_UNITS(BU_ID),
        WO_TO_REGION AS WORK_ORDERS(REGION_ID) REFERENCES REGIONS(REGION_ID)
    )
    FACTS (
        CAPACITY.HEADCOUNT AS HEADCOUNT COMMENT='Number of employees',
        CAPACITY.FTE_AVAILABLE AS FTE_AVAILABLE COMMENT='Full-time equivalent capacity available',
        CAPACITY.FTE_ALLOCATED AS FTE_ALLOCATED COMMENT='FTE already allocated to projects',
        CAPACITY.FTE_REMAINING AS FTE_REMAINING COMMENT='FTE available for new work',
        CAPACITY.UTILIZATION_PCT AS UTILIZATION_PCT COMMENT='Capacity utilization percentage',
        DEMAND.DEMAND_FTE AS DEMAND_FTE COMMENT='FTE required by demand forecast',
        DEMAND.REVENUE_FORECAST_EUR AS REVENUE_FORECAST_EUR COMMENT='Revenue forecast in EUR based on commercial contribution',
        DEMAND.PROBABILITY_PCT AS PROBABILITY_PCT COMMENT='Probability of demand materializing',
        WORK_ORDERS.ESTIMATED_HOURS AS ESTIMATED_HOURS COMMENT='Estimated hours for work order'
    )
    DIMENSIONS (
        CAPACITY.YEAR_MONTH AS CAPACITY_MONTH WITH SYNONYMS=('month','period','date') COMMENT='Month of capacity record (YYYY-MM format)',
        DEMAND.TARGET_MONTH AS DEMAND_MONTH WITH SYNONYMS=('forecast month','target date') COMMENT='Target month for demand',
        DEMAND.DEMAND_SOURCE AS DEMAND_SOURCE WITH SYNONYMS=('source','origin') COMMENT='Source of demand (PIPELINE, COMMITTED, etc.)',
        DEMAND.FORECAST_CONFIDENCE AS FORECAST_CONFIDENCE COMMENT='Confidence level of forecast',
        WORK_ORDERS.PRIORITY AS WO_PRIORITY WITH SYNONYMS=('priority','urgency') COMMENT='Work order priority (CRITICAL, HIGH, MEDIUM, LOW)',
        WORK_ORDERS.STATUS AS WO_STATUS WITH SYNONYMS=('status','state') COMMENT='Work order status',
        WORK_ORDERS.WORK_ORDER_TYPE AS WO_TYPE WITH SYNONYMS=('type','category') COMMENT='Type of work order',
        BUSINESS_UNITS.BU_NAME AS BU_NAME WITH SYNONYMS=('business unit name','division name') COMMENT='Name of business unit',
        REGIONS.REGION_NAME AS REGION_NAME WITH SYNONYMS=('region name','territory') COMMENT='French region name',
        SKILL_CATEGORIES.SKILL_CATEGORY_NAME AS SKILL_NAME WITH SYNONYMS=('skill name','competency') COMMENT='Technical skill category name'
    )
    METRICS (
        CAPACITY.TOTAL_HEADCOUNT AS SUM(CAPACITY.HEADCOUNT) COMMENT='Total headcount',
        CAPACITY.TOTAL_FTE_AVAILABLE AS SUM(CAPACITY.FTE_AVAILABLE) COMMENT='Total FTE capacity',
        CAPACITY.TOTAL_FTE_REMAINING AS SUM(CAPACITY.FTE_REMAINING) COMMENT='Total remaining capacity',
        CAPACITY.AVG_UTILIZATION AS AVG(CAPACITY.UTILIZATION_PCT) COMMENT='Average utilization rate',
        DEMAND.TOTAL_DEMAND_FTE AS SUM(DEMAND.DEMAND_FTE) COMMENT='Total FTE demand',
        DEMAND.TOTAL_REVENUE_FORECAST AS SUM(DEMAND.REVENUE_FORECAST_EUR) COMMENT='Total forecasted revenue',
        WORK_ORDERS.WO_COUNT AS COUNT(*) COMMENT='Number of work orders',
        WORK_ORDERS.TOTAL_ESTIMATED_HOURS AS SUM(WORK_ORDERS.ESTIMATED_HOURS) COMMENT='Total estimated hours'
    )
    COMMENT='Semantic view for Resource & Capacity Planning - 18 month forecasting, scenario modeling, staffing based on commercial contribution';


-- ============================================================================
-- 4. SEMANTIC VIEW: ESG REGULATORY REPORTING (UC2)
-- ============================================================================
-- Addresses: CSRD, Bilan GES, Index Égalité, data traceability, audit-ready

CREATE OR REPLACE SEMANTIC VIEW TDF_DATA_PLATFORM.ANALYTICS.SV_ESG_REPORTING
    TABLES (
        CARBON_EMISSIONS AS ENERGY.CARBON_EMISSIONS PRIMARY KEY (EMISSION_ID) 
            WITH SYNONYMS=('carbon','CO2','emissions','GHG','greenhouse gas','Bilan GES') 
            COMMENT='Carbon emissions by scope with full traceability to source',
        ENERGY_CONSUMPTION AS ENERGY.CONSUMPTION_READINGS PRIMARY KEY (READING_ID) 
            WITH SYNONYMS=('energy','electricity','consumption','kWh') 
            COMMENT='Energy consumption readings from sites',
        RENEWABLE_ENERGY AS ENERGY.RENEWABLE_ENERGY PRIMARY KEY (RENEWABLE_ID) 
            WITH SYNONYMS=('renewable','green energy','solar','wind') 
            COMMENT='Renewable energy sources and percentages',
        DIVERSITY_METRICS AS HR.DIVERSITY_METRICS PRIMARY KEY (DIVERSITY_ID) 
            WITH SYNONYMS=('diversity','equality','Index Égalité','gender') 
            COMMENT='Diversity and equality metrics for French Index Égalité',
        BOARD_SCORECARD AS ESG.BOARD_SCORECARD PRIMARY KEY (SCORECARD_ID) 
            WITH SYNONYMS=('ESG scorecard','ESG KPIs','sustainability') 
            COMMENT='Board-level ESG scorecard',
        COMPLIANCE AS ESG.COMPLIANCE_REQUIREMENTS PRIMARY KEY (REQUIREMENT_ID) 
            WITH SYNONYMS=('compliance','regulation','CSRD','DPEF') 
            COMMENT='ESG compliance requirements and status',
        AUDIT_TRAIL AS ESG.AUDIT_TRAIL PRIMARY KEY (AUDIT_TRAIL_ID) 
            WITH SYNONYMS=('audit','lineage','traceability') 
            COMMENT='Full data lineage for audit trail',
        REGIONS AS CORE.REGIONS PRIMARY KEY (REGION_ID)
    )
    RELATIONSHIPS (
        CARBON_TO_REGION AS CARBON_EMISSIONS(REGION_ID) REFERENCES REGIONS(REGION_ID),
        DIVERSITY_TO_REGION AS DIVERSITY_METRICS(REGION_ID) REFERENCES REGIONS(REGION_ID)
    )
    FACTS (
        CARBON_EMISSIONS.EMISSIONS_KG_CO2E AS EMISSIONS_KG COMMENT='Carbon emissions in kg CO2 equivalent',
        CARBON_EMISSIONS.EMISSIONS_TONNES_CO2E AS EMISSIONS_TONNES COMMENT='Carbon emissions in tonnes',
        CARBON_EMISSIONS.CARBON_INTENSITY_KG_EUR AS CARBON_INTENSITY COMMENT='Carbon intensity (kg CO2 per EUR revenue)',
        ENERGY_CONSUMPTION.CONSUMPTION_KWH AS ENERGY_KWH COMMENT='Energy consumption in kWh',
        ENERGY_CONSUMPTION.ENERGY_COST_EUR AS ENERGY_COST COMMENT='Energy cost in EUR',
        RENEWABLE_ENERGY.RENEWABLE_PCT AS RENEWABLE_PCT COMMENT='Percentage of renewable energy',
        DIVERSITY_METRICS.FEMALE_PERCENTAGE AS FEMALE_PCT COMMENT='Percentage of female employees',
        DIVERSITY_METRICS.MANAGEMENT_FEMALE_PCT AS MGMT_FEMALE_PCT COMMENT='Percentage of female management',
        DIVERSITY_METRICS.PAY_EQUITY_INDEX AS PAY_EQUITY COMMENT='Pay equity index',
        DIVERSITY_METRICS.EGALITE_INDEX_SCORE AS EGALITE_INDEX COMMENT='French Index Égalité score (target ≥75)',
        BOARD_SCORECARD.EQUALITY_INDEX_SCORE AS ESG_EQUALITY_SCORE COMMENT='Board-level equality index',
        BOARD_SCORECARD.TRAINING_HOURS_PER_EMPLOYEE AS TRAINING_HOURS COMMENT='Training hours per employee'
    )
    DIMENSIONS (
        CARBON_EMISSIONS.EMISSION_SCOPE AS EMISSION_SCOPE WITH SYNONYMS=('scope','scope 1','scope 2','scope 3') COMMENT='GHG Protocol scope (SCOPE_1, SCOPE_2, SCOPE_3)',
        CARBON_EMISSIONS.EMISSION_CATEGORY AS EMISSION_CATEGORY COMMENT='Category of emission (ELECTRICITY, FUEL, etc.)',
        CARBON_EMISSIONS.FISCAL_YEAR AS FISCAL_YEAR COMMENT='Fiscal year for reporting',
        CARBON_EMISSIONS.YEAR_MONTH AS YEAR_MONTH COMMENT='Year-month for trending',
        ENERGY_CONSUMPTION.ENERGY_SOURCE AS ENERGY_SOURCE COMMENT='Source of energy',
        RENEWABLE_ENERGY.RENEWABLE_SOURCE AS RENEWABLE_SOURCE COMMENT='Type of renewable source',
        COMPLIANCE.REGULATION_NAME AS REGULATION WITH SYNONYMS=('regulation name','framework') COMMENT='Regulatory framework (CSRD, DPEF, etc.)',
        COMPLIANCE.COMPLIANCE_STATUS AS COMPLIANCE_STATUS WITH SYNONYMS=('status','compliant') COMMENT='Compliance status',
        COMPLIANCE.COMPLIANCE_DEADLINE AS DEADLINE COMMENT='Compliance deadline',
        BOARD_SCORECARD.OVERALL_ESG_STATUS AS ESG_STATUS COMMENT='Overall ESG status (GREEN, AMBER, RED)',
        AUDIT_TRAIL.SOURCE_SCHEMA AS SOURCE_SCHEMA COMMENT='Source schema for data lineage',
        AUDIT_TRAIL.SOURCE_TABLE AS SOURCE_TABLE COMMENT='Source table for data lineage',
        REGIONS.REGION_NAME AS REGION_NAME
    )
    METRICS (
        CARBON_EMISSIONS.TOTAL_EMISSIONS_TONNES AS SUM(CARBON_EMISSIONS.EMISSIONS_TONNES_CO2E) COMMENT='Total carbon emissions in tonnes',
        CARBON_EMISSIONS.AVG_CARBON_INTENSITY AS AVG(CARBON_EMISSIONS.CARBON_INTENSITY_KG_EUR) COMMENT='Average carbon intensity',
        ENERGY_CONSUMPTION.TOTAL_ENERGY_KWH AS SUM(ENERGY_CONSUMPTION.CONSUMPTION_KWH) COMMENT='Total energy consumption',
        ENERGY_CONSUMPTION.TOTAL_ENERGY_COST AS SUM(ENERGY_CONSUMPTION.ENERGY_COST_EUR) COMMENT='Total energy cost',
        RENEWABLE_ENERGY.AVG_RENEWABLE_PCT AS AVG(RENEWABLE_ENERGY.RENEWABLE_PCT) COMMENT='Average renewable energy percentage',
        DIVERSITY_METRICS.AVG_EGALITE_INDEX AS AVG(DIVERSITY_METRICS.EGALITE_INDEX_SCORE) COMMENT='Average Index Égalité score'
    )
    COMMENT='Semantic view for ESG Regulatory Reporting - CSRD, Bilan GES, Index Égalité with full audit trail and data traceability';


-- ============================================================================
-- 5. SEMANTIC VIEW: DIGITAL TWIN & INFRASTRUCTURE (UC3)
-- ============================================================================
-- Addresses: 2,000+ pylons, data harmonization, discrepancy detection

CREATE OR REPLACE SEMANTIC VIEW TDF_DATA_PLATFORM.ANALYTICS.SV_DIGITAL_TWIN
    TABLES (
        SITES AS INFRASTRUCTURE.SITES PRIMARY KEY (SITE_ID) 
            WITH SYNONYMS=('site','location','installation') 
            COMMENT='TDF infrastructure sites including towers, rooftops, indoor',
        TOWERS AS INFRASTRUCTURE.TOWERS PRIMARY KEY (TOWER_ID) 
            WITH SYNONYMS=('tower','pylon','mast','pylône') 
            COMMENT='Tower structures - over 2,000 pylons across France',
        EQUIPMENT AS INFRASTRUCTURE.EQUIPMENT PRIMARY KEY (EQUIPMENT_ID) 
            WITH SYNONYMS=('equipment','asset','device','antenna') 
            COMMENT='Infrastructure equipment inventory',
        CLIENT_INSTALLATIONS AS INFRASTRUCTURE.CLIENT_INSTALLATIONS PRIMARY KEY (INSTALLATION_ID) 
            WITH SYNONYMS=('client','tenant','operator installation') 
            COMMENT='Client/operator installations on TDF infrastructure',
        DATA_QUALITY AS DIGITAL_TWIN.DATA_QUALITY_SCORES PRIMARY KEY (SCORE_ID) 
            WITH SYNONYMS=('data quality','DQ','quality score') 
            COMMENT='Data quality scores for Digital Twin validation',
        DISCREPANCIES AS DIGITAL_TWIN.DISCREPANCY_LOG PRIMARY KEY (DISCREPANCY_ID) 
            WITH SYNONYMS=('discrepancy','mismatch','error','issue') 
            COMMENT='Discrepancy log between field reality and database',
        OPERATORS AS CORE.OPERATORS PRIMARY KEY (OPERATOR_ID) 
            WITH SYNONYMS=('operator','MNO','client') 
            COMMENT='Mobile network operators and clients',
        REGIONS AS CORE.REGIONS PRIMARY KEY (REGION_ID)
    )
    RELATIONSHIPS (
        TOWERS_TO_SITES AS TOWERS(SITE_ID) REFERENCES SITES(SITE_ID),
        EQUIPMENT_TO_SITES AS EQUIPMENT(SITE_ID) REFERENCES SITES(SITE_ID),
        CLIENT_TO_SITES AS CLIENT_INSTALLATIONS(SITE_ID) REFERENCES SITES(SITE_ID),
        CLIENT_TO_OPERATORS AS CLIENT_INSTALLATIONS(OPERATOR_ID) REFERENCES OPERATORS(OPERATOR_ID)
    )
    FACTS (
        SITES.CURRENT_TENANTS AS CURRENT_TENANTS COMMENT='Current number of tenants on site',
        SITES.MAX_TENANTS AS MAX_TENANTS COMMENT='Maximum tenant capacity',
        SITES.COLOCATION_RATE AS COLOCATION_RATE COMMENT='Colocation rate (tenants per site)',
        SITES.ANNUAL_REVENUE_EUR AS SITE_REVENUE COMMENT='Annual revenue from site',
        SITES.RISK_SCORE AS SITE_RISK_SCORE COMMENT='Site risk score',
        TOWERS.HEIGHT_M AS TOWER_HEIGHT COMMENT='Tower height in meters',
        TOWERS.CURRENT_LOAD_KG AS TOWER_LOAD COMMENT='Current load on tower in kg',
        TOWERS.LOAD_CAPACITY_KG AS TOWER_CAPACITY COMMENT='Tower load capacity in kg',
        TOWERS.REPLACEMENT_COST_EUR AS TOWER_REPLACEMENT_COST COMMENT='Tower replacement cost',
        EQUIPMENT.REPLACEMENT_COST_EUR AS EQUIPMENT_REPLACEMENT_COST COMMENT='Equipment replacement cost',
        EQUIPMENT.AGE_YEARS AS EQUIPMENT_AGE COMMENT='Equipment age in years',
        CLIENT_INSTALLATIONS.ANNUAL_REVENUE_EUR AS CLIENT_REVENUE COMMENT='Annual revenue from client installation',
        DATA_QUALITY.OVERALL_SCORE AS DQ_OVERALL_SCORE COMMENT='Data quality overall score (0-100)',
        DATA_QUALITY.COMPLETENESS_SCORE AS DQ_COMPLETENESS COMMENT='Data completeness score',
        DATA_QUALITY.ACCURACY_SCORE AS DQ_ACCURACY COMMENT='Data accuracy score',
        DISCREPANCIES.DAYS_OPEN AS DISCREPANCY_DAYS_OPEN COMMENT='Days discrepancy has been open'
    )
    DIMENSIONS (
        SITES.SITE_TYPE AS SITE_TYPE WITH SYNONYMS=('type','category') COMMENT='Type of site (TOWER, ROOFTOP, INDOOR, DATACENTER)',
        SITES.STATUS AS SITE_STATUS WITH SYNONYMS=('site status','operational status') COMMENT='Site status (ACTIVE, PLANNED, etc.)',
        SITES.DIGITAL_TWIN_STATUS AS DT_STATUS WITH SYNONYMS=('digital twin status','sync status') COMMENT='Digital Twin sync status (SYNCED, DISCREPANCY)',
        TOWERS.TOWER_TYPE AS TOWER_TYPE COMMENT='Type of tower structure',
        TOWERS.STRUCTURAL_STATUS AS STRUCTURAL_STATUS COMMENT='Structural condition of tower',
        EQUIPMENT.EQUIPMENT_CATEGORY AS EQUIPMENT_CATEGORY COMMENT='Category of equipment',
        EQUIPMENT.LIFECYCLE_STATUS AS LIFECYCLE_STATUS COMMENT='Lifecycle status of equipment',
        DATA_QUALITY.ENTITY_TYPE AS DQ_ENTITY_TYPE COMMENT='Entity type being scored',
        DATA_QUALITY.SCORE_LABEL AS DQ_LABEL COMMENT='Quality label (EXCELLENT, GOOD, etc.)',
        DISCREPANCIES.STATUS AS DISCREPANCY_STATUS WITH SYNONYMS=('issue status') COMMENT='Discrepancy status (OPEN, RESOLVED, etc.)',
        DISCREPANCIES.SEVERITY AS DISCREPANCY_SEVERITY WITH SYNONYMS=('severity','criticality') COMMENT='Severity of discrepancy',
        DISCREPANCIES.DISCREPANCY_TYPE AS DISCREPANCY_TYPE COMMENT='Type of discrepancy',
        OPERATORS.OPERATOR_NAME AS OPERATOR_NAME WITH SYNONYMS=('client name','MNO name') COMMENT='Name of operator',
        REGIONS.REGION_NAME AS REGION_NAME
    )
    METRICS (
        SITES.SITE_COUNT AS COUNT(DISTINCT SITES.SITE_ID) COMMENT='Number of sites',
        TOWERS.TOWER_COUNT AS COUNT(DISTINCT TOWERS.TOWER_ID) COMMENT='Number of towers/pylons',
        SITES.TOTAL_SITE_REVENUE AS SUM(SITES.ANNUAL_REVENUE_EUR) COMMENT='Total site revenue',
        SITES.AVG_COLOCATION AS AVG(SITES.COLOCATION_RATE) COMMENT='Average colocation rate',
        EQUIPMENT.EQUIPMENT_COUNT AS COUNT(DISTINCT EQUIPMENT.EQUIPMENT_ID) COMMENT='Number of equipment items',
        EQUIPMENT.TOTAL_REPLACEMENT_COST AS SUM(EQUIPMENT.REPLACEMENT_COST_EUR) COMMENT='Total equipment replacement cost',
        DATA_QUALITY.AVG_DQ_SCORE AS AVG(DATA_QUALITY.OVERALL_SCORE) COMMENT='Average data quality score',
        DISCREPANCIES.OPEN_DISCREPANCY_COUNT AS COUNT(CASE WHEN DISCREPANCIES.STATUS = 'OPEN' THEN 1 END) COMMENT='Open discrepancies count'
    )
    COMMENT='Semantic view for Digital Twin & Infrastructure Data Mastery - 2,000+ pylons harmonized, discrepancy detection, data quality control';


-- ============================================================================
-- 6. SEMANTIC VIEW: CAPEX & LIFECYCLE MANAGEMENT (UC4)
-- ============================================================================
-- Addresses: 7-10 year equipment lifecycles, predictive renewal model, 
-- installation dates, life status tracking

CREATE OR REPLACE SEMANTIC VIEW TDF_DATA_PLATFORM.ANALYTICS.SV_CAPEX_LIFECYCLE
    TABLES (
        EQUIPMENT AS INFRASTRUCTURE.EQUIPMENT PRIMARY KEY (EQUIPMENT_ID) 
            WITH SYNONYMS=('equipment','asset','device') 
            COMMENT='Equipment inventory with installation dates and lifecycle status',
        EQUIPMENT_STATUS AS OPERATIONS.EQUIPMENT_STATUS PRIMARY KEY (STATUS_ID) 
            WITH SYNONYMS=('equipment status','condition','health') 
            COMMENT='Current equipment status and failure risk',
        RENEWAL_FORECAST AS FINANCE.RENEWAL_FORECAST PRIMARY KEY (FORECAST_ID) 
            WITH SYNONYMS=('renewal','replacement forecast','lifecycle forecast') 
            COMMENT='Predictive renewal model for equipment replacement',
        CAPEX_BUDGET AS FINANCE.CAPEX_BUDGET PRIMARY KEY (BUDGET_ID) 
            WITH SYNONYMS=('CAPEX budget','capital budget','investment budget') 
            COMMENT='CAPEX budget by category',
        CAPEX_ACTUALS AS FINANCE.CAPEX_ACTUALS PRIMARY KEY (ACTUAL_ID) 
            WITH SYNONYMS=('CAPEX actuals','capital spending','actual investment') 
            COMMENT='Actual CAPEX spending',
        SITES AS INFRASTRUCTURE.SITES PRIMARY KEY (SITE_ID),
        EQUIPMENT_TYPES AS CORE.EQUIPMENT_TYPES PRIMARY KEY (EQUIPMENT_TYPE_ID),
        BUSINESS_UNITS AS CORE.BUSINESS_UNITS PRIMARY KEY (BU_ID)
    )
    RELATIONSHIPS (
        EQUIPMENT_TO_SITES AS EQUIPMENT(SITE_ID) REFERENCES SITES(SITE_ID),
        EQUIPMENT_TO_TYPES AS EQUIPMENT(EQUIPMENT_TYPE_ID) REFERENCES EQUIPMENT_TYPES(EQUIPMENT_TYPE_ID),
        EQUIPMENT_STATUS_TO_EQ AS EQUIPMENT_STATUS(EQUIPMENT_ID) REFERENCES EQUIPMENT(EQUIPMENT_ID),
        RENEWAL_TO_EQUIPMENT AS RENEWAL_FORECAST(EQUIPMENT_ID) REFERENCES EQUIPMENT(EQUIPMENT_ID),
        CAPEX_BUDGET_TO_BU AS CAPEX_BUDGET(BU_ID) REFERENCES BUSINESS_UNITS(BU_ID),
        CAPEX_ACTUALS_TO_BU AS CAPEX_ACTUALS(BU_ID) REFERENCES BUSINESS_UNITS(BU_ID)
    )
    FACTS (
        EQUIPMENT.AGE_YEARS AS AGE_YEARS COMMENT='Equipment age in years (typical lifecycle 7-10 years)',
        EQUIPMENT.CONDITION_SCORE AS CONDITION_SCORE COMMENT='Equipment condition score (0-100)',
        EQUIPMENT.FAILURE_RISK_SCORE AS FAILURE_RISK COMMENT='Failure risk score',
        EQUIPMENT.REPLACEMENT_COST_EUR AS REPLACEMENT_COST COMMENT='Replacement cost in EUR',
        EQUIPMENT.ORIGINAL_COST_EUR AS ORIGINAL_COST COMMENT='Original purchase cost',
        EQUIPMENT.BOOK_VALUE_EUR AS BOOK_VALUE COMMENT='Current book value',
        EQUIPMENT_STATUS.FAILURE_PROBABILITY_30D AS FAILURE_PROB_30D COMMENT='30-day failure probability',
        RENEWAL_FORECAST.TOTAL_COST_EUR AS RENEWAL_COST COMMENT='Total renewal cost',
        RENEWAL_FORECAST.LABOR_COST_EUR AS LABOR_COST COMMENT='Labor cost for renewal',
        RENEWAL_FORECAST.EQUIPMENT_COST_EUR AS EQUIPMENT_COST COMMENT='Equipment cost for renewal',
        RENEWAL_FORECAST.CONFIDENCE_PCT AS RENEWAL_CONFIDENCE COMMENT='Renewal forecast confidence',
        CAPEX_BUDGET.BUDGET_EUR AS BUDGET_EUR COMMENT='CAPEX budget amount',
        CAPEX_ACTUALS.ACTUAL_EUR AS ACTUAL_EUR COMMENT='Actual CAPEX spent',
        CAPEX_ACTUALS.CUMULATIVE_YTD_EUR AS YTD_ACTUAL COMMENT='Year-to-date CAPEX actual'
    )
    DIMENSIONS (
        EQUIPMENT.INSTALLATION_DATE AS INSTALLATION_DATE WITH SYNONYMS=('installed date','purchase date') COMMENT='Date equipment was installed',
        EQUIPMENT.EXPECTED_END_OF_LIFE AS EOL_DATE WITH SYNONYMS=('end of life','EOL') COMMENT='Expected end of life date',
        EQUIPMENT.LIFECYCLE_STATUS AS LIFECYCLE_STATUS WITH SYNONYMS=('lifecycle','life status') COMMENT='Current lifecycle status (ACTIVE, AGING, END_OF_LIFE)',
        EQUIPMENT.EQUIPMENT_CATEGORY AS EQUIPMENT_CATEGORY COMMENT='Category of equipment',
        EQUIPMENT.IS_CRITICAL AS IS_CRITICAL COMMENT='Whether equipment is critical',
        EQUIPMENT_STATUS.RECOMMENDED_ACTION AS RECOMMENDED_ACTION WITH SYNONYMS=('action','recommendation') COMMENT='Recommended maintenance action',
        RENEWAL_FORECAST.REPLACEMENT_YEAR AS REPLACEMENT_YEAR WITH SYNONYMS=('forecast year','renewal year') COMMENT='Year equipment should be replaced',
        RENEWAL_FORECAST.REPLACEMENT_PRIORITY AS REPLACEMENT_PRIORITY COMMENT='Priority for replacement',
        RENEWAL_FORECAST.IN_CURRENT_BUDGET AS IN_BUDGET COMMENT='Whether renewal is in current budget',
        CAPEX_BUDGET.FISCAL_YEAR AS FISCAL_YEAR COMMENT='Fiscal year',
        CAPEX_BUDGET.CAPEX_CATEGORY AS CAPEX_CATEGORY WITH SYNONYMS=('category','investment category') COMMENT='CAPEX category (MAINTENANCE, GROWTH, etc.)',
        CAPEX_BUDGET.CAPEX_SUBCATEGORY AS CAPEX_SUBCATEGORY COMMENT='CAPEX subcategory',
        EQUIPMENT_TYPES.EQUIPMENT_TYPE_NAME AS EQUIPMENT_TYPE_NAME COMMENT='Type of equipment',
        EQUIPMENT_TYPES.EXPECTED_LIFESPAN_YEARS AS EXPECTED_LIFESPAN COMMENT='Expected lifespan in years (7-10 years typical)',
        BUSINESS_UNITS.BU_NAME AS BU_NAME,
        SITES.SITE_NAME AS SITE_NAME
    )
    METRICS (
        EQUIPMENT.EQUIPMENT_COUNT AS COUNT(DISTINCT EQUIPMENT.EQUIPMENT_ID) COMMENT='Total equipment count',
        EQUIPMENT.AVG_AGE AS AVG(EQUIPMENT.AGE_YEARS) COMMENT='Average equipment age in years',
        EQUIPMENT.TOTAL_REPLACEMENT_COST AS SUM(EQUIPMENT.REPLACEMENT_COST_EUR) COMMENT='Total replacement cost',
        EQUIPMENT.TOTAL_BOOK_VALUE AS SUM(EQUIPMENT.BOOK_VALUE_EUR) COMMENT='Total book value',
        EQUIPMENT_STATUS.AVG_CONDITION AS AVG(EQUIPMENT_STATUS.CONDITION_SCORE) COMMENT='Average condition score',
        EQUIPMENT_STATUS.AVG_FAILURE_RISK AS AVG(EQUIPMENT_STATUS.FAILURE_RISK_SCORE) COMMENT='Average failure risk',
        RENEWAL_FORECAST.TOTAL_RENEWAL_COST AS SUM(RENEWAL_FORECAST.TOTAL_COST_EUR) COMMENT='Total renewal cost',
        CAPEX_BUDGET.TOTAL_BUDGET AS SUM(CAPEX_BUDGET.BUDGET_EUR) COMMENT='Total CAPEX budget',
        CAPEX_ACTUALS.TOTAL_ACTUAL AS SUM(CAPEX_ACTUALS.ACTUAL_EUR) COMMENT='Total CAPEX actuals'
    )
    COMMENT='Semantic view for CAPEX & Lifecycle Management - 7-10 year equipment lifecycles, predictive renewal model, installation dates and life status tracking';


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

