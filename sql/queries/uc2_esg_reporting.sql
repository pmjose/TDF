-- ============================================================================
-- TDF DATA PLATFORM - USE CASE 2: ESG REGULATORY REPORTING
-- ============================================================================
-- Audited reports with full data lineage
-- ============================================================================

USE DATABASE TDF_DATA_PLATFORM;
USE SCHEMA ANALYTICS;

-- ============================================================================
-- KPI 1: ESG Dashboard Summary
-- ============================================================================

SELECT * FROM VW_ESG_DASHBOARD
ORDER BY REPORTING_DATE DESC
LIMIT 7;

-- ============================================================================
-- KPI 2: Carbon Emissions by Scope (GHG Protocol)
-- ============================================================================

SELECT 
    YEAR_MONTH,
    EMISSION_SCOPE,
    SUM(EMISSIONS_TONNES_CO2E) AS TOTAL_TONNES_CO2E,
    AVG(CARBON_INTENSITY_KG_EUR) AS AVG_INTENSITY,
    SUM(TARGET_EMISSIONS_KG) / 1000 AS TARGET_TONNES,
    SUM(VARIANCE_TO_TARGET_KG) / 1000 AS VARIANCE_TONNES,
    CASE 
        WHEN SUM(VARIANCE_TO_TARGET_KG) <= 0 THEN 'ON_TARGET'
        ELSE 'ABOVE_TARGET'
    END AS STATUS
FROM TDF_DATA_PLATFORM.ENERGY.CARBON_EMISSIONS
WHERE YEAR_MONTH >= '2025-06-01'
GROUP BY YEAR_MONTH, EMISSION_SCOPE
ORDER BY YEAR_MONTH, EMISSION_SCOPE;

-- ============================================================================
-- KPI 3: Renewable Energy Progress
-- ============================================================================

SELECT 
    YEAR_MONTH,
    SUM(TOTAL_CONSUMPTION_KWH) / 1000000 AS TOTAL_CONSUMPTION_MWH,
    SUM(RENEWABLE_CONSUMPTION_KWH) / 1000000 AS RENEWABLE_MWH,
    AVG(RENEWABLE_PCT) AS RENEWABLE_PCT,
    50 AS TARGET_PCT,
    AVG(RENEWABLE_PCT) - 50 AS VARIANCE_TO_TARGET,
    CASE WHEN AVG(RENEWABLE_PCT) >= 50 THEN 'GREEN' ELSE 'AMBER' END AS STATUS
FROM TDF_DATA_PLATFORM.ENERGY.RENEWABLE_ENERGY
WHERE YEAR_MONTH >= '2025-06-01'
GROUP BY YEAR_MONTH
ORDER BY YEAR_MONTH;

-- ============================================================================
-- KPI 4: Gender Equality Index (H/F)
-- ============================================================================

SELECT 
    YEAR_MONTH,
    SUM(TOTAL_HEADCOUNT) AS TOTAL_EMPLOYEES,
    AVG(FEMALE_PERCENTAGE) AS FEMALE_PCT,
    AVG(MANAGEMENT_FEMALE_PCT) AS MGMT_FEMALE_PCT,
    AVG(PAY_EQUITY_INDEX) AS PAY_EQUITY_INDEX,
    AVG(EGALITE_INDEX_SCORE) AS EQUALITY_INDEX,
    85 AS TARGET_INDEX,
    AVG(EGALITE_INDEX_SCORE) - 85 AS VARIANCE,
    CASE WHEN AVG(EGALITE_INDEX_SCORE) >= 85 THEN 'COMPLIANT' ELSE 'ACTION_NEEDED' END AS STATUS
FROM TDF_DATA_PLATFORM.HR.DIVERSITY_METRICS
WHERE YEAR_MONTH >= '2025-06-01'
GROUP BY YEAR_MONTH
ORDER BY YEAR_MONTH;

-- ============================================================================
-- KPI 5: ESG Audit Trail (Data Lineage for Auditors)
-- ============================================================================

SELECT 
    rr.REPORT_NAME,
    rr.REPORTING_YEAR,
    rr.STATUS AS REPORT_STATUS,
    at.METRIC_NAME,
    at.METRIC_VALUE,
    at.METRIC_UNIT,
    at.SOURCE_SCHEMA || '.' || at.SOURCE_TABLE AS DATA_SOURCE,
    at.AGGREGATION_METHOD,
    at.RECORD_COUNT,
    at.DATA_QUALITY_SCORE,
    at.IS_VERIFIED,
    at.VERIFIED_BY,
    at.VERIFICATION_DATE
FROM TDF_DATA_PLATFORM.ESG.REGULATORY_REPORTS rr
JOIN TDF_DATA_PLATFORM.ESG.AUDIT_TRAIL at ON rr.REPORT_ID = at.REPORT_ID
ORDER BY rr.REPORTING_YEAR DESC, at.METRIC_NAME;

-- ============================================================================
-- KPI 6: Compliance Status
-- ============================================================================

SELECT 
    REGULATION_NAME,
    REGULATION_TYPE,
    COMPLIANCE_DEADLINE,
    COMPLIANCE_STATUS,
    LAST_ASSESSMENT_DATE,
    NEXT_ASSESSMENT_DATE,
    NON_COMPLIANCE_RISK,
    COMPLIANCE_OWNER,
    CASE COMPLIANCE_STATUS 
        WHEN 'COMPLIANT' THEN 'GREEN'
        WHEN 'PARTIAL' THEN 'AMBER'
        ELSE 'RED'
    END AS STATUS_COLOR
FROM TDF_DATA_PLATFORM.ESG.COMPLIANCE_REQUIREMENTS
ORDER BY 
    CASE NON_COMPLIANCE_RISK WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
    COMPLIANCE_DEADLINE;

-- ============================================================================
-- KPI 7: Board ESG Scorecard
-- ============================================================================

SELECT 
    REPORTING_DATE,
    -- Environmental
    CARBON_EMISSIONS_TONNES,
    CARBON_INTENSITY_KG_EUR,
    RENEWABLE_ENERGY_PCT,
    ENVIRONMENTAL_STATUS,
    -- Social
    TOTAL_EMPLOYEES,
    FEMALE_EMPLOYEES_PCT,
    EQUALITY_INDEX_SCORE,
    SOCIAL_STATUS,
    -- Governance
    BOARD_INDEPENDENCE_PCT,
    GOVERNANCE_STATUS,
    -- Overall
    OVERALL_ESG_STATUS,
    FITCH_CREDIT_RATING
FROM TDF_DATA_PLATFORM.ESG.BOARD_SCORECARD
ORDER BY REPORTING_DATE DESC;

-- ============================================================================
-- EXECUTIVE SUMMARY: ESG Compliance Dashboard
-- ============================================================================

SELECT 
    'ESG COMPLIANCE SUMMARY' AS METRIC_TYPE,
    (SELECT SUM(EMISSIONS_TONNES_CO2E) FROM TDF_DATA_PLATFORM.ENERGY.CARBON_EMISSIONS WHERE FISCAL_YEAR = 2025) AS YTD_CARBON_TONNES,
    (SELECT AVG(RENEWABLE_PCT) FROM TDF_DATA_PLATFORM.ENERGY.RENEWABLE_ENERGY WHERE YEAR_MONTH >= '2025-06-01') AS AVG_RENEWABLE_PCT,
    (SELECT AVG(EGALITE_INDEX_SCORE) FROM TDF_DATA_PLATFORM.HR.DIVERSITY_METRICS WHERE YEAR_MONTH >= '2025-06-01') AS AVG_EQUALITY_INDEX,
    (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.ESG.COMPLIANCE_REQUIREMENTS WHERE COMPLIANCE_STATUS = 'COMPLIANT') AS COMPLIANT_REGS,
    (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.ESG.COMPLIANCE_REQUIREMENTS) AS TOTAL_REGS,
    'BBB-' AS FITCH_RATING;

