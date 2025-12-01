-- ============================================================================
-- TDF DATA PLATFORM - EXECUTIVE DASHBOARD
-- ============================================================================
-- C-Level KPIs and strategic views
-- Based on TDF 2024: EUR 799.1M Revenue, 42-53% EBITDAaL, BBB- Rating
-- ============================================================================

USE DATABASE TDF_DATA_PLATFORM;
USE SCHEMA ANALYTICS;

-- ============================================================================
-- EXECUTIVE KPI SUMMARY (Single View)
-- ============================================================================

SELECT * FROM VW_EXECUTIVE_KPIS
ORDER BY PERIOD_DATE DESC
LIMIT 1;

-- ============================================================================
-- REVENUE OVERVIEW (EUR 799.1M Annual)
-- ============================================================================

SELECT * FROM VW_REVENUE_EXECUTIVE
WHERE FISCAL_YEAR = 2025
ORDER BY PERIOD_DATE DESC, REVENUE_EUR_M DESC;

-- ============================================================================
-- REVENUE BY CLIENT (Concentration Risk)
-- ============================================================================

SELECT * FROM VW_REVENUE_BY_CLIENT_EXECUTIVE
WHERE PERIOD_DATE = (SELECT MAX(PERIOD_DATE) FROM TDF_DATA_PLATFORM.FINANCE.REVENUE_BY_CLIENT)
ORDER BY REVENUE_EUR_M DESC;

-- ============================================================================
-- EBITDA PERFORMANCE (Target: 42-53%)
-- ============================================================================

SELECT * FROM VW_EBITDA_BY_BU
WHERE FISCAL_YEAR = 2025
ORDER BY PERIOD_DATE DESC;

-- ============================================================================
-- RISK DASHBOARD
-- ============================================================================

SELECT * FROM VW_RISK_DASHBOARD;

-- ============================================================================
-- MARKET SHARE BY REGION
-- ============================================================================

SELECT * FROM VW_MARKET_SHARE
ORDER BY TDF_SITES DESC;

-- ============================================================================
-- CLIENT CONCENTRATION
-- ============================================================================

SELECT * FROM VW_CLIENT_CONCENTRATION
ORDER BY ANNUAL_REVENUE_EUR DESC;

-- ============================================================================
-- COST PER TOWER ANALYSIS
-- ============================================================================

SELECT * FROM VW_COST_PER_TOWER
ORDER BY TOWER_COUNT DESC;

-- ============================================================================
-- REVENUE PER SITE
-- ============================================================================

SELECT * FROM VW_REVENUE_PER_SITE
ORDER BY AVG_REVENUE_PER_SITE DESC;

-- ============================================================================
-- INVESTMENT SCENARIOS (C-Level What-If)
-- ============================================================================

SELECT * FROM VW_INVESTMENT_SCENARIOS
ORDER BY NPV_EUR_M DESC;

-- ============================================================================
-- SALES PIPELINE
-- ============================================================================

SELECT 
    cp.OPPORTUNITY_NAME,
    o.OPERATOR_NAME,
    cp.OPPORTUNITY_TYPE,
    r.REGION_NAME,
    cp.SITE_COUNT,
    cp.ESTIMATED_VALUE_EUR / 1000000 AS VALUE_EUR_M,
    cp.PIPELINE_STAGE,
    cp.PROBABILITY_PCT,
    cp.WEIGHTED_VALUE_EUR / 1000000 AS WEIGHTED_VALUE_EUR_M,
    cp.EXPECTED_CLOSE_DATE
FROM TDF_DATA_PLATFORM.COMMERCIAL.CLIENT_PIPELINE cp
JOIN TDF_DATA_PLATFORM.CORE.OPERATORS o ON cp.OPERATOR_ID = o.OPERATOR_ID
LEFT JOIN TDF_DATA_PLATFORM.CORE.REGIONS r ON cp.REGION_ID = r.REGION_ID
ORDER BY cp.EXPECTED_CLOSE_DATE;

-- ============================================================================
-- COMPREHENSIVE EXECUTIVE SUMMARY
-- ============================================================================

SELECT 
    '=== TDF EXECUTIVE SUMMARY ===' AS SECTION,
    NULL AS METRIC,
    NULL AS VALUE,
    NULL AS STATUS

UNION ALL

SELECT 
    'FINANCIAL',
    'Annual Revenue (EUR M)',
    '799.1',
    'ON_TARGET'

UNION ALL

SELECT 
    'FINANCIAL',
    'EBITDAaL Margin (%)',
    (SELECT ROUND(AVG(EBITDAAL_MARGIN_PCT), 1)::VARCHAR FROM TDF_DATA_PLATFORM.FINANCE.EBITDA_METRICS WHERE FISCAL_YEAR = 2025),
    CASE WHEN (SELECT AVG(EBITDAAL_MARGIN_PCT) FROM TDF_DATA_PLATFORM.FINANCE.EBITDA_METRICS WHERE FISCAL_YEAR = 2025) >= 42 THEN 'GREEN' ELSE 'AMBER' END

UNION ALL

SELECT 
    'FINANCIAL',
    'Fitch Credit Rating',
    'BBB-',
    'STABLE'

UNION ALL

SELECT 
    'INFRASTRUCTURE',
    'Active Sites',
    (SELECT COUNT(*)::VARCHAR FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES WHERE STATUS = 'ACTIVE'),
    'GREEN'

UNION ALL

SELECT 
    'INFRASTRUCTURE',
    'Total Towers',
    (SELECT COUNT(*)::VARCHAR FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.TOWERS),
    'GREEN'

UNION ALL

SELECT 
    'INFRASTRUCTURE',
    'Colocation Rate',
    (SELECT ROUND(AVG(COLOCATION_RATE), 2)::VARCHAR FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES WHERE STATUS = 'ACTIVE'),
    CASE WHEN (SELECT AVG(COLOCATION_RATE) FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES WHERE STATUS = 'ACTIVE') >= 3.5 THEN 'GREEN' ELSE 'AMBER' END

UNION ALL

SELECT 
    'ESG',
    'Renewable Energy (%)',
    (SELECT ROUND(AVG(RENEWABLE_PCT), 1)::VARCHAR FROM TDF_DATA_PLATFORM.ENERGY.RENEWABLE_ENERGY WHERE YEAR_MONTH >= '2025-06-01'),
    CASE WHEN (SELECT AVG(RENEWABLE_PCT) FROM TDF_DATA_PLATFORM.ENERGY.RENEWABLE_ENERGY WHERE YEAR_MONTH >= '2025-06-01') >= 45 THEN 'GREEN' ELSE 'AMBER' END

UNION ALL

SELECT 
    'ESG',
    'Equality Index',
    (SELECT ROUND(AVG(EGALITE_INDEX_SCORE), 0)::VARCHAR FROM TDF_DATA_PLATFORM.HR.DIVERSITY_METRICS WHERE YEAR_MONTH >= '2025-06-01'),
    CASE WHEN (SELECT AVG(EGALITE_INDEX_SCORE) FROM TDF_DATA_PLATFORM.HR.DIVERSITY_METRICS WHERE YEAR_MONTH >= '2025-06-01') >= 85 THEN 'GREEN' ELSE 'AMBER' END

UNION ALL

SELECT 
    'RISK',
    'Avg Equipment Risk Score',
    (SELECT ROUND(AVG(FAILURE_RISK_SCORE), 0)::VARCHAR FROM TDF_DATA_PLATFORM.OPERATIONS.EQUIPMENT_STATUS),
    CASE WHEN (SELECT AVG(FAILURE_RISK_SCORE) FROM TDF_DATA_PLATFORM.OPERATIONS.EQUIPMENT_STATUS) <= 40 THEN 'GREEN' WHEN (SELECT AVG(FAILURE_RISK_SCORE) FROM TDF_DATA_PLATFORM.OPERATIONS.EQUIPMENT_STATUS) <= 60 THEN 'AMBER' ELSE 'RED' END

UNION ALL

SELECT 
    'RISK',
    'Open Discrepancies',
    (SELECT COUNT(*)::VARCHAR FROM TDF_DATA_PLATFORM.DIGITAL_TWIN.DISCREPANCY_LOG WHERE STATUS = 'OPEN'),
    CASE WHEN (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.DIGITAL_TWIN.DISCREPANCY_LOG WHERE STATUS = 'OPEN') <= 50 THEN 'GREEN' ELSE 'AMBER' END

UNION ALL

SELECT 
    'HR',
    'Total Employees',
    (SELECT COUNT(*)::VARCHAR FROM TDF_DATA_PLATFORM.HR.EMPLOYEES WHERE EMPLOYMENT_STATUS = 'ACTIVE'),
    'GREEN';

