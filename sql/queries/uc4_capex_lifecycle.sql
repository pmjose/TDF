-- ============================================================================
-- TDF DATA PLATFORM - USE CASE 4: CAPEX & EQUIPMENT LIFECYCLE
-- ============================================================================
-- Equipment lifecycle management and CAPEX planning
-- ============================================================================

USE DATABASE TDF_DATA_PLATFORM;
USE SCHEMA ANALYTICS;

-- ============================================================================
-- KPI 1: Equipment Lifecycle Summary
-- ============================================================================

SELECT * FROM VW_EQUIPMENT_LIFECYCLE
ORDER BY 
    CASE LIFECYCLE_STATUS WHEN 'END_OF_LIFE' THEN 1 WHEN 'AGING' THEN 2 WHEN 'OPERATIONAL' THEN 3 ELSE 4 END,
    AVG_RISK_SCORE DESC;

-- ============================================================================
-- KPI 2: CAPEX Budget vs Actual
-- ============================================================================

SELECT * FROM VW_CAPEX_BUDGET_VS_ACTUAL
WHERE FISCAL_YEAR = 2025
ORDER BY CAPEX_CATEGORY, CAPEX_SUBCATEGORY;

-- ============================================================================
-- KPI 3: Equipment at High Risk
-- ============================================================================

SELECT * FROM VW_EQUIPMENT_AT_RISK
ORDER BY FAILURE_RISK_SCORE DESC
LIMIT 50;

-- ============================================================================
-- KPI 4: Renewal Forecast by Year
-- ============================================================================

SELECT * FROM VW_RENEWAL_FORECAST_SUMMARY
ORDER BY REPLACEMENT_YEAR, 
    CASE REPLACEMENT_PRIORITY WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END;

-- ============================================================================
-- KPI 5: Equipment Age Distribution
-- ============================================================================

SELECT 
    CASE 
        WHEN AGE_YEARS < 3 THEN '0-2 years'
        WHEN AGE_YEARS < 6 THEN '3-5 years'
        WHEN AGE_YEARS < 10 THEN '6-9 years'
        WHEN AGE_YEARS < 15 THEN '10-14 years'
        ELSE '15+ years'
    END AS AGE_BAND,
    COUNT(*) AS EQUIPMENT_COUNT,
    AVG(CONDITION_SCORE) AS AVG_CONDITION_SCORE,
    AVG(FAILURE_RISK_SCORE) AS AVG_RISK_SCORE,
    SUM(REPLACEMENT_COST_EUR) / 1000000 AS REPLACEMENT_COST_EUR_M
FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.EQUIPMENT
GROUP BY AGE_BAND
ORDER BY 
    CASE AGE_BAND 
        WHEN '0-2 years' THEN 1 
        WHEN '3-5 years' THEN 2 
        WHEN '6-9 years' THEN 3 
        WHEN '10-14 years' THEN 4 
        ELSE 5 
    END;

-- ============================================================================
-- KPI 6: Maintenance Cost Trend
-- ============================================================================

SELECT 
    DATE_TRUNC('MONTH', mr.PERFORMED_DATE) AS MONTH,
    mr.MAINTENANCE_TYPE,
    COUNT(*) AS MAINTENANCE_COUNT,
    SUM(mr.TOTAL_COST_EUR) / 1000 AS TOTAL_COST_EUR_K,
    AVG(mr.DURATION_HOURS) AS AVG_DURATION_HOURS,
    SUM(mr.DOWNTIME_HOURS) AS TOTAL_DOWNTIME_HOURS
FROM TDF_DATA_PLATFORM.OPERATIONS.MAINTENANCE_RECORDS mr
WHERE mr.PERFORMED_DATE >= '2025-06-01'
GROUP BY MONTH, mr.MAINTENANCE_TYPE
ORDER BY MONTH, mr.MAINTENANCE_TYPE;

-- ============================================================================
-- KPI 7: Equipment Status Overview
-- ============================================================================

SELECT 
    OPERATIONAL_STATUS,
    CONDITION_LABEL,
    COUNT(*) AS EQUIPMENT_COUNT,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS PERCENTAGE,
    AVG(FAILURE_RISK_SCORE) AS AVG_RISK_SCORE,
    SUM(CASE WHEN MAINTENANCE_OVERDUE THEN 1 ELSE 0 END) AS OVERDUE_COUNT
FROM TDF_DATA_PLATFORM.OPERATIONS.EQUIPMENT_STATUS
GROUP BY OPERATIONAL_STATUS, CONDITION_LABEL
ORDER BY 
    CASE OPERATIONAL_STATUS WHEN 'FAILED' THEN 1 WHEN 'DEGRADED' THEN 2 WHEN 'MAINTENANCE' THEN 3 ELSE 4 END,
    CASE CONDITION_LABEL WHEN 'CRITICAL' THEN 1 WHEN 'POOR' THEN 2 WHEN 'FAIR' THEN 3 WHEN 'GOOD' THEN 4 ELSE 5 END;

-- ============================================================================
-- KPI 8: CAPEX Categories (Growth 61%, Maintenance 8%)
-- ============================================================================

SELECT 
    CAPEX_CATEGORY,
    SUM(BUDGET_EUR) / 1000000 AS BUDGET_EUR_M,
    SUM(BUDGET_EUR) / (SELECT SUM(BUDGET_EUR) FROM TDF_DATA_PLATFORM.FINANCE.CAPEX_BUDGET WHERE FISCAL_YEAR = 2025) * 100 AS PCT_OF_TOTAL
FROM TDF_DATA_PLATFORM.FINANCE.CAPEX_BUDGET
WHERE FISCAL_YEAR = 2025
GROUP BY CAPEX_CATEGORY
ORDER BY BUDGET_EUR_M DESC;

-- ============================================================================
-- KPI 9: Predicted Replacement Cost by Year
-- ============================================================================

SELECT 
    REPLACEMENT_YEAR,
    COUNT(*) AS EQUIPMENT_COUNT,
    SUM(TOTAL_COST_EUR) / 1000000 AS TOTAL_COST_EUR_M,
    SUM(CASE WHEN IN_CURRENT_BUDGET THEN TOTAL_COST_EUR ELSE 0 END) / 1000000 AS BUDGETED_EUR_M,
    SUM(CASE WHEN NOT IN_CURRENT_BUDGET THEN TOTAL_COST_EUR ELSE 0 END) / 1000000 AS UNBUDGETED_EUR_M,
    AVG(CONFIDENCE_PCT) AS AVG_CONFIDENCE
FROM TDF_DATA_PLATFORM.FINANCE.RENEWAL_FORECAST
WHERE REPLACEMENT_YEAR BETWEEN 2025 AND 2030
GROUP BY REPLACEMENT_YEAR
ORDER BY REPLACEMENT_YEAR;

-- ============================================================================
-- EXECUTIVE SUMMARY: CAPEX & Lifecycle Dashboard
-- ============================================================================

SELECT 
    'CAPEX & LIFECYCLE SUMMARY' AS METRIC_TYPE,
    (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.EQUIPMENT) AS TOTAL_EQUIPMENT,
    (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.EQUIPMENT WHERE LIFECYCLE_STATUS = 'END_OF_LIFE') AS END_OF_LIFE_COUNT,
    (SELECT AVG(FAILURE_RISK_SCORE) FROM TDF_DATA_PLATFORM.OPERATIONS.EQUIPMENT_STATUS) AS AVG_RISK_SCORE,
    (SELECT SUM(BUDGET_EUR) / 1000000 FROM TDF_DATA_PLATFORM.FINANCE.CAPEX_BUDGET WHERE FISCAL_YEAR = 2025) AS CAPEX_BUDGET_EUR_M,
    (SELECT SUM(TOTAL_COST_EUR) / 1000000 FROM TDF_DATA_PLATFORM.FINANCE.RENEWAL_FORECAST WHERE REPLACEMENT_YEAR = 2025) AS RENEWAL_FORECAST_EUR_M,
    (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.OPERATIONS.EQUIPMENT_STATUS WHERE RECOMMENDED_ACTION IN ('REPLACE_NOW', 'REPLACE_SOON')) AS EQUIPMENT_TO_REPLACE;

