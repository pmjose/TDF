-- ============================================================================
-- TDF DATA PLATFORM - USE CASE 3: INFRASTRUCTURE & DIGITAL TWIN
-- ============================================================================
-- Infrastructure data mastery and Digital Twin quality
-- ============================================================================

USE DATABASE TDF_DATA_PLATFORM;
USE SCHEMA ANALYTICS;

-- ============================================================================
-- KPI 1: Infrastructure Overview (8,785 Sites, 7,877 Towers)
-- ============================================================================

SELECT 
    SITE_TYPE,
    STATUS,
    COUNT(*) AS SITE_COUNT,
    AVG(COLOCATION_RATE) AS AVG_COLOCATION_RATE,
    AVG(RISK_SCORE) AS AVG_RISK_SCORE,
    SUM(ANNUAL_REVENUE_EUR) / 1000000 AS TOTAL_REVENUE_EUR_M
FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES
GROUP BY SITE_TYPE, STATUS
ORDER BY SITE_COUNT DESC;

-- ============================================================================
-- KPI 2: Colocation Analysis (TDF Benchmark: 3.9x overall, 1.8x MNO)
-- ============================================================================

SELECT * FROM VW_COLOCATION_ANALYSIS
ORDER BY COLOCATION_RATE DESC;

-- ============================================================================
-- KPI 3: Digital Twin Quality Score
-- ============================================================================

SELECT 
    SCORE_DATE,
    ENTITY_TYPE,
    OVERALL_SCORE,
    COMPLETENESS_SCORE,
    ACCURACY_SCORE,
    CONSISTENCY_SCORE,
    SCORE_LABEL,
    MEETS_TARGET,
    OPEN_DISCREPANCIES,
    CRITICAL_ISSUES
FROM TDF_DATA_PLATFORM.DIGITAL_TWIN.DATA_QUALITY_SCORES
WHERE ENTITY_TYPE = 'COMPANY'
ORDER BY SCORE_DATE DESC;

-- ============================================================================
-- KPI 4: Digital Twin Sync Status
-- ============================================================================

SELECT 
    DIGITAL_TWIN_STATUS,
    COUNT(*) AS SITE_COUNT,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS PERCENTAGE,
    AVG(RISK_SCORE) AS AVG_RISK_SCORE
FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES
WHERE STATUS = 'ACTIVE'
GROUP BY DIGITAL_TWIN_STATUS
ORDER BY SITE_COUNT DESC;

-- ============================================================================
-- KPI 5: Discrepancy Summary by Severity
-- ============================================================================

SELECT * FROM VW_DISCREPANCY_SUMMARY
ORDER BY 
    CASE SEVERITY WHEN 'CRITICAL' THEN 1 WHEN 'HIGH' THEN 2 WHEN 'MEDIUM' THEN 3 ELSE 4 END,
    DISCREPANCY_COUNT DESC;

-- ============================================================================
-- KPI 6: Infrastructure by Region
-- ============================================================================

SELECT 
    r.REGION_NAME,
    COUNT(DISTINCT s.SITE_ID) AS SITE_COUNT,
    COUNT(DISTINCT t.TOWER_ID) AS TOWER_COUNT,
    SUM(s.CURRENT_TENANTS) AS TOTAL_TENANTS,
    AVG(s.COLOCATION_RATE) AS AVG_COLOCATION,
    SUM(s.ANNUAL_REVENUE_EUR) / 1000000 AS REVENUE_EUR_M,
    AVG(s.RISK_SCORE) AS AVG_RISK_SCORE
FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES s
LEFT JOIN TDF_DATA_PLATFORM.INFRASTRUCTURE.TOWERS t ON s.SITE_ID = t.SITE_ID
LEFT JOIN TDF_DATA_PLATFORM.CORE.DEPARTMENTS d ON s.DEPARTMENT_ID = d.DEPARTMENT_ID
LEFT JOIN TDF_DATA_PLATFORM.CORE.REGIONS r ON d.REGION_ID = r.REGION_ID
WHERE s.STATUS = 'ACTIVE'
GROUP BY r.REGION_NAME
ORDER BY SITE_COUNT DESC;

-- ============================================================================
-- KPI 7: Client Installations by Operator
-- ============================================================================

SELECT 
    o.OPERATOR_NAME,
    COUNT(DISTINCT ci.INSTALLATION_ID) AS INSTALLATION_COUNT,
    COUNT(DISTINCT ci.SITE_ID) AS SITES_USED,
    SUM(ci.ANNUAL_REVENUE_EUR) / 1000000 AS ANNUAL_REVENUE_EUR_M,
    AVG(ci.EQUIPMENT_COUNT) AS AVG_EQUIPMENT_PER_SITE
FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.CLIENT_INSTALLATIONS ci
JOIN TDF_DATA_PLATFORM.CORE.OPERATORS o ON ci.OPERATOR_ID = o.OPERATOR_ID
WHERE ci.STATUS = 'ACTIVE'
GROUP BY o.OPERATOR_NAME
ORDER BY ANNUAL_REVENUE_EUR_M DESC;

-- ============================================================================
-- KPI 8: Tower Lifecycle Status
-- ============================================================================

SELECT 
    t.LIFECYCLE_STATUS,
    t.STRUCTURAL_CONDITION,
    COUNT(*) AS TOWER_COUNT,
    AVG(t.HEIGHT_M) AS AVG_HEIGHT_M,
    AVG(DATEDIFF(YEAR, t.INSTALLATION_DATE, CURRENT_DATE())) AS AVG_AGE_YEARS,
    SUM(t.REPLACEMENT_COST_EUR) / 1000000 AS TOTAL_REPLACEMENT_COST_EUR_M
FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.TOWERS t
GROUP BY t.LIFECYCLE_STATUS, t.STRUCTURAL_CONDITION
ORDER BY 
    CASE t.LIFECYCLE_STATUS WHEN 'END_OF_LIFE' THEN 1 WHEN 'AGING' THEN 2 WHEN 'OPERATIONAL' THEN 3 ELSE 4 END;

-- ============================================================================
-- EXECUTIVE SUMMARY: Infrastructure Health Dashboard
-- ============================================================================

SELECT 
    'INFRASTRUCTURE SUMMARY' AS METRIC_TYPE,
    (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES WHERE STATUS = 'ACTIVE') AS ACTIVE_SITES,
    (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.TOWERS) AS TOTAL_TOWERS,
    (SELECT AVG(COLOCATION_RATE) FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES WHERE STATUS = 'ACTIVE') AS AVG_COLOCATION_RATE,
    (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES WHERE DIGITAL_TWIN_STATUS = 'SYNCED') AS DT_SYNCED_SITES,
    (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.DIGITAL_TWIN.DISCREPANCY_LOG WHERE STATUS = 'OPEN') AS OPEN_DISCREPANCIES,
    (SELECT AVG(OVERALL_SCORE) FROM TDF_DATA_PLATFORM.DIGITAL_TWIN.DATA_QUALITY_SCORES WHERE ENTITY_TYPE = 'COMPANY') AS DT_QUALITY_SCORE;

