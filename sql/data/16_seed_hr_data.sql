-- ============================================================================
-- TDF DATA PLATFORM - SEED DATA: HR
-- ============================================================================
-- ~1,500 Employees, Skills, Workforce Capacity, Diversity Metrics
-- ============================================================================

USE DATABASE TDF_DATA_PLATFORM;
USE SCHEMA HR;

-- ============================================================================
-- SKILLS
-- ============================================================================

INSERT INTO SKILLS (SKILL_ID, SKILL_CODE, SKILL_NAME, SKILL_CATEGORY_ID, SKILL_TYPE, IS_CRITICAL, DESCRIPTION, MARKET_DEMAND, INTERNAL_DEMAND)
VALUES
    -- RF Engineering
    ('SKL-RF01', 'RF-DESIGN', 'RF Network Design', 'SC-RF', 'TECHNICAL', TRUE, 'Design of radio frequency networks', 'HIGH', 'HIGH'),
    ('SKL-RF02', 'RF-OPTIM', 'RF Optimization', 'SC-RF', 'TECHNICAL', TRUE, 'Network optimization and troubleshooting', 'HIGH', 'HIGH'),
    ('SKL-RF03', 'ANT-INSTALL', 'Antenna Installation', 'SC-RF', 'TECHNICAL', FALSE, 'Antenna mounting and alignment', 'MEDIUM', 'HIGH'),
    
    -- Tower Work
    ('SKL-TW01', 'TOWER-CLIMB', 'Tower Climbing', 'SC-TOWER', 'CERTIFICATION', TRUE, 'Certified tower climber', 'HIGH', 'HIGH'),
    ('SKL-TW02', 'RIGGING', 'Rigging & Hoisting', 'SC-TOWER', 'CERTIFICATION', TRUE, 'Equipment rigging on towers', 'HIGH', 'HIGH'),
    ('SKL-TW03', 'RESCUE', 'Tower Rescue', 'SC-TOWER', 'CERTIFICATION', TRUE, 'Emergency rescue certification', 'HIGH', 'HIGH'),
    
    -- Electrical
    ('SKL-EL01', 'HV-ELECT', 'High Voltage Electrical', 'SC-ELEC', 'CERTIFICATION', TRUE, 'High voltage systems certification', 'HIGH', 'HIGH'),
    ('SKL-EL02', 'UPS-MAINT', 'UPS Maintenance', 'SC-ELEC', 'TECHNICAL', FALSE, 'UPS system maintenance', 'MEDIUM', 'HIGH'),
    ('SKL-EL03', 'GENERATOR', 'Generator Systems', 'SC-ELEC', 'TECHNICAL', FALSE, 'Diesel generator maintenance', 'MEDIUM', 'MEDIUM'),
    
    -- Broadcast
    ('SKL-TX01', 'DTT-TX', 'DTT Transmission', 'SC-TX', 'TECHNICAL', TRUE, 'Digital TV transmitter operation', 'LOW', 'HIGH'),
    ('SKL-TX02', 'FM-TX', 'FM Radio Transmission', 'SC-TX', 'TECHNICAL', TRUE, 'FM transmitter maintenance', 'LOW', 'HIGH'),
    ('SKL-TX03', 'DAB-TX', 'DAB+ Transmission', 'SC-TX', 'TECHNICAL', FALSE, 'DAB+ digital radio systems', 'MEDIUM', 'MEDIUM'),
    
    -- Networking
    ('SKL-NT01', 'IP-NET', 'IP Networking', 'SC-NET', 'TECHNICAL', FALSE, 'TCP/IP, routing, switching', 'HIGH', 'MEDIUM'),
    ('SKL-NT02', 'FIBER', 'Fiber Optics', 'SC-NET', 'TECHNICAL', FALSE, 'Fiber installation and testing', 'HIGH', 'MEDIUM'),
    ('SKL-NT03', 'MICROWAVE', 'Microwave Links', 'SC-NET', 'TECHNICAL', FALSE, 'Point-to-point microwave systems', 'MEDIUM', 'MEDIUM'),
    
    -- Project Management
    ('SKL-PM01', 'PMP', 'PMP Certification', 'SC-PM', 'CERTIFICATION', FALSE, 'Project Management Professional', 'HIGH', 'MEDIUM'),
    ('SKL-PM02', 'AGILE', 'Agile/Scrum', 'SC-PM', 'CERTIFICATION', FALSE, 'Agile methodology', 'HIGH', 'MEDIUM'),
    
    -- Safety
    ('SKL-SF01', 'HSE', 'Health & Safety', 'SC-SAFETY', 'CERTIFICATION', TRUE, 'HSE certification', 'HIGH', 'HIGH'),
    ('SKL-SF02', 'FIRST-AID', 'First Aid', 'SC-SAFETY', 'CERTIFICATION', FALSE, 'First aid certification', 'MEDIUM', 'HIGH'),
    
    -- Environmental
    ('SKL-EN01', 'ESG-REP', 'ESG Reporting', 'SC-ENV', 'TECHNICAL', FALSE, 'ESG metrics and reporting', 'HIGH', 'HIGH'),
    ('SKL-EN02', 'CARBON', 'Carbon Accounting', 'SC-ENV', 'TECHNICAL', FALSE, 'Carbon footprint calculation', 'HIGH', 'HIGH');

-- ============================================================================
-- EMPLOYEES (~1,500)
-- ============================================================================

INSERT INTO EMPLOYEES (
    EMPLOYEE_ID, EMPLOYEE_CODE, FIRST_NAME, LAST_NAME, EMAIL, GENDER,
    HIRE_DATE, EMPLOYMENT_TYPE, EMPLOYMENT_STATUS,
    JOB_TITLE, JOB_LEVEL, BU_ID, DEPARTMENT, COST_CENTER_ID,
    WORK_LOCATION, REGION_ID, IS_FIELD_WORKER, SALARY_BAND, FTE_PERCENTAGE
)
SELECT 
    'EMP-' || LPAD(ROW_NUMBER() OVER (), 5, '0') AS EMPLOYEE_ID,
    'TDF' || LPAD(ROW_NUMBER() OVER (), 5, '0') AS EMPLOYEE_CODE,
    CASE MOD(ROW_NUMBER() OVER (), 20)
        WHEN 0 THEN 'Jean' WHEN 1 THEN 'Marie' WHEN 2 THEN 'Pierre' WHEN 3 THEN 'Sophie'
        WHEN 4 THEN 'Michel' WHEN 5 THEN 'Isabelle' WHEN 6 THEN 'Philippe' WHEN 7 THEN 'Catherine'
        WHEN 8 THEN 'Nicolas' WHEN 9 THEN 'Nathalie' WHEN 10 THEN 'François' WHEN 11 THEN 'Sandrine'
        WHEN 12 THEN 'Laurent' WHEN 13 THEN 'Valérie' WHEN 14 THEN 'Christophe' WHEN 15 THEN 'Stéphanie'
        WHEN 16 THEN 'David' WHEN 17 THEN 'Céline' WHEN 18 THEN 'Julien' ELSE 'Aurélie'
    END AS FIRST_NAME,
    CASE MOD(ROW_NUMBER() OVER (), 25)
        WHEN 0 THEN 'Martin' WHEN 1 THEN 'Bernard' WHEN 2 THEN 'Dubois' WHEN 3 THEN 'Thomas'
        WHEN 4 THEN 'Robert' WHEN 5 THEN 'Richard' WHEN 6 THEN 'Petit' WHEN 7 THEN 'Durand'
        WHEN 8 THEN 'Leroy' WHEN 9 THEN 'Moreau' WHEN 10 THEN 'Simon' WHEN 11 THEN 'Laurent'
        WHEN 12 THEN 'Lefebvre' WHEN 13 THEN 'Michel' WHEN 14 THEN 'Garcia' WHEN 15 THEN 'David'
        WHEN 16 THEN 'Bertrand' WHEN 17 THEN 'Roux' WHEN 18 THEN 'Vincent' WHEN 19 THEN 'Fournier'
        WHEN 20 THEN 'Morel' WHEN 21 THEN 'Girard' WHEN 22 THEN 'André' WHEN 23 THEN 'Mercier'
        ELSE 'Dupont'
    END AS LAST_NAME,
    'employee' || ROW_NUMBER() OVER () || '@tdf.fr' AS EMAIL,
    CASE WHEN UNIFORM(0, 100, RANDOM()) < 55 THEN 'M' ELSE 'F' END AS GENDER,  -- ~45% female
    DATEADD(YEAR, -UNIFORM(1, 25, RANDOM()), CURRENT_DATE()) AS HIRE_DATE,
    CASE WHEN UNIFORM(0, 100, RANDOM()) < 95 THEN 'PERMANENT' ELSE 'CONTRACT' END AS EMPLOYMENT_TYPE,
    'ACTIVE' AS EMPLOYMENT_STATUS,
    CASE MOD(ROW_NUMBER() OVER (), 15)
        WHEN 0 THEN 'RF Engineer' WHEN 1 THEN 'Tower Technician' WHEN 2 THEN 'Electrical Technician'
        WHEN 3 THEN 'Broadcast Engineer' WHEN 4 THEN 'Network Engineer' WHEN 5 THEN 'Project Manager'
        WHEN 6 THEN 'Site Manager' WHEN 7 THEN 'Field Supervisor' WHEN 8 THEN 'NOC Operator'
        WHEN 9 THEN 'Data Analyst' WHEN 10 THEN 'HSE Coordinator' WHEN 11 THEN 'Civil Engineer'
        WHEN 12 THEN 'IT Specialist' WHEN 13 THEN 'Finance Analyst' ELSE 'Operations Manager'
    END AS JOB_TITLE,
    CASE 
        WHEN UNIFORM(0, 100, RANDOM()) < 40 THEN 'MID'
        WHEN UNIFORM(0, 100, RANDOM()) < 70 THEN 'SENIOR'
        WHEN UNIFORM(0, 100, RANDOM()) < 90 THEN 'LEAD'
        WHEN UNIFORM(0, 100, RANDOM()) < 97 THEN 'MANAGER'
        ELSE 'DIRECTOR'
    END AS JOB_LEVEL,
    CASE MOD(ROW_NUMBER() OVER (), 13)
        WHEN 0 THEN 'BU-HOST' WHEN 1 THEN 'BU-HOST' WHEN 2 THEN 'BU-HOST' WHEN 3 THEN 'BU-HOST'
        WHEN 4 THEN 'BU-DTT' WHEN 5 THEN 'BU-RADIO' WHEN 6 THEN 'BU-NOC'
        WHEN 7 THEN 'BU-ENG' WHEN 8 THEN 'BU-EDC' WHEN 9 THEN 'BU-IND'
        WHEN 10 THEN 'BU-CORP' WHEN 11 THEN 'BU-OTHR' ELSE 'BU-PMN'
    END AS BU_ID,
    'Operations' AS DEPARTMENT,
    'CC-HOST' AS COST_CENTER_ID,
    CASE MOD(ROW_NUMBER() OVER (), 13)
        WHEN 0 THEN 'Paris (Romainville)' WHEN 1 THEN 'Lyon' WHEN 2 THEN 'Toulouse'
        WHEN 3 THEN 'Marseille' WHEN 4 THEN 'Bordeaux' WHEN 5 THEN 'Lille'
        WHEN 6 THEN 'Nantes' WHEN 7 THEN 'Strasbourg' WHEN 8 THEN 'Rennes'
        WHEN 9 THEN 'Nice' WHEN 10 THEN 'Montpellier' WHEN 11 THEN 'Grenoble'
        ELSE 'Paris (Romainville)'
    END AS WORK_LOCATION,
    CASE MOD(ROW_NUMBER() OVER (), 13)
        WHEN 0 THEN 'REG-IDF' WHEN 1 THEN 'REG-ARA' WHEN 2 THEN 'REG-OCC'
        WHEN 3 THEN 'REG-PAC' WHEN 4 THEN 'REG-NAQ' WHEN 5 THEN 'REG-HDF'
        WHEN 6 THEN 'REG-PDL' WHEN 7 THEN 'REG-GES' WHEN 8 THEN 'REG-BRE'
        WHEN 9 THEN 'REG-PAC' WHEN 10 THEN 'REG-OCC' WHEN 11 THEN 'REG-ARA'
        ELSE 'REG-IDF'
    END AS REGION_ID,
    CASE WHEN MOD(ROW_NUMBER() OVER (), 3) < 2 THEN TRUE ELSE FALSE END AS IS_FIELD_WORKER,
    'BAND_' || (MOD(ROW_NUMBER() OVER (), 8) + 2) AS SALARY_BAND,
    1.0 AS FTE_PERCENTAGE
FROM TABLE(GENERATOR(ROWCOUNT => 1500));

-- ============================================================================
-- SKILLS_MATRIX (Employee-Skill mapping)
-- ============================================================================

INSERT INTO SKILLS_MATRIX (
    SKILL_MATRIX_ID, EMPLOYEE_ID, SKILL_ID,
    PROFICIENCY_LEVEL, PROFICIENCY_LABEL, IS_CERTIFIED, LAST_ASSESSED_DATE
)
SELECT 
    'SM-' || LPAD(ROW_NUMBER() OVER (), 7, '0') AS SKILL_MATRIX_ID,
    e.EMPLOYEE_ID,
    s.SKILL_ID,
    UNIFORM(2, 5, RANDOM()) AS PROFICIENCY_LEVEL,
    CASE UNIFORM(2, 5, RANDOM())
        WHEN 2 THEN 'BASIC'
        WHEN 3 THEN 'INTERMEDIATE'
        WHEN 4 THEN 'ADVANCED'
        ELSE 'EXPERT'
    END AS PROFICIENCY_LABEL,
    CASE WHEN UNIFORM(0, 100, RANDOM()) < 60 THEN TRUE ELSE FALSE END AS IS_CERTIFIED,
    DATEADD(MONTH, -UNIFORM(1, 24, RANDOM()), CURRENT_DATE()) AS LAST_ASSESSED_DATE
FROM EMPLOYEES e
CROSS JOIN SKILLS s
WHERE UNIFORM(0, 100, RANDOM()) < 30  -- Each employee has ~30% of skills
LIMIT 8000;

-- ============================================================================
-- WORKFORCE_CAPACITY (Monthly capacity - June to December 2025)
-- ============================================================================

INSERT INTO WORKFORCE_CAPACITY (
    CAPACITY_ID, YEAR_MONTH, BU_ID, REGION_ID, SKILL_CATEGORY_ID, JOB_FAMILY,
    HEADCOUNT, FTE_AVAILABLE, FTE_ALLOCATED, TOTAL_HOURS_AVAILABLE, HOURS_PLANNED,
    UTILIZATION_PCT, IS_FORECAST, FORECAST_CONFIDENCE
)
SELECT 
    'CAP-' || LPAD(ROW_NUMBER() OVER (), 6, '0') AS CAPACITY_ID,
    cal.DATE_KEY AS YEAR_MONTH,
    bu.BU_ID,
    r.REGION_ID,
    sc.SKILL_CATEGORY_ID,
    'Operations' AS JOB_FAMILY,
    UNIFORM(10, 80, RANDOM()) AS HEADCOUNT,
    UNIFORM(8, 75, RANDOM()) AS FTE_AVAILABLE,
    UNIFORM(5, 70, RANDOM()) AS FTE_ALLOCATED,
    UNIFORM(1200, 12000, RANDOM()) AS TOTAL_HOURS_AVAILABLE,
    UNIFORM(1000, 11000, RANDOM()) AS HOURS_PLANNED,
    UNIFORM(70, 95, RANDOM()) AS UTILIZATION_PCT,
    CASE WHEN cal.DATE_KEY > CURRENT_DATE() THEN TRUE ELSE FALSE END AS IS_FORECAST,
    CASE WHEN cal.DATE_KEY > CURRENT_DATE() THEN 'MEDIUM' ELSE 'HIGH' END AS FORECAST_CONFIDENCE
FROM (SELECT DISTINCT DATE_TRUNC('MONTH', DATE_KEY) AS DATE_KEY FROM TDF_DATA_PLATFORM.CORE.CALENDAR WHERE DATE_KEY BETWEEN '2025-06-01' AND '2025-12-31') cal
CROSS JOIN (SELECT * FROM TDF_DATA_PLATFORM.CORE.BUSINESS_UNITS WHERE BU_TYPE != 'CORPORATE' LIMIT 5) bu
CROSS JOIN (SELECT * FROM TDF_DATA_PLATFORM.CORE.REGIONS LIMIT 5) r
CROSS JOIN (SELECT * FROM TDF_DATA_PLATFORM.CORE.SKILL_CATEGORIES LIMIT 3) sc;

-- ============================================================================
-- DIVERSITY_METRICS (ESG H/F Equality - UC2)
-- ============================================================================

INSERT INTO DIVERSITY_METRICS (
    METRIC_ID, YEAR_MONTH, REPORT_TYPE, BU_ID, JOB_LEVEL,
    TOTAL_HEADCOUNT, MALE_COUNT, FEMALE_COUNT, FEMALE_PERCENTAGE,
    MANAGEMENT_TOTAL, MANAGEMENT_FEMALE, MANAGEMENT_FEMALE_PCT,
    PAY_EQUITY_INDEX, AVG_AGE, EGALITE_INDEX_SCORE
)
SELECT 
    'DIV-' || LPAD(ROW_NUMBER() OVER (), 5, '0') AS METRIC_ID,
    cal.DATE_KEY AS YEAR_MONTH,
    'MONTHLY' AS REPORT_TYPE,
    bu.BU_ID,
    'ALL' AS JOB_LEVEL,
    UNIFORM(80, 200, RANDOM()) AS TOTAL_HEADCOUNT,
    UNIFORM(45, 120, RANDOM()) AS MALE_COUNT,
    UNIFORM(35, 80, RANDOM()) AS FEMALE_COUNT,
    UNIFORM(40, 48, RANDOM()) AS FEMALE_PERCENTAGE,  -- Target ~45%
    UNIFORM(10, 30, RANDOM()) AS MANAGEMENT_TOTAL,
    UNIFORM(3, 12, RANDOM()) AS MANAGEMENT_FEMALE,
    UNIFORM(30, 45, RANDOM()) AS MANAGEMENT_FEMALE_PCT,
    UNIFORM(92, 100, RANDOM()) AS PAY_EQUITY_INDEX,
    UNIFORM(38, 45, RANDOM()) AS AVG_AGE,
    UNIFORM(82, 95, RANDOM()) AS EGALITE_INDEX_SCORE  -- French equality index
FROM (SELECT DISTINCT DATE_TRUNC('MONTH', DATE_KEY) AS DATE_KEY FROM TDF_DATA_PLATFORM.CORE.CALENDAR WHERE DATE_KEY BETWEEN '2025-06-01' AND '2025-12-31') cal
CROSS JOIN (SELECT * FROM TDF_DATA_PLATFORM.CORE.BUSINESS_UNITS WHERE BU_TYPE != 'CORPORATE') bu;

SELECT 'HR DATA SEEDED' AS STATUS,
       (SELECT COUNT(*) FROM EMPLOYEES) AS EMPLOYEES_COUNT,
       (SELECT COUNT(*) FROM SKILLS) AS SKILLS_COUNT,
       (SELECT COUNT(*) FROM SKILLS_MATRIX) AS SKILLS_MATRIX_COUNT,
       (SELECT COUNT(*) FROM WORKFORCE_CAPACITY) AS CAPACITY_COUNT,
       (SELECT COUNT(*) FROM DIVERSITY_METRICS) AS DIVERSITY_COUNT;

