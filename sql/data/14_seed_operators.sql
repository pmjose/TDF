-- ============================================================================
-- TDF DATA PLATFORM - SEED DATA: OPERATORS & REFERENCE DATA
-- ============================================================================
-- Mobile Operators, Business Units, Equipment Types, Skill Categories
-- ============================================================================

USE DATABASE TDF_DATA_PLATFORM;
USE SCHEMA CORE;

-- ============================================================================
-- OPERATORS (4 Major French MNOs + Others)
-- ============================================================================

INSERT INTO OPERATORS (OPERATOR_ID, OPERATOR_CODE, OPERATOR_NAME, OPERATOR_TYPE, PARENT_COMPANY, CONTRACT_START_DATE, CONTRACT_END_DATE, ANNUAL_REVENUE_EUR, CREDIT_RATING, IS_STRATEGIC_CLIENT)
VALUES
    ('OP-ORANGE', 'ORANGE', 'Orange France', 'MNO', 'Orange SA', '2010-01-01', '2030-12-31', 145000000, 'BBB+', TRUE),
    ('OP-SFR', 'SFR', 'SFR', 'MNO', 'Altice France', '2012-03-01', '2028-12-31', 125000000, 'BB', TRUE),
    ('OP-BOUYGUES', 'BOUYGUES', 'Bouygues Telecom', 'MNO', 'Bouygues SA', '2011-06-01', '2029-06-30', 110000000, 'BBB', TRUE),
    ('OP-FREE', 'FREE', 'Free Mobile', 'MNO', 'Iliad SA', '2012-01-01', '2027-12-31', 85000000, 'BB+', TRUE),
    ('OP-TDF-BC', 'TDF-BC', 'TDF Broadcast (Internal)', 'BROADCASTER', 'TDF Infrastructure', '2000-01-01', '2099-12-31', 0, 'BBB-', FALSE),
    ('OP-FTV', 'FTV', 'France Télévisions', 'BROADCASTER', 'France Télévisions', '2005-01-01', '2030-12-31', 35000000, 'AA', TRUE),
    ('OP-RADIOFR', 'RADIOFR', 'Radio France', 'BROADCASTER', 'Radio France', '2005-01-01', '2030-12-31', 28000000, 'AA', TRUE),
    ('OP-NRJ', 'NRJ', 'NRJ Group', 'BROADCASTER', 'NRJ Group', '2008-01-01', '2028-12-31', 15000000, 'BBB', FALSE),
    ('OP-SIGFOX', 'SIGFOX', 'Sigfox', 'IOT', 'UnaBiz', '2015-01-01', '2026-12-31', 5000000, 'B', FALSE),
    ('OP-LORA', 'LORA', 'LoRa Alliance Members', 'IOT', 'Various', '2016-01-01', '2028-12-31', 8000000, 'B+', FALSE);

-- ============================================================================
-- BUSINESS UNITS (TDF Organizational Structure)
-- ============================================================================

INSERT INTO BUSINESS_UNITS (BU_ID, BU_CODE, BU_NAME, BU_TYPE, PARENT_BU_ID, REVENUE_TARGET_EUR, HEADCOUNT_TARGET, COST_CENTER)
VALUES
    -- Level 1: Main Segments
    ('BU-CORP', 'CORPORATE', 'TDF Corporate', 'CORPORATE', NULL, 799100000, 1500, 'CC-CORP'),
    ('BU-BC', 'BROADCAST', 'Broadcast Services', 'BROADCAST', 'BU-CORP', 273900000, 450, 'CC-BC'),
    ('BU-TEL', 'TELECOM', 'Telecom Infrastructure', 'TELECOM', 'BU-CORP', 483700000, 850, 'CC-TEL'),
    ('BU-EDGE', 'CONNECTIVITY', 'Connectivity & Edge', 'CONNECTIVITY', 'BU-CORP', 34400000, 150, 'CC-EDGE'),
    
    -- Level 2: Broadcast Sub-units
    ('BU-DTT', 'DTT', 'Digital Television (TNT)', 'BROADCAST', 'BU-BC', 157300000, 180, 'CC-DTT'),
    ('BU-RADIO', 'RADIO', 'Radio Services', 'BROADCAST', 'BU-BC', 116600000, 170, 'CC-RADIO'),
    
    -- Level 2: Telecom Sub-units
    ('BU-HOST', 'HOSTING', 'Site Hosting', 'TELECOM', 'BU-TEL', 457700000, 600, 'CC-HOST'),
    ('BU-OTHR', 'OTHER-TEL', 'Other Telecom Services', 'TELECOM', 'BU-TEL', 26000000, 150, 'CC-OTHR'),
    
    -- Level 2: Connectivity Sub-units
    ('BU-IND', 'INDOOR', 'Indoor Coverage', 'CONNECTIVITY', 'BU-EDGE', 10400000, 40, 'CC-IND'),
    ('BU-EDC', 'EDGE-DC', 'Edge & Connect', 'CONNECTIVITY', 'BU-EDGE', 22600000, 80, 'CC-EDC'),
    ('BU-PMN', 'PMN', 'Private Mobile Networks', 'CONNECTIVITY', 'BU-EDGE', 1400000, 20, 'CC-PMN'),
    
    -- Operations & Support
    ('BU-NOC', 'NOC', 'Network Operations Center', 'CORPORATE', 'BU-CORP', 0, 120, 'CC-NOC'),
    ('BU-ENG', 'ENGINEERING', 'Engineering & Innovation', 'CORPORATE', 'BU-CORP', 0, 80, 'CC-ENG');

-- ============================================================================
-- SITE TYPES
-- ============================================================================

INSERT INTO SITE_TYPES (SITE_TYPE_ID, SITE_TYPE_CODE, SITE_TYPE_NAME, DESCRIPTION, TYPICAL_HEIGHT_M, TYPICAL_CAPACITY, MAINTENANCE_FREQUENCY_DAYS, AVERAGE_ENERGY_KWH_MONTH)
VALUES
    ('ST-TOWER', 'TOWER', 'Ground Tower', 'Self-supporting lattice or monopole tower', 50, 4, 180, 2500),
    ('ST-ROOF', 'ROOFTOP', 'Rooftop Installation', 'Building rooftop antenna installation', 10, 3, 365, 1500),
    ('ST-INDOOR', 'INDOOR', 'Indoor DAS', 'Distributed Antenna System for indoor coverage', 0, 2, 365, 800),
    ('ST-EDC', 'DATACENTER', 'Edge Data Center', 'Mobile Edge Computing facility', 0, 10, 90, 50000),
    ('ST-BCAST', 'BROADCAST', 'Broadcast Site', 'High-power TV/Radio transmission site', 200, 6, 90, 15000),
    ('ST-SMALL', 'SMALLCELL', 'Small Cell', 'Urban small cell installation', 5, 1, 365, 200);

-- ============================================================================
-- EQUIPMENT TYPES
-- ============================================================================

INSERT INTO EQUIPMENT_TYPES (EQUIPMENT_TYPE_ID, EQUIPMENT_TYPE_CODE, EQUIPMENT_TYPE_NAME, CATEGORY, MANUFACTURER, EXPECTED_LIFESPAN_YEARS, MAINTENANCE_INTERVAL_MONTHS, AVERAGE_UNIT_COST_EUR, POWER_CONSUMPTION_W, IS_CRITICAL)
VALUES
    -- Antennas
    ('ET-ANT-4G', 'ANT-4G', '4G LTE Panel Antenna', 'ANTENNA', 'Ericsson/Nokia/Huawei', 10, 24, 3500, 0, FALSE),
    ('ET-ANT-5G', 'ANT-5G', '5G Massive MIMO Antenna', 'ANTENNA', 'Ericsson/Nokia/Samsung', 8, 12, 15000, 0, FALSE),
    ('ET-ANT-FM', 'ANT-FM', 'FM Radio Antenna', 'ANTENNA', 'RFS/Kathrein', 15, 36, 8000, 0, FALSE),
    ('ET-ANT-DTT', 'ANT-DTT', 'DTT UHF Antenna', 'ANTENNA', 'Kathrein/Andrew', 15, 36, 12000, 0, FALSE),
    
    -- Transmitters
    ('ET-TX-DTT', 'TX-DTT', 'DTT Transmitter', 'TRANSMITTER', 'Rohde & Schwarz', 12, 12, 150000, 5000, TRUE),
    ('ET-TX-FM', 'TX-FM', 'FM Radio Transmitter', 'TRANSMITTER', 'Nautel/GatesAir', 15, 12, 80000, 3000, TRUE),
    ('ET-TX-DAB', 'TX-DAB', 'DAB+ Transmitter', 'TRANSMITTER', 'Rohde & Schwarz', 10, 12, 120000, 2500, TRUE),
    
    -- Radio Units
    ('ET-RRU-4G', 'RRU-4G', '4G Remote Radio Unit', 'NETWORK', 'Ericsson/Nokia', 8, 12, 8000, 500, TRUE),
    ('ET-RRU-5G', 'RRU-5G', '5G Radio Unit', 'NETWORK', 'Ericsson/Nokia/Samsung', 7, 12, 25000, 1200, TRUE),
    ('ET-BBU', 'BBU', 'Baseband Unit', 'NETWORK', 'Ericsson/Nokia/Huawei', 7, 12, 35000, 800, TRUE),
    
    -- Power
    ('ET-UPS', 'UPS', 'Uninterruptible Power Supply', 'POWER', 'APC/Eaton', 10, 6, 15000, 200, TRUE),
    ('ET-GEN', 'GENERATOR', 'Diesel Generator', 'POWER', 'Caterpillar/Kohler', 20, 6, 50000, 0, TRUE),
    ('ET-RECT', 'RECTIFIER', 'Power Rectifier', 'POWER', 'Eltek/Emerson', 12, 12, 8000, 100, TRUE),
    ('ET-BATT', 'BATTERY', 'Battery Bank', 'POWER', 'Various', 5, 6, 12000, 0, TRUE),
    
    -- HVAC
    ('ET-AC', 'AC', 'Air Conditioning Unit', 'HVAC', 'Daikin/Carrier', 10, 6, 5000, 2000, FALSE),
    ('ET-VENT', 'VENTILATION', 'Ventilation System', 'HVAC', 'Various', 15, 12, 3000, 500, FALSE),
    
    -- Security & Monitoring
    ('ET-CAM', 'CAMERA', 'Security Camera', 'SECURITY', 'Hikvision/Axis', 7, 12, 500, 15, FALSE),
    ('ET-ALARM', 'ALARM', 'Alarm System', 'SECURITY', 'Various', 10, 12, 2000, 50, FALSE),
    ('ET-SENSOR', 'SENSOR', 'Environmental Sensor', 'MONITORING', 'Various', 7, 24, 300, 5, FALSE);

-- ============================================================================
-- SKILL CATEGORIES
-- ============================================================================

INSERT INTO SKILL_CATEGORIES (SKILL_CATEGORY_ID, SKILL_CATEGORY_CODE, SKILL_CATEGORY_NAME, DESCRIPTION, IS_TECHNICAL, CERTIFICATION_REQUIRED)
VALUES
    ('SC-RF', 'RF', 'RF Engineering', 'Radio frequency engineering and antenna systems', TRUE, TRUE),
    ('SC-TOWER', 'TOWER', 'Tower Climbing & Rigging', 'Tower work, rigging, and structural maintenance', TRUE, TRUE),
    ('SC-ELEC', 'ELECTRICAL', 'Electrical Systems', 'Power systems, UPS, generators', TRUE, TRUE),
    ('SC-TX', 'BROADCAST-TX', 'Broadcast Transmission', 'TV and radio transmitter maintenance', TRUE, TRUE),
    ('SC-NET', 'NETWORKING', 'Network Engineering', 'IP networking, fiber, transport', TRUE, FALSE),
    ('SC-IT', 'IT', 'IT Systems', 'Servers, databases, applications', TRUE, FALSE),
    ('SC-PM', 'PROJECT-MGT', 'Project Management', 'Project planning and execution', FALSE, TRUE),
    ('SC-CIVIL', 'CIVIL', 'Civil Engineering', 'Site construction, foundations', TRUE, TRUE),
    ('SC-SAFETY', 'SAFETY', 'Health & Safety', 'Safety compliance and training', FALSE, TRUE),
    ('SC-ENV', 'ENVIRONMENTAL', 'Environmental', 'ESG, environmental compliance', FALSE, FALSE);

-- ============================================================================
-- COST CENTERS
-- ============================================================================

INSERT INTO COST_CENTERS (COST_CENTER_ID, COST_CENTER_CODE, COST_CENTER_NAME, BU_ID, MANAGER_NAME, ANNUAL_BUDGET_EUR)
VALUES
    ('CC-CORP', 'CC-1000', 'Corporate HQ', 'BU-CORP', 'Executive Team', 50000000),
    ('CC-BC', 'CC-2000', 'Broadcast Division', 'BU-BC', 'Director Broadcast', 80000000),
    ('CC-TEL', 'CC-3000', 'Telecom Division', 'BU-TEL', 'Director Telecom', 150000000),
    ('CC-EDGE', 'CC-4000', 'Connectivity Division', 'BU-EDGE', 'Director Connectivity', 25000000),
    ('CC-DTT', 'CC-2100', 'DTT Operations', 'BU-DTT', 'Manager DTT', 45000000),
    ('CC-RADIO', 'CC-2200', 'Radio Operations', 'BU-RADIO', 'Manager Radio', 35000000),
    ('CC-HOST', 'CC-3100', 'Site Hosting Ops', 'BU-HOST', 'Manager Hosting', 120000000),
    ('CC-OTHR', 'CC-3200', 'Other Telecom', 'BU-OTHR', 'Manager Services', 20000000),
    ('CC-IND', 'CC-4100', 'Indoor Coverage', 'BU-IND', 'Manager Indoor', 8000000),
    ('CC-EDC', 'CC-4200', 'Edge Computing', 'BU-EDC', 'Manager Edge', 15000000),
    ('CC-PMN', 'CC-4300', 'Private Networks', 'BU-PMN', 'Manager PMN', 3000000),
    ('CC-NOC', 'CC-5000', 'NOC Operations', 'BU-NOC', 'NOC Director', 30000000),
    ('CC-ENG', 'CC-6000', 'Engineering', 'BU-ENG', 'CTO', 20000000);

SELECT 'OPERATORS & REFERENCE DATA SEEDED' AS STATUS,
       (SELECT COUNT(*) FROM OPERATORS) AS OPERATORS_COUNT,
       (SELECT COUNT(*) FROM BUSINESS_UNITS) AS BU_COUNT,
       (SELECT COUNT(*) FROM SITE_TYPES) AS SITE_TYPES_COUNT,
       (SELECT COUNT(*) FROM EQUIPMENT_TYPES) AS EQUIPMENT_TYPES_COUNT;

