-- ============================================================================
-- TDF DATA PLATFORM - ENERGY TABLES
-- ============================================================================
-- Energy: Consumption Readings, Carbon Emissions
-- Supports UC2: ESG Regulatory Reporting
-- Note: Energy is re-invoiced to customers as pass-through
-- ============================================================================

USE ROLE SYSADMIN;
USE WAREHOUSE TDF_WH;
USE DATABASE TDF_DATA_PLATFORM;
USE SCHEMA ENERGY;

-- ============================================================================
-- CONSUMPTION_READINGS (kWh per site - monthly)
-- ============================================================================

CREATE OR REPLACE TABLE CONSUMPTION_READINGS (
    READING_ID VARCHAR(20) PRIMARY KEY,
    
    -- Location
    SITE_ID VARCHAR(20) NOT NULL REFERENCES TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES(SITE_ID),
    METER_ID VARCHAR(30),
    
    -- Period
    READING_DATE DATE NOT NULL,
    YEAR_MONTH DATE NOT NULL COMMENT 'First day of month',
    
    -- Consumption
    CONSUMPTION_KWH FLOAT NOT NULL,
    CONSUMPTION_PRIOR_MONTH_KWH FLOAT,
    CONSUMPTION_PRIOR_YEAR_KWH FLOAT,
    
    -- Peak/Off-Peak
    PEAK_CONSUMPTION_KWH FLOAT,
    OFF_PEAK_CONSUMPTION_KWH FLOAT,
    
    -- Cost
    ENERGY_COST_EUR FLOAT,
    COST_PER_KWH_EUR FLOAT,
    
    -- Pass-through
    IS_PASSTHROUGH BOOLEAN DEFAULT TRUE COMMENT 'Re-invoiced to customer',
    PASSTHROUGH_AMOUNT_EUR FLOAT,
    
    -- Normalization
    HEATING_DEGREE_DAYS INTEGER COMMENT 'For seasonal adjustment',
    COOLING_DEGREE_DAYS INTEGER,
    NORMALIZED_CONSUMPTION_KWH FLOAT COMMENT 'Weather-normalized',
    
    -- Quality
    IS_ESTIMATED BOOLEAN DEFAULT FALSE,
    ESTIMATION_METHOD VARCHAR(50),
    DATA_QUALITY_SCORE INTEGER COMMENT '0-100',
    
    -- ESG Audit Trail
    SOURCE_SYSTEM VARCHAR(50) DEFAULT 'TDF_ENERGY_MGT',
    SOURCE_RECORD_ID VARCHAR(50),
    
    -- Audit
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- ENERGY_SUPPLIERS (Energy provider contracts)
-- ============================================================================

CREATE OR REPLACE TABLE ENERGY_SUPPLIERS (
    SUPPLIER_ID VARCHAR(20) PRIMARY KEY,
    SUPPLIER_NAME VARCHAR(100) NOT NULL,
    
    -- Contract
    CONTRACT_NUMBER VARCHAR(50),
    CONTRACT_START_DATE DATE,
    CONTRACT_END_DATE DATE,
    
    -- Pricing
    FIXED_RATE_EUR_KWH FLOAT,
    VARIABLE_RATE_EUR_KWH FLOAT,
    
    -- Green Energy
    RENEWABLE_PCT FLOAT COMMENT 'Percentage from renewable sources',
    CARBON_FACTOR_KG_KWH FLOAT COMMENT 'kg CO2 per kWh',
    
    -- Coverage
    REGIONS_COVERED VARCHAR(500),
    SITES_COUNT INTEGER,
    
    -- Audit
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- CARBON_EMISSIONS (CO2 calculations - UC2 ESG)
-- ============================================================================

CREATE OR REPLACE TABLE CARBON_EMISSIONS (
    EMISSION_ID VARCHAR(20) PRIMARY KEY,
    
    -- Period
    YEAR_MONTH DATE NOT NULL,
    FISCAL_YEAR INTEGER,
    
    -- Scope
    EMISSION_SCOPE VARCHAR(20) NOT NULL COMMENT 'SCOPE_1, SCOPE_2, SCOPE_3',
    EMISSION_CATEGORY VARCHAR(50) COMMENT 'ELECTRICITY, FUEL, VEHICLES, TRAVEL, SUPPLY_CHAIN',
    
    -- Location
    SITE_ID VARCHAR(20) REFERENCES TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES(SITE_ID),
    REGION_ID VARCHAR(10) REFERENCES TDF_DATA_PLATFORM.CORE.REGIONS(REGION_ID),
    BU_ID VARCHAR(10) REFERENCES TDF_DATA_PLATFORM.CORE.BUSINESS_UNITS(BU_ID),
    
    -- Emission Data
    EMISSIONS_KG_CO2E FLOAT NOT NULL COMMENT 'kg CO2 equivalent',
    EMISSIONS_TONNES_CO2E FLOAT GENERATED ALWAYS AS (EMISSIONS_KG_CO2E / 1000) VIRTUAL,
    
    -- Activity Data
    ACTIVITY_QUANTITY FLOAT COMMENT 'kWh, liters, km, etc.',
    ACTIVITY_UNIT VARCHAR(20),
    EMISSION_FACTOR FLOAT COMMENT 'kg CO2e per activity unit',
    EMISSION_FACTOR_SOURCE VARCHAR(100),
    
    -- Intensity Metrics
    REVENUE_EUR FLOAT,
    CARBON_INTENSITY_KG_EUR FLOAT COMMENT 'kg CO2e per EUR revenue',
    EMPLOYEES INTEGER,
    CARBON_INTENSITY_KG_EMPLOYEE FLOAT,
    
    -- Targets
    TARGET_EMISSIONS_KG FLOAT,
    VARIANCE_TO_TARGET_KG FLOAT,
    
    -- Methodology
    CALCULATION_METHOD VARCHAR(100) COMMENT 'GHG Protocol, ISO 14064, etc.',
    DATA_QUALITY_SCORE INTEGER COMMENT '0-100',
    IS_VERIFIED BOOLEAN DEFAULT FALSE,
    VERIFIER_NAME VARCHAR(100),
    
    -- ESG Audit Trail - Critical for UC2
    SOURCE_SYSTEM VARCHAR(50) DEFAULT 'TDF_ESG_PLATFORM',
    SOURCE_RECORD_ID VARCHAR(50) NOT NULL,
    CALCULATION_DATE TIMESTAMP_NTZ,
    CALCULATED_BY VARCHAR(100),
    
    -- Audit
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- RENEWABLE_ENERGY (Renewable energy tracking)
-- ============================================================================

CREATE OR REPLACE TABLE RENEWABLE_ENERGY (
    RENEWABLE_ID VARCHAR(20) PRIMARY KEY,
    
    -- Period
    YEAR_MONTH DATE NOT NULL,
    
    -- Location
    SITE_ID VARCHAR(20) REFERENCES TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES(SITE_ID),
    REGION_ID VARCHAR(10) REFERENCES TDF_DATA_PLATFORM.CORE.REGIONS(REGION_ID),
    
    -- Consumption Breakdown
    TOTAL_CONSUMPTION_KWH FLOAT,
    RENEWABLE_CONSUMPTION_KWH FLOAT,
    NON_RENEWABLE_CONSUMPTION_KWH FLOAT,
    RENEWABLE_PCT FLOAT,
    
    -- Renewable Sources
    SOLAR_KWH FLOAT,
    WIND_KWH FLOAT,
    HYDRO_KWH FLOAT,
    OTHER_RENEWABLE_KWH FLOAT,
    
    -- On-site Generation
    ONSITE_SOLAR_KWH FLOAT,
    ONSITE_GENERATION_PCT FLOAT,
    
    -- Certificates
    GO_CERTIFICATES_MWH FLOAT COMMENT 'Guarantees of Origin',
    
    -- Targets
    TARGET_RENEWABLE_PCT FLOAT,
    VARIANCE_TO_TARGET_PCT FLOAT,
    
    -- Audit
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- ENERGY_EFFICIENCY_METRICS
-- ============================================================================

CREATE OR REPLACE TABLE ENERGY_EFFICIENCY_METRICS (
    METRIC_ID VARCHAR(20) PRIMARY KEY,
    
    -- Period
    YEAR_MONTH DATE NOT NULL,
    
    -- Location
    SITE_ID VARCHAR(20) REFERENCES TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES(SITE_ID),
    DC_ID VARCHAR(20) REFERENCES TDF_DATA_PLATFORM.INFRASTRUCTURE.DATA_CENTERS(DC_ID),
    SITE_TYPE VARCHAR(30),
    
    -- PUE (Power Usage Effectiveness) - for data centers
    IT_LOAD_KW FLOAT,
    TOTAL_FACILITY_LOAD_KW FLOAT,
    PUE_RATIO FLOAT COMMENT 'Total/IT load, target <1.5',
    
    -- Site Efficiency
    CONSUMPTION_KWH FLOAT,
    EQUIPMENT_COUNT INTEGER,
    KWH_PER_EQUIPMENT FLOAT,
    
    -- Trends
    EFFICIENCY_IMPROVEMENT_PCT FLOAT COMMENT 'vs prior period',
    
    -- Benchmarks
    INDUSTRY_BENCHMARK_PUE FLOAT,
    VS_BENCHMARK_PCT FLOAT,
    
    -- Actions
    EFFICIENCY_INITIATIVES VARCHAR(500),
    ESTIMATED_SAVINGS_KWH FLOAT,
    ESTIMATED_SAVINGS_EUR FLOAT,
    
    -- Audit
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- INDEXES
-- ============================================================================

CREATE OR REPLACE INDEX IDX_CONSUMPTION_SITE ON CONSUMPTION_READINGS(SITE_ID);
CREATE OR REPLACE INDEX IDX_CONSUMPTION_PERIOD ON CONSUMPTION_READINGS(YEAR_MONTH);
CREATE OR REPLACE INDEX IDX_EMISSIONS_PERIOD ON CARBON_EMISSIONS(YEAR_MONTH);
CREATE OR REPLACE INDEX IDX_EMISSIONS_SCOPE ON CARBON_EMISSIONS(EMISSION_SCOPE);
CREATE OR REPLACE INDEX IDX_RENEWABLE_PERIOD ON RENEWABLE_ENERGY(YEAR_MONTH);

SELECT 'ENERGY TABLES CREATED' AS STATUS, CURRENT_TIMESTAMP() AS CREATED_AT;

