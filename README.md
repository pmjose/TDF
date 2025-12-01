# TDF Data Platform - Snowflake Database

A comprehensive Snowflake database solution for TDF Infrastructure, enabling data-driven decision making across four core business use cases.

## Overview

**TDF (Télédiffusion de France)** is a leading French telecommunications infrastructure company with:
- **8,785 active sites** across metropolitan France
- **7,877 towers** (manufactured at TDF Toulouse factory)
- **EUR 799.1M** annual revenue
- **42-53%** EBITDAaL margin
- **BBB-** Fitch credit rating (stable)

This database platform provides a single source of truth for:
1. **Resource & Capacity Planning** (P1)
2. **ESG Regulatory Reporting** (P1)
3. **Infrastructure Data Mastery & Digital Twin** (P2)
4. **CAPEX & Equipment Lifecycle Management** (P2)

## Data Period

- **Historical Data:** June 1, 2025 - December 19, 2025
- **Forecast Horizon:** December 2025 - June 2027

## Database Architecture

### Schemas

| Schema | Purpose |
|--------|---------|
| `CORE` | Master data (regions, departments, business units, operators) |
| `HR` | Workforce, skills, capacity planning, diversity metrics |
| `COMMERCIAL` | Demand forecasting, projects, contracts, investment scenarios |
| `OPERATIONS` | Work orders, maintenance, resource allocation, incidents |
| `INFRASTRUCTURE` | Sites, towers, rooftops, antennas, equipment, fibre |
| `FINANCE` | CAPEX, budgets, accounting, revenue by segment/client |
| `ENERGY` | Consumption readings, carbon emissions, renewables |
| `ESG` | Regulatory reports, audit trails, board scorecard, compliance |
| `DIGITAL_TWIN` | 3D models, discrepancy detection, data quality |
| `ANALYTICS` | Pre-built views for dashboards |

### Data Scale (Per TDF 2025 Investor Presentation)

| Category | Count |
|----------|-------|
| Active Sites | 8,785 |
| Towers | 7,877 |
| Rooftops (PoS) | 8,174 |
| Indoor Sites (DAS) | 908 |
| Edge Data Centers | 102 |
| Regional Data Centers | 4 |
| Points of Service | 21,244 |
| Antennas | ~25,000 |
| Broadcast Transmitters | ~3,500 |
| Equipment Pieces | ~80,000 |
| Client Installations | ~20,000 |
| Fibre Network | 5,500 km |
| Employees | ~1,500 |

## Deployment

### Option 1: Git Integration (Recommended)

1. **One-time setup** (requires ACCOUNTADMIN):
```sql
-- Run 00_GIT_SETUP.sql to configure Git integration
-- Repository: https://github.com/pmjose/TDF
```

2. **Deploy from Git:**
```sql
ALTER GIT REPOSITORY TDF_DATA_PLATFORM.PUBLIC.TDF_REPO FETCH;
EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/00_MASTER_DEPLOY.sql;
```

### Option 2: Manual Deployment

Run scripts in order using Snowflake worksheet or SnowSQL:

```sql
-- DDL Files (01-12)
USE ROLE SYSADMIN;
-- Run each DDL file in order...

-- Data Files (13-22)
-- Run each seed file in order...
```

## File Structure

```
/sql
  00_MASTER_DEPLOY.sql        # Orchestrator script
  00_GIT_SETUP.sql            # Git integration setup
  
  /ddl                        # Table definitions (12 files)
    01_database_setup.sql     # Database, schemas, roles, warehouse
    02_core_tables.sql        # Regions, departments, operators
    03_infrastructure_tables.sql
    04_hr_tables.sql
    05_commercial_tables.sql
    06_operations_tables.sql
    07_finance_tables.sql
    08_energy_tables.sql
    09_esg_tables.sql
    10_digital_twin_tables.sql
    11_analytics_views.sql
    12_executive_views.sql
  
  /data                       # Seed data (10 files)
    13_seed_france_geo.sql    # 13 regions, 96 departments, calendar
    14_seed_operators.sql     # Orange, SFR, Bouygues, Free + reference data
    15_seed_infrastructure.sql # Sites, towers, DCs, equipment
    16_seed_hr_data.sql       # Employees, skills, diversity
    17_seed_operations.sql    # Work orders, maintenance, incidents
    18_seed_finance.sql       # Revenue, CAPEX, accounting
    19_seed_energy_esg.sql    # Energy, carbon, ESG reports
    20_seed_executive_kpis.sql # Digital twin, scenarios
    21_seed_commercial.sql    # Demand forecast, projects, contracts
    22_seed_infrastructure_detail.sql  # Rooftops, antennas, fibre, PoS
  
  /queries                    # Sample queries for demos (6 files)
    uc1_capacity_planning.sql
    uc2_esg_reporting.sql
    uc3_digital_twin.sql
    uc4_capex_lifecycle.sql
    executive_dashboard.sql
    esg_regulatory_reports.sql
```

## Use Cases

### UC1: Resource & Capacity Planning

**Business Challenge:** Manual monthly Excel-based capacity assessment taking 3 days, leading to delayed decisions.

**Solution:** Real-time automated forecasting for 18 months with dynamic scenario modeling.

**Key Tables:**
- `HR.EMPLOYEES`, `HR.SKILLS_MATRIX`, `HR.WORKFORCE_CAPACITY`
- `COMMERCIAL.DEMAND_FORECAST`, `COMMERCIAL.PROJECTS`, `COMMERCIAL.CONTRACTS`
- `OPERATIONS.WORK_ORDERS`, `OPERATIONS.RESOURCE_ALLOCATION`

**Sample Query:**
```sql
SELECT * FROM ANALYTICS.VW_CAPACITY_VS_DEMAND WHERE YEAR_MONTH >= '2025-06-01';
```

### UC2: ESG Regulatory Reporting

**Business Challenge:** High-stakes regulatory reports requiring manual aggregation with perfect data lineage for external audit.

**Solution:** Traced, auditable reporting engine with data traceability to source entries.

**Supported Regulations:**

| Regulation | Type | View |
|------------|------|------|
| CSRD | EU 2022/2464 | `VW_CSRD_REPORT` |
| Index Égalité H/F | French Law 2018-771 | `VW_INDEX_EGALITE_PROFESSIONNELLE` |
| Bilan GES | Art L229-25 | `VW_BILAN_GES` |
| DPEF | Art L225-102-1 | `VW_DPEF_REPORT` |
| EU Taxonomy | EU 2020/852 | `VW_EU_TAXONOMY` |

**Key Tables:**
- `FINANCE.ACCOUNTING_ENTRIES` (with full audit trail)
- `ENERGY.CONSUMPTION_READINGS`, `ENERGY.CARBON_EMISSIONS`, `ENERGY.RENEWABLE_ENERGY`
- `HR.DIVERSITY_METRICS`
- `ESG.REGULATORY_REPORTS`, `ESG.AUDIT_TRAIL`, `ESG.BOARD_SCORECARD`
- `ESG.COMPLIANCE_REQUIREMENTS`, `ESG.ESG_TARGETS`

**Sample Queries:**
```sql
-- French Gender Equality Index (mandatory publication)
SELECT * FROM ANALYTICS.VW_INDEX_EGALITE_SUMMARY;

-- CSRD Report (EU comprehensive ESG)
SELECT * FROM ANALYTICS.VW_CSRD_REPORT;

-- Carbon Footprint (Bilan GES)
SELECT * FROM ANALYTICS.VW_BILAN_GES;

-- Compliance Dashboard
SELECT * FROM ANALYTICS.VW_ESG_COMPLIANCE_DASHBOARD;
```

### UC3: Infrastructure Data Mastery & Digital Twin

**Business Challenge:** Data coherence issues and lack of single source of truth for 7,877 towers limiting Digital Twin value.

**Solution:** Infrastructure Data Control Tower with real-time discrepancy detection.

**Key Tables:**
- `INFRASTRUCTURE.SITES`, `INFRASTRUCTURE.TOWERS`, `INFRASTRUCTURE.ROOFTOPS`
- `INFRASTRUCTURE.EQUIPMENT`, `INFRASTRUCTURE.ANTENNAS`, `INFRASTRUCTURE.BROADCAST_TRANSMITTERS`
- `INFRASTRUCTURE.FIBRE_NETWORK`, `INFRASTRUCTURE.POINTS_OF_SERVICE`
- `DIGITAL_TWIN.ASSET_MODELS`, `DIGITAL_TWIN.DISCREPANCY_LOG`
- `DIGITAL_TWIN.DATA_QUALITY_SCORES`, `DIGITAL_TWIN.VALIDATION_RULES`

**Sample Query:**
```sql
SELECT * FROM ANALYTICS.VW_DIGITAL_TWIN_QUALITY;
```

### UC4: CAPEX & Equipment Lifecycle

**Business Challenge:** Lack of comprehensive data on 7-10 year equipment lifecycles making it difficult to forecast CAPEX.

**Solution:** Predictive renewal model with exhaustive equipment inventory and lifecycle tracking.

**Key Tables:**
- `INFRASTRUCTURE.EQUIPMENT` (with lifecycle status)
- `FINANCE.CAPEX_BUDGET`, `FINANCE.CAPEX_ACTUALS`, `FINANCE.RENEWAL_FORECAST`
- `OPERATIONS.MAINTENANCE_RECORDS`, `OPERATIONS.EQUIPMENT_STATUS`

**Sample Query:**
```sql
SELECT * FROM ANALYTICS.VW_RENEWAL_FORECAST_SUMMARY WHERE REPLACEMENT_YEAR BETWEEN 2025 AND 2028;
```

## C-Level Executive Views

Pre-built views for executive dashboards with traffic light indicators:

| View | Purpose |
|------|---------|
| `VW_EXECUTIVE_KPIS` | Single-view KPI summary with traffic lights |
| `VW_REVENUE_EXECUTIVE` | Revenue by segment with YoY comparison |
| `VW_REVENUE_BY_CLIENT_EXECUTIVE` | Client revenue concentration analysis |
| `VW_EBITDA_BY_BU` | EBITDA margin by business unit (42-53% target) |
| `VW_RISK_DASHBOARD` | Consolidated risk view (equipment, compliance, data quality) |
| `VW_CLIENT_CONCENTRATION` | Client revenue dependency analysis |
| `VW_INVESTMENT_SCENARIOS` | What-if analysis for CAPEX decisions |
| `VW_COST_PER_TOWER` | Total cost of ownership analysis |
| `VW_REVENUE_PER_SITE` | Site profitability metrics |
| `VW_MARKET_SHARE` | Regional market position |

## ESG Regulatory Compliance

### French Regulations

| Regulation | Description | Frequency |
|------------|-------------|-----------|
| **Index Égalité H/F** | Gender equality index (must score ≥75/100) | Annual (March 1) |
| **Bilan GES** | Greenhouse gas emissions report | Every 4 years |
| **DPEF** | Extra-financial performance declaration | Annual |

### EU Regulations

| Regulation | Description | Frequency |
|------------|-------------|-----------|
| **CSRD** | Corporate Sustainability Reporting Directive | Annual |
| **EU Taxonomy** | Taxonomy-aligned revenue/CAPEX/OPEX | Annual |

## France Geographic Coverage

- **13 Metropolitan Regions:** Île-de-France, Auvergne-Rhône-Alpes, Nouvelle-Aquitaine, Occitanie, Hauts-de-France, Grand Est, PACA, Pays de la Loire, Bretagne, Normandie, Bourgogne-Franche-Comté, Centre-Val de Loire, Corse
- **96 Departments:** Full coverage with proper codes (01-95, 2A, 2B)
- **GPS Coordinates:** Metropolitan France (lat: 42.0-51.5, lon: -5.0 to 9.5)

## Key TDF Locations

- **Fort de Romainville** (Les Lilas, 93) - Network Operations Center
- **Toulouse** (31) - Tower manufacturing facility
- **Tour Eiffel** (75) - Major broadcast site
- **Pic du Midi** (65) - High-altitude broadcast site
- **Mont Ventoux** (84) - Strategic broadcast location
- **Bordeaux, Lille, Marseille, Rennes** - Regional Data Centers

## Mobile Operators (Clients)

| Operator | Revenue Share | Contract Status |
|----------|--------------|-----------------|
| Orange | ~31% | Strategic Client |
| SFR | ~27% | Strategic Client |
| Bouygues Telecom | ~24% | Strategic Client |
| Free Mobile | ~18% | Strategic Client |

## Broadcast Clients

- **France Télévisions** (France 2, France 3, etc.)
- **Radio France** (France Inter, France Info, France Culture)
- **NRJ Group** (NRJ, Nostalgie, Chérie FM)
- **RTL Group**

## Technical Notes

### Snowflake Features Used

- **Git Integration:** Native repository connection for CI/CD deployment
- **Views:** 20+ pre-built analytics views for dashboard consumption
- **Future Grants:** Automatic permissions for new objects
- **Window Functions:** For time-series analysis and rankings

### Data Quality

- ~3% intentional discrepancies for Digital Twin demo
- Seasonal energy patterns (winter +30%, summer +15%)
- Realistic equipment age distribution (1-15 years)
- Full audit trail for ESG compliance

### Roles

| Role | Purpose |
|------|---------|
| `TDF_ADMIN` | Full database administration |
| `TDF_ANALYST` | Read access to all schemas |
| `TDF_ENGINEER` | Infrastructure and operations access |
| `TDF_EXECUTIVE` | Executive dashboard access |

## Quick Start Queries

```sql
-- Executive KPIs
SELECT * FROM ANALYTICS.VW_EXECUTIVE_KPIS ORDER BY PERIOD_DATE DESC LIMIT 1;

-- Infrastructure Summary
SELECT SITE_TYPE, COUNT(*) AS COUNT, AVG(COLOCATION_RATE) AS AVG_COLOCATION
FROM INFRASTRUCTURE.SITES WHERE STATUS = 'ACTIVE' GROUP BY SITE_TYPE;

-- ESG Board Scorecard
SELECT * FROM ESG.BOARD_SCORECARD ORDER BY REPORTING_DATE DESC LIMIT 1;

-- Equipment at Risk
SELECT COUNT(*) AS CRITICAL_EQUIPMENT
FROM OPERATIONS.EQUIPMENT_STATUS WHERE FAILURE_RISK_SCORE >= 80;
```

## License

Proprietary - TDF Infrastructure demonstration database

## Contact

For questions about this demo database, contact your TDF/Snowflake account team.
