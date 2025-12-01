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
| `COMMERCIAL` | Demand forecasting, projects, investment scenarios |
| `OPERATIONS` | Work orders, maintenance, resource allocation |
| `INFRASTRUCTURE` | Sites, towers, rooftops, antennas, equipment |
| `FINANCE` | CAPEX, budgets, accounting, revenue by segment |
| `ENERGY` | Consumption readings, carbon emissions |
| `ESG` | Regulatory reports, audit trails, board scorecard |
| `DIGITAL_TWIN` | 3D models, discrepancy detection |
| `ANALYTICS` | Pre-built views for dashboards |

### Key Metrics (Real TDF Scale)

| Category | Count |
|----------|-------|
| Active Sites | 8,785 |
| Towers | 7,877 |
| Rooftops (PoS) | 8,174 |
| Indoor Sites | 908 |
| Edge Data Centers | 102 |
| Regional Data Centers | 4 |
| Points of Service | 21,244 |
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

```bash
snowsql -f sql/ddl/01_database_setup.sql
snowsql -f sql/ddl/02_core_tables.sql
# ... continue with all DDL files
snowsql -f sql/data/13_seed_france_geo.sql
# ... continue with all seed files
```

## File Structure

```
/sql
  00_MASTER_DEPLOY.sql        # Orchestrator script
  00_GIT_SETUP.sql            # Git integration setup
  
  /ddl                        # Table definitions
    01_database_setup.sql
    02_core_tables.sql
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
  
  /data                       # Seed data
    13_seed_france_geo.sql    # 13 regions, 96 departments
    14_seed_operators.sql     # Orange, SFR, Bouygues, Free
    15_seed_infrastructure.sql
    16_seed_hr_data.sql
    17_seed_operations.sql
    18_seed_finance.sql
    19_seed_energy_esg.sql
    20_seed_executive_kpis.sql
  
  /queries                    # Sample queries for demos
    uc1_capacity_planning.sql
    uc2_esg_reporting.sql
    uc3_digital_twin.sql
    uc4_capex_lifecycle.sql
    executive_dashboard.sql
```

## Use Cases

### UC1: Resource & Capacity Planning

**Business Challenge:** Manual monthly Excel-based capacity assessment taking 3 days, leading to delayed decisions.

**Solution:** Real-time automated forecasting for 18 months with dynamic scenario modeling.

**Key Tables:**
- `HR.EMPLOYEES`, `HR.SKILLS_MATRIX`, `HR.WORKFORCE_CAPACITY`
- `COMMERCIAL.DEMAND_FORECAST`, `COMMERCIAL.PROJECTS`
- `OPERATIONS.WORK_ORDERS`, `OPERATIONS.RESOURCE_ALLOCATION`

**Sample Query:**
```sql
SELECT * FROM ANALYTICS.VW_CAPACITY_VS_DEMAND WHERE YEAR_MONTH >= '2025-06-01';
```

### UC2: ESG Regulatory Reporting

**Business Challenge:** High-stakes regulatory reports (ESG, Carbon Footprint, H/F Equality) requiring manual aggregation with perfect data lineage for external audit.

**Solution:** Traced, auditable reporting engine with data traceability to source entries.

**Key Tables:**
- `FINANCE.ACCOUNTING_ENTRIES` (with full audit trail)
- `ENERGY.CONSUMPTION_READINGS`, `ENERGY.CARBON_EMISSIONS`
- `HR.DIVERSITY_METRICS`
- `ESG.REGULATORY_REPORTS`, `ESG.AUDIT_TRAIL`, `ESG.BOARD_SCORECARD`

**Sample Query:**
```sql
SELECT * FROM ESG.AUDIT_TRAIL WHERE REPORT_ID = 'RPT-2025-001';
```

### UC3: Infrastructure Data Mastery & Digital Twin

**Business Challenge:** Data coherence issues and lack of single source of truth for 2,000+ pylons limiting Digital Twin value.

**Solution:** Infrastructure Data Control Tower with real-time discrepancy detection.

**Key Tables:**
- `INFRASTRUCTURE.SITES`, `INFRASTRUCTURE.TOWERS`, `INFRASTRUCTURE.EQUIPMENT`
- `DIGITAL_TWIN.ASSET_MODELS`, `DIGITAL_TWIN.DISCREPANCY_LOG`
- `DIGITAL_TWIN.DATA_QUALITY_SCORES`

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

Pre-built views for executive dashboards:

| View | Purpose |
|------|---------|
| `VW_EXECUTIVE_KPIS` | Single-view KPI summary with traffic lights |
| `VW_REVENUE_EXECUTIVE` | Revenue by segment with YoY comparison |
| `VW_EBITDA_BY_BU` | EBITDA margin by business unit |
| `VW_RISK_DASHBOARD` | Consolidated risk view |
| `VW_CLIENT_CONCENTRATION` | Client revenue dependency analysis |
| `VW_INVESTMENT_SCENARIOS` | What-if analysis for CAPEX decisions |

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

## Mobile Operators (Clients)

| Operator | Revenue Share |
|----------|--------------|
| Orange | ~31% |
| SFR | ~27% |
| Bouygues Telecom | ~24% |
| Free Mobile | ~18% |

## Technical Notes

### Snowflake Features Used

- **Git Integration:** Native repository connection for CI/CD
- **Virtual Columns:** Computed columns (e.g., COLOCATION_RATE, AGE_YEARS)
- **Views:** Pre-built analytics views for dashboard consumption
- **Future Grants:** Automatic permissions for new objects

### Data Quality

- ~3% intentional discrepancies for Digital Twin demo
- Seasonal energy patterns (winter +30%, summer +15%)
- Realistic equipment age distribution (1-15 years)

## License

Proprietary - TDF Infrastructure demonstration database

## Contact

For questions about this demo database, contact your TDF/Snowflake account team.

