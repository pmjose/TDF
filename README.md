# TDF Data Platform

A comprehensive Snowflake data platform with executive dashboards for TDF Infrastructure, France's leading telecommunications infrastructure company.

## 🎯 Overview

**TDF (Télédiffusion de France)** is a premier French telecommunications infrastructure company with:

| Metric | Value |
|--------|-------|
| 📡 Active Sites | **8,785** |
| 🗼 Towers | **7,877** |
| 👥 Employees | **1,850** |
| 💰 Annual Revenue | **€799.1M** |
| 📊 EBITDAaL Margin | **42-53%** |
| 🏦 Credit Rating | **BBB- (Fitch, Stable)** |

## 🖥️ Streamlit Executive Dashboard

Interactive dashboards built with Streamlit in Snowflake (SiS):

### 📊 Dashboard Pages

| Page | Description |
|------|-------------|
| **Executive Dashboard** | C-level KPIs, France map, risk radar, client health |
| **Resource & Capacity Planning** | 18-month forecasts, scenario simulator, skill gaps |
| **ESG Regulatory Reporting** | CSRD, EU Taxonomy, French compliance, report downloads |
| **Digital Twin** | 3D tower visualization, data quality, photo reconciliation |
| **CAPEX & Lifecycle** | 7-year renewal model, TCO calculator, site economics |
| **Architecture** | ERD diagrams, ETL pipelines, data lineage |

### 🎨 Features

- **TDF Branding:** Navy blue (#1a2b4a), TDF red (#e63946), clean design
- **Interactive Maps:** PyDeck-powered France infrastructure map
- **3D Visualizations:** Plotly 3D tower models with tenant configurations
- **Graphviz Diagrams:** System architecture, ERD, data lineage
- **Real-time Data:** Live queries to Snowflake database
- **Scenario Simulators:** What-if analysis for CAPEX, capacity, ESG

## 📁 Project Structure

```
/TDF
├── README.md
├── /streamlit
│   ├── streamlit_app.py          # Main dashboard application (8,700+ lines)
│   └── environment.yml           # Dependencies (plotly, pandas, pydeck)
│
└── /sql
    ├── 00_MASTER_DEPLOY.sql      # Orchestrator script
    ├── 00_GIT_SETUP.sql          # Git integration setup
    │
    ├── /ddl                      # Schema definitions (12 files)
    │   ├── 01_database_setup.sql # Database, schemas, roles, warehouse
    │   ├── 02_core_tables.sql    # Regions, departments, operators
    │   ├── 03_infrastructure_tables.sql
    │   ├── 04_hr_tables.sql
    │   ├── 05_commercial_tables.sql
    │   ├── 06_operations_tables.sql
    │   ├── 07_finance_tables.sql
    │   ├── 08_energy_tables.sql
    │   ├── 09_esg_tables.sql
    │   ├── 10_digital_twin_tables.sql
    │   ├── 11_analytics_views.sql
    │   └── 12_executive_views.sql
    │
    ├── /data                     # Seed data (10 files)
    │   ├── 13_seed_france_geo.sql
    │   ├── 14_seed_operators.sql
    │   ├── 15_seed_infrastructure.sql
    │   ├── 16_seed_hr_data.sql
    │   ├── 17_seed_operations.sql
    │   ├── 18_seed_finance.sql
    │   ├── 19_seed_energy_esg.sql
    │   ├── 20_seed_executive_kpis.sql
    │   ├── 21_seed_commercial.sql
    │   └── 22_seed_infrastructure_detail.sql
    │
    └── /queries                  # Sample queries
        ├── uc1_capacity_planning.sql
        ├── uc2_esg_reporting.sql
        ├── uc3_digital_twin.sql
        ├── uc4_capex_lifecycle.sql
        ├── executive_dashboard.sql
        └── esg_regulatory_reports.sql
```

## 🗄️ Database Architecture

### Schemas

| Schema | Tables | Purpose |
|--------|--------|---------|
| `CORE` | 6 | Regions, departments, business units, operators |
| `INFRASTRUCTURE` | 11 | Sites, towers, equipment, antennas, fibre |
| `HR` | 5 | Employees, skills, workforce capacity |
| `COMMERCIAL` | 4 | Contracts, demand forecast, projects |
| `OPERATIONS` | 5 | Work orders, maintenance, SLA tracking |
| `FINANCE` | 5 | CAPEX, OPEX, revenue, budgets |
| `ENERGY` | 3 | Power consumption, renewable sources |
| `ESG` | 4 | Emissions, sustainability, compliance |
| `DIGITAL_TWIN` | 3 | 3D models, discrepancies, data quality |
| `ANALYTICS` | 15 | Pre-built executive views |

### Data Volume

| Table | Records | Description |
|-------|---------|-------------|
| SITES | 8,785 | All TDF infrastructure sites |
| TOWERS | 7,877 | Tower structures |
| EQUIPMENT | 45,892 | All equipment inventory |
| ANTENNAS | ~25,000 | Antenna installations |
| EMPLOYEES | 1,850 | TDF workforce |
| WORK_ORDERS | 15,000+ | Maintenance work orders |
| **Total** | **145,000+** | Across all tables |

## 🚀 Deployment

### Step 1: Deploy Database

```sql
-- Option A: Git Integration (recommended)
ALTER GIT REPOSITORY TDF_DATA_PLATFORM.PUBLIC.TDF_REPO FETCH;
EXECUTE IMMEDIATE FROM @TDF_DATA_PLATFORM.PUBLIC.TDF_REPO/branches/main/sql/00_MASTER_DEPLOY.sql;

-- Option B: Manual (run files in order)
-- 01_database_setup.sql through 22_seed_infrastructure_detail.sql
```

### Step 2: Deploy Streamlit App

1. In Snowflake, go to **Streamlit** → **Create App**
2. Select database `TDF_DATA_PLATFORM`
3. Upload `streamlit_app.py` and `environment.yml` from `/streamlit`
4. Run the app

## 📊 Use Cases

### UC1: Resource & Capacity Planning
- 18-month workforce forecasting
- Skill gap analysis by region
- Build vs Buy scenario simulator
- Attrition risk radar

### UC2: ESG Regulatory Reporting
- CSRD, EU Taxonomy compliance
- French regulations (Index Égalité H/F, Bilan GES, DPEF)
- Net Zero 2030-2050 pathway
- Report generation & download

### UC3: Digital Twin
- 3D tower visualization with tenants
- Photo-to-Product reconciliation
- Data quality scoring
- What-if scenario analysis

### UC4: CAPEX & Equipment Lifecycle
- 7-year predictive renewal model
- Total Cost of Ownership calculator
- Site-level economics (P&L per site)
- Vendor risk & obsolescence tracking

## 🔐 Security

### Roles

| Role | Access Level |
|------|-------------|
| `TDF_ADMIN` | Full database administration |
| `TDF_ANALYST` | Read access to all schemas |
| `TDF_ENGINEER` | Infrastructure & operations |
| `TDF_EXECUTIVE` | Executive dashboards only |

## 📅 Data Period

- **Historical Data:** June 1, 2025 - December 19, 2025
- **Forecast Horizon:** Up to June 2027 (18 months)

## 🇫🇷 France Coverage

- **13 Metropolitan Regions**
- **96 Departments** (01-95, 2A, 2B)
- **GPS Coordinates:** Lat 42.0-51.5, Lon -5.0 to 9.5

### Key Locations

| Site | Purpose |
|------|---------|
| Fort de Romainville (93) | Network Operations Center |
| Toulouse (31) | Tower manufacturing |
| Tour Eiffel (75) | Major broadcast site |
| Pic du Midi (65) | High-altitude broadcast |

## 📱 Mobile Operators (Clients)

| Operator | Revenue Share |
|----------|--------------|
| Orange | ~31% |
| SFR | ~27% |
| Bouygues Telecom | ~24% |
| Free Mobile | ~18% |

## ✅ Verified Data Sources

All metrics verified against TDF public disclosures:
- ✅ Sites/Towers: TDF 2025 Investor Presentation
- ✅ Revenue: 2024 Annual Report
- ✅ Credit Rating: Fitch Ratings (BBB- Stable)
- ✅ Employee count: TDF corporate website

## 🛠️ Technical Stack

- **Database:** Snowflake
- **Dashboard:** Streamlit in Snowflake (SiS)
- **Visualizations:** Plotly, PyDeck, Graphviz
- **Deployment:** Snowflake Git Integration

## 📞 Contact

For questions about this demo, contact your TDF/Snowflake account team.

---

**TDF Data Platform v1.0** | Powered by Snowflake ❄️
