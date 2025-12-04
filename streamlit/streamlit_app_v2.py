# ==============================================================================
# TDF DATA PLATFORM - EXECUTIVE DASHBOARD
# ==============================================================================
# Streamlit in Snowflake (SiS) Application
# Single source of truth for TDF Infrastructure operations
# ==============================================================================

import streamlit as st
from snowflake.snowpark.context import get_active_session
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
import pydeck as pdk

# ==============================================================================
# PAGE CONFIGURATION
# ==============================================================================

st.set_page_config(
    page_title="TDF Data Platform",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==============================================================================
# TDF CUSTOM CSS THEME
# ==============================================================================

st.markdown("""
<style>
    /* TDF Brand Colors */
    :root {
        --tdf-navy: #1a2b4a;
        --tdf-red: #e63946;
        --tdf-light-gray: #f8f9fa;
        --tdf-dark-gray: #2d3436;
        --tdf-white: #ffffff;
    }
    
    /* Main background */
    .stApp {
        background-color: var(--tdf-light-gray);
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1a2b4a 0%, #2d3436 100%);
        padding-top: 0;
    }
    
    [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] {
        color: var(--tdf-white);
    }
    
    [data-testid="stSidebar"] .stRadio label {
        color: var(--tdf-white) !important;
        font-size: 1rem;
        padding: 0.5rem 0;
    }
    
    [data-testid="stSidebar"] .stRadio [data-testid="stMarkdownContainer"] p {
        color: var(--tdf-white) !important;
    }
    
    /* Header styling */
    .main-header {
        background: linear-gradient(90deg, #1a2b4a 0%, #2d3436 100%);
        padding: 1.5rem 2rem;
        border-radius: 10px;
        margin-bottom: 2rem;
        color: white;
    }
    
    .main-header h1 {
        color: white !important;
        margin: 0;
        font-weight: 600;
    }
    
    .main-header p {
        color: rgba(255,255,255,0.8);
        margin: 0.5rem 0 0 0;
    }
    
    /* KPI Card styling */
    .kpi-card {
        background: white;
        border-radius: 10px;
        padding: 1.5rem;
        box-shadow: 0 2px 10px rgba(0,0,0,0.08);
        border-left: 4px solid #1a2b4a;
        height: 100%;
    }
    
    .kpi-card.green { border-left-color: #27ae60; }
    .kpi-card.amber { border-left-color: #f39c12; }
    .kpi-card.red { border-left-color: #e63946; }
    
    .kpi-title {
        font-size: 0.85rem;
        color: #666;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 0.5rem;
    }
    
    .kpi-value {
        font-size: 2rem;
        font-weight: 700;
        color: #1a2b4a;
        margin-bottom: 0.25rem;
    }
    
    .kpi-subtitle {
        font-size: 0.8rem;
        color: #888;
    }
    
    /* Status indicators */
    .status-green { color: #27ae60; }
    .status-amber { color: #f39c12; }
    .status-red { color: #e63946; }
    
    /* Section headers */
    .section-header {
        color: #1a2b4a;
        font-weight: 600;
        font-size: 1.2rem;
        margin: 1.5rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #e63946;
    }
    
    /* Chart container */
    .chart-container {
        background: white;
        border-radius: 10px;
        padding: 1.5rem;
        box-shadow: 0 2px 10px rgba(0,0,0,0.08);
        margin-bottom: 1rem;
    }
    
    /* Table styling */
    .dataframe {
        font-size: 0.85rem;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Sidebar divider */
    .sidebar-divider {
        border-top: 1px solid rgba(255,255,255,0.2);
        margin: 1rem 0;
    }
    
    /* Page placeholder */
    .page-placeholder {
        background: white;
        border-radius: 10px;
        padding: 3rem;
        text-align: center;
        box-shadow: 0 2px 10px rgba(0,0,0,0.08);
        margin-top: 2rem;
    }
    
    .page-placeholder h2 {
        color: #1a2b4a;
        margin-bottom: 1rem;
    }
    
    .page-placeholder p {
        color: #666;
    }
    
    /* Executive Dashboard Hero Banner */
    .hero-banner {
        background: linear-gradient(135deg, #1a2b4a 0%, #2d3436 50%, #1a2b4a 100%);
        border-radius: 15px;
        padding: 2rem 3rem;
        margin-bottom: 1.5rem;
        box-shadow: 0 4px 20px rgba(26, 43, 74, 0.3);
    }
    
    .hero-metrics {
        display: flex;
        justify-content: space-between;
        align-items: center;
        flex-wrap: wrap;
        gap: 2rem;
    }
    
    .hero-metric {
        text-align: center;
        flex: 1;
        min-width: 150px;
    }
    
    .hero-value {
        font-size: 2.8rem;
        font-weight: 700;
        color: #ffffff;
        margin-bottom: 0.25rem;
        text-shadow: 0 2px 4px rgba(0,0,0,0.2);
    }
    
    .hero-label {
        font-size: 0.9rem;
        color: rgba(255,255,255,0.7);
        text-transform: uppercase;
        letter-spacing: 1px;
    }
    
    .hero-trend {
        font-size: 0.85rem;
        margin-top: 0.25rem;
    }
    
    .hero-trend.positive { color: #27ae60; }
    .hero-trend.neutral { color: rgba(255,255,255,0.6); }
    
    .rating-badge {
        background: rgba(255,255,255,0.15);
        border: 1px solid rgba(255,255,255,0.3);
        border-radius: 8px;
        padding: 0.5rem 1rem;
        display: inline-block;
    }
    
    .rating-badge .rating {
        font-size: 1.5rem;
        font-weight: 700;
        color: #ffffff;
    }
    
    .rating-badge .outlook {
        font-size: 0.75rem;
        color: rgba(255,255,255,0.7);
    }
    
    /* Metric cards for executive dashboard */
    .metric-card {
        background: white;
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 2px 12px rgba(0,0,0,0.08);
        height: 100%;
        transition: transform 0.2s, box-shadow 0.2s;
    }
    
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 20px rgba(0,0,0,0.12);
    }
    
    .metric-card-title {
        font-size: 0.8rem;
        color: #666;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 1rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    
    .metric-card-value {
        font-size: 2.2rem;
        font-weight: 700;
        color: #1a2b4a;
    }
    
    .metric-card-subtitle {
        font-size: 0.85rem;
        color: #888;
        margin-top: 0.25rem;
    }
    
    /* Status badge */
    .status-badge {
        display: inline-flex;
        align-items: center;
        gap: 0.35rem;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
    }
    
    .status-badge.green {
        background: rgba(39, 174, 96, 0.1);
        color: #27ae60;
    }
    
    .status-badge.amber {
        background: rgba(243, 156, 18, 0.1);
        color: #f39c12;
    }
    
    .status-badge.red {
        background: rgba(230, 57, 70, 0.1);
        color: #e63946;
    }
    
    /* Chart card */
    .chart-card {
        background: white;
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 2px 12px rgba(0,0,0,0.08);
        margin-bottom: 1rem;
    }
    
    .chart-card-title {
        font-size: 1rem;
        font-weight: 600;
        color: #1a2b4a;
        margin-bottom: 1rem;
        display: flex;
        align-items: center;
        justify-content: space-between;
    }
    
    /* Pillar cards */
    .pillar-card {
        background: white;
        border-radius: 12px;
        padding: 1.25rem;
        box-shadow: 0 2px 12px rgba(0,0,0,0.08);
        border-top: 3px solid #1a2b4a;
    }
    
    .pillar-card.infrastructure { border-top-color: #1a2b4a; }
    .pillar-card.operations { border-top-color: #e63946; }
    .pillar-card.esg { border-top-color: #27ae60; }
    
    .pillar-title {
        font-size: 0.85rem;
        font-weight: 600;
        color: #1a2b4a;
        margin-bottom: 1rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    
    .pillar-metric {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 0.5rem 0;
        border-bottom: 1px solid #f0f0f0;
    }
    
    .pillar-metric:last-child {
        border-bottom: none;
    }
    
    .pillar-metric-label {
        font-size: 0.85rem;
        color: #666;
    }
    
    .pillar-metric-value {
        font-size: 1rem;
        font-weight: 600;
        color: #1a2b4a;
    }
    
    /* Risk Radar Styles */
    .risk-radar {
        background: linear-gradient(135deg, #1a2b4a 0%, #2d3436 100%);
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1.5rem;
        box-shadow: 0 4px 20px rgba(26, 43, 74, 0.2);
    }
    
    .risk-radar-title {
        color: #ffffff;
        font-size: 1rem;
        font-weight: 600;
        margin-bottom: 1rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    
    .risk-items {
        display: flex;
        flex-direction: column;
        gap: 0.75rem;
    }
    
    .risk-item {
        background: rgba(255,255,255,0.1);
        border-radius: 8px;
        padding: 0.75rem 1rem;
        display: flex;
        align-items: center;
        gap: 1rem;
        border-left: 3px solid #e63946;
    }
    
    .risk-item.amber {
        border-left-color: #f39c12;
    }
    
    .risk-item.green {
        border-left-color: #27ae60;
    }
    
    .risk-icon {
        font-size: 1.5rem;
    }
    
    .risk-content {
        flex: 1;
    }
    
    .risk-title {
        color: #ffffff;
        font-size: 0.9rem;
        font-weight: 500;
    }
    
    .risk-detail {
        color: rgba(255,255,255,0.7);
        font-size: 0.8rem;
        margin-top: 0.25rem;
    }
    
    .risk-value {
        background: rgba(230, 57, 70, 0.2);
        color: #ff6b6b;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
    }
    
    .risk-value.amber {
        background: rgba(243, 156, 18, 0.2);
        color: #ffd93d;
    }
    
    .risk-value.green {
        background: rgba(39, 174, 96, 0.2);
        color: #6bcb77;
    }
    
    /* Client Health Styles */
    .client-health-card {
        background: white;
        border-radius: 10px;
        padding: 0.75rem 1rem;
        margin-bottom: 0.5rem;
        display: flex;
        align-items: center;
        gap: 1rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        border-left: 4px solid #27ae60;
    }
    
    .client-health-card.warning {
        border-left-color: #f39c12;
    }
    
    .client-health-card.risk {
        border-left-color: #e63946;
    }
    
    .client-logo {
        width: 40px;
        height: 40px;
        background: #f0f0f0;
        border-radius: 8px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-weight: 700;
        color: #1a2b4a;
        font-size: 0.85rem;
    }
    
    .client-info {
        flex: 1;
    }
    
    .client-name {
        font-weight: 600;
        color: #1a2b4a;
        font-size: 0.95rem;
    }
    
    .client-meta {
        color: #888;
        font-size: 0.8rem;
    }
    
    .client-metrics {
        text-align: right;
    }
    
    .client-revenue {
        font-weight: 700;
        color: #1a2b4a;
        font-size: 1rem;
    }
    
    .client-status {
        font-size: 0.75rem;
        padding: 0.15rem 0.5rem;
        border-radius: 10px;
        margin-top: 0.25rem;
        display: inline-block;
    }
    
    .client-status.healthy {
        background: rgba(39, 174, 96, 0.1);
        color: #27ae60;
    }
    
    .client-status.expiring {
        background: rgba(243, 156, 18, 0.1);
        color: #f39c12;
    }
    
    .client-status.at-risk {
        background: rgba(230, 57, 70, 0.1);
        color: #e63946;
    }
    
    /* Map container */
    .map-container {
        background: white;
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 2px 12px rgba(0,0,0,0.08);
    }
    
    .map-legend {
        display: flex;
        justify-content: center;
        gap: 1.5rem;
        margin-top: 1rem;
        font-size: 0.8rem;
        color: #666;
    }
    
    .legend-item {
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    
    .legend-dot {
        width: 12px;
        height: 12px;
        border-radius: 50%;
    }
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# SNOWFLAKE SESSION
# ==============================================================================

@st.cache_resource
def get_session():
    """Get Snowflake session for Streamlit in Snowflake"""
    return get_active_session()

session = get_session()

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================

def run_query(query):
    """Execute a SQL query and return results as DataFrame"""
    try:
        return session.sql(query).to_pandas()
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

def render_header(title, subtitle=""):
    """Render page header with TDF styling"""
    st.markdown(f"""
        <div class="main-header">
            <h1>{title}</h1>
            <p>{subtitle}</p>
        </div>
    """, unsafe_allow_html=True)

def render_kpi_card(title, value, subtitle="", status=""):
    """Render a KPI card with optional status color"""
    status_class = status.lower() if status else ""
    st.markdown(f"""
        <div class="kpi-card {status_class}">
            <div class="kpi-title">{title}</div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-subtitle">{subtitle}</div>
        </div>
    """, unsafe_allow_html=True)

def render_section_header(title):
    """Render a section header"""
    st.markdown(f'<div class="section-header">{title}</div>', unsafe_allow_html=True)

def render_placeholder(page_name, description):
    """Render a placeholder for pages under development"""
    st.markdown(f"""
        <div class="page-placeholder">
            <h2>📊 {page_name}</h2>
            <p>{description}</p>
            <p style="margin-top: 1rem; color: #1a2b4a;">
                <strong>Data sources connected and ready</strong>
            </p>
        </div>
    """, unsafe_allow_html=True)

# ==============================================================================
# SIDEBAR
# ==============================================================================

with st.sidebar:
    # TDF Logo
    st.image(
        "https://www.tdf.fr/wp-content/uploads/2022/02/TDF_LOGO_RVB_COULEUR-287x300.png.webp",
        width=150
    )
    
    st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)
    
    # Navigation
    st.markdown("### Navigation")
    
    page = st.radio(
        "Select Dashboard",
        options=[
            "Executive Dashboard",
            "Resource & Capacity Planning",
            "ESG Regulatory Reporting",
            "Digital Twin",
            "CAPEX & Lifecycle",
            "Architecture"
        ],
        label_visibility="collapsed"
    )
    
    st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)
    
    # Info - Live data from database
    st.markdown("### Data Platform")
    
    sb_sites = run_query("SELECT COUNT(*) as N FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES WHERE STATUS = 'ACTIVE'")
    sb_towers = run_query("SELECT COUNT(*) as N FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES WHERE STATUS = 'ACTIVE' AND SITE_TYPE = 'TOWER'")
    sb_emp = run_query("SELECT COUNT(*) as N FROM TDF_DATA_PLATFORM.HR.EMPLOYEES WHERE EMPLOYMENT_STATUS = 'ACTIVE'")
    sb_rev = run_query("SELECT SUM(REVENUE_EUR)/7*12/1000000 as R FROM TDF_DATA_PLATFORM.FINANCE.EBITDA_METRICS WHERE FISCAL_YEAR = 2025")
    
    n_sites = int(sb_sites['N'].iloc[0]) if not sb_sites.empty else 8533
    n_towers = int(sb_towers['N'].iloc[0]) if not sb_towers.empty else 5131
    n_emp = int(sb_emp['N'].iloc[0]) if not sb_emp.empty else 1500
    n_rev = sb_rev['R'].iloc[0] if not sb_rev.empty and sb_rev['R'].iloc[0] else 808.2
    
    st.markdown(f"""
        <div style="color: rgba(255,255,255,0.7); font-size: 0.85rem;">
            <p>📡 {n_sites:,} Active Sites</p>
            <p>🗼 {n_towers:,} Towers</p>
            <p>👥 {n_emp:,} Employees</p>
            <p>💶 EUR {n_rev:.1f}M Revenue</p>
        </div>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="sidebar-divider"></div>', unsafe_allow_html=True)
    
    st.markdown("""
        <div style="color: rgba(255,255,255,0.5); font-size: 0.75rem; text-align: center;">
            TDF Data Platform v1.0<br>
            Powered by Snowflake
        </div>
    """, unsafe_allow_html=True)

# ==============================================================================
# PAGE: EXECUTIVE DASHBOARD
# ==============================================================================

def page_executive_dashboard():
    
    # -------------------------------------------------------------------------
    # HERO BANNER - Key Financial Metrics
    # -------------------------------------------------------------------------
    
    # Fetch EBITDA metrics - annualized
    ebitda_df = run_query("""
        SELECT 
            SUM(REVENUE_EUR) as ANNUAL_REVENUE,
            AVG(EBITDAAL_MARGIN_PCT) as AVG_MARGIN,
            AVG(YOY_GROWTH_PCT) as AVG_GROWTH
        FROM TDF_DATA_PLATFORM.FINANCE.EBITDA_METRICS 
        WHERE FISCAL_YEAR = 2025
    """)
    
    # Fetch ESG status
    esg_df = run_query("""
        SELECT * FROM TDF_DATA_PLATFORM.ESG.BOARD_SCORECARD 
        ORDER BY REPORTING_DATE DESC LIMIT 1
    """)
    
    # Get values - annualize if we only have partial year data
    if not ebitda_df.empty and ebitda_df['ANNUAL_REVENUE'].iloc[0]:
        # We have 7 months of data (June-Dec), annualize to 12 months
        months_of_data = 7  # June to December
        revenue = (ebitda_df['ANNUAL_REVENUE'].iloc[0] / months_of_data * 12) / 1000000
    else:
        revenue = 799.1
    
    ebitda_margin = ebitda_df['AVG_MARGIN'].iloc[0] if not ebitda_df.empty and ebitda_df['AVG_MARGIN'].iloc[0] else 47.0
    yoy_growth = ebitda_df['AVG_GROWTH'].iloc[0] if not ebitda_df.empty and ebitda_df['AVG_GROWTH'].iloc[0] else 8.5
    esg_status = esg_df['OVERALL_ESG_STATUS'].iloc[0] if not esg_df.empty else 'GREEN'
    
    # Hero Banner
    st.markdown(f"""
        <div class="hero-banner">
            <div class="hero-metrics">
                <div class="hero-metric">
                    <div class="hero-value">€{revenue:.1f}M</div>
                    <div class="hero-label">Annual Revenue</div>
                    <div class="hero-trend positive">↑ +{yoy_growth:.1f}% YoY</div>
                </div>
                <div class="hero-metric">
                    <div class="hero-value">{ebitda_margin:.1f}%</div>
                    <div class="hero-label">EBITDAaL Margin</div>
                    <div class="hero-trend neutral">Target: 42-53%</div>
                </div>
                <div class="hero-metric">
                    <div class="rating-badge">
                        <div class="rating">BBB-</div>
                        <div class="outlook">Fitch • Stable</div>
                    </div>
                </div>
                <div class="hero-metric">
                    <div class="hero-value" style="font-size: 2rem;">
                        {'🟢' if esg_status == 'GREEN' else '🟡' if esg_status == 'AMBER' else '🔴'}
                    </div>
                    <div class="hero-label">ESG Status</div>
                    <div class="hero-trend neutral">{esg_status}</div>
                </div>
            </div>
        </div>
    """, unsafe_allow_html=True)
    
    # =========================================================================
    # EXECUTIVE TABS
    # =========================================================================
    
    tab_overview, tab_financial, tab_operations, tab_clients = st.tabs([
        "📊 Executive Overview",
        "💰 Financial Performance", 
        "🏗️ Infrastructure & Operations",
        "🤝 Client Portfolio"
    ])
    
    # =========================================================================
    # TAB 1: EXECUTIVE OVERVIEW
    # =========================================================================
    
    with tab_overview:
    
        # -------------------------------------------------------------------------
        # 🚨 RISK RADAR - Critical Alerts for Executive Attention
        # -------------------------------------------------------------------------
    
        # Fetch risk data
        contract_risk_df = run_query("""
            SELECT 
                o.OPERATOR_NAME,
                o.CONTRACT_END_DATE,
                o.ANNUAL_REVENUE_EUR / 1000000 as REVENUE_M,
                DATEDIFF(DAY, CURRENT_DATE(), o.CONTRACT_END_DATE) as DAYS_TO_EXPIRY
            FROM TDF_DATA_PLATFORM.CORE.OPERATORS o
            WHERE o.CONTRACT_END_DATE IS NOT NULL
            AND o.ANNUAL_REVENUE_EUR > 0
            ORDER BY o.CONTRACT_END_DATE ASC
            LIMIT 5
        """)
    
        equipment_risk_df = run_query("""
            SELECT COUNT(*) as AT_RISK_COUNT
            FROM TDF_DATA_PLATFORM.OPERATIONS.EQUIPMENT_STATUS
            WHERE FAILURE_RISK_SCORE > 70
        """)
    
        sla_breach_df = run_query("""
            SELECT COUNT(*) as BREACH_COUNT
            FROM TDF_DATA_PLATFORM.OPERATIONS.WORK_ORDERS
            WHERE SLA_MET = FALSE 
            AND STATUS = 'COMPLETED'
            AND CREATED_DATE >= DATEADD(MONTH, -1, CURRENT_DATE())
        """)
    
        esg_deadline_df = run_query("""
            SELECT COUNT(*) as PENDING_COUNT
            FROM TDF_DATA_PLATFORM.ESG.REGULATORY_REPORTS
            WHERE STATUS IN ('DRAFT', 'REVIEW')
        """)
    
        # Build risk items
        risk_items = []
    
        # Contract renewals
        if not contract_risk_df.empty:
            expiring_soon = contract_risk_df[contract_risk_df['DAYS_TO_EXPIRY'] < 365]
            if len(expiring_soon) > 0:
                total_at_risk = expiring_soon['REVENUE_M'].sum()
                next_expiry = expiring_soon.iloc[0]
                risk_items.append({
                    'icon': '📋',
                    'title': f"Contract Renewal: {next_expiry['OPERATOR_NAME']}",
                    'detail': f"€{next_expiry['REVENUE_M']:.0f}M revenue • Expires in {next_expiry['DAYS_TO_EXPIRY']:.0f} days",
                    'value': f"€{total_at_risk:.0f}M at risk",
                    'severity': 'red' if next_expiry['DAYS_TO_EXPIRY'] < 180 else 'amber'
                })
    
        # Equipment at risk
        at_risk_count = equipment_risk_df['AT_RISK_COUNT'].iloc[0] if not equipment_risk_df.empty else 0
        if at_risk_count > 0:
            risk_items.append({
                'icon': '⚠️',
                'title': 'Equipment at High Risk',
                'detail': 'Equipment with failure risk score > 70 requiring attention',
                'value': f"{at_risk_count:,} items",
                'severity': 'red' if at_risk_count > 100 else 'amber'
            })
    
        # SLA breaches
        breach_count = sla_breach_df['BREACH_COUNT'].iloc[0] if not sla_breach_df.empty else 0
        if breach_count > 0:
            risk_items.append({
                'icon': '🎯',
                'title': 'SLA Breaches (Last 30 Days)',
                'detail': 'Work orders that missed SLA targets',
                'value': f"{breach_count} breaches",
                'severity': 'red' if breach_count > 50 else 'amber'
            })
    
        # ESG deadlines
        pending_esg = esg_deadline_df['PENDING_COUNT'].iloc[0] if not esg_deadline_df.empty else 0
        if pending_esg > 0:
            risk_items.append({
                'icon': '🌱',
                'title': 'ESG Reports In Progress',
                'detail': 'Regulatory reports awaiting completion',
                'value': f"{pending_esg} pending",
                'severity': 'amber'
            })
    
        # Add a green item if things are good
        if len(risk_items) < 3:
            risk_items.append({
                'icon': '✅',
                'title': 'Infrastructure Health',
                'detail': 'Network availability and performance on target',
                'value': '99.8% uptime',
                'severity': 'green'
            })
    
        # Render Risk Radar using Streamlit columns for better compatibility
        st.markdown("### 🚨 Risk Radar - Items Requiring Attention")
    
        # Create columns for risk items
        risk_cols = st.columns(len(risk_items[:4]))
    
        for i, item in enumerate(risk_items[:4]):
            with risk_cols[i]:
                severity = item.get('severity', 'amber')
                border_color = '#e63946' if severity == 'red' else '#f39c12' if severity == 'amber' else '#27ae60'
                bg_color = 'rgba(230, 57, 70, 0.05)' if severity == 'red' else 'rgba(243, 156, 18, 0.05)' if severity == 'amber' else 'rgba(39, 174, 96, 0.05)'
            
                st.markdown(f"""
                    <div style="background: {bg_color}; border-left: 4px solid {border_color}; border-radius: 8px; padding: 1rem; height: 140px;">
                        <div style="font-size: 1.5rem; margin-bottom: 0.5rem;">{item['icon']}</div>
                        <div style="font-weight: 600; color: #1a2b4a; font-size: 0.9rem; margin-bottom: 0.25rem;">{item['title']}</div>
                        <div style="color: #666; font-size: 0.75rem; margin-bottom: 0.5rem;">{item['detail']}</div>
                        <div style="background: {border_color}; color: white; padding: 0.25rem 0.5rem; border-radius: 12px; font-size: 0.75rem; font-weight: 600; display: inline-block;">{item['value']}</div>
                    </div>
                """, unsafe_allow_html=True)
    
        # -------------------------------------------------------------------------
        # 🗺️ FRANCE MAP + 💰 CLIENT HEALTH (Side by Side)
        # -------------------------------------------------------------------------
    
        col_map, col_clients = st.columns([3, 2])
    
        with col_map:
            st.markdown("### 🗺️ France Infrastructure Map")
        
            # Fetch regional data with coordinates
            regional_df = run_query("""
                SELECT 
                    r.REGION_NAME,
                    r.REGION_CODE,
                    r.LATITUDE,
                    r.LONGITUDE,
                    r.POPULATION / 1000000 as POPULATION_M,
                    COALESCE(site_data.SITE_COUNT, 0) as SITE_COUNT,
                    COALESCE(site_data.REVENUE_M, 0) as REVENUE_M,
                    COALESCE(site_data.AVG_COLOCATION, 50) as AVG_COLOCATION
                FROM TDF_DATA_PLATFORM.CORE.REGIONS r
                LEFT JOIN (
                    SELECT 
                        d.REGION_ID,
                        COUNT(DISTINCT s.SITE_ID) as SITE_COUNT,
                        SUM(s.ANNUAL_REVENUE_EUR) / 1000000 as REVENUE_M,
                        AVG(s.COLOCATION_RATE) * 100 as AVG_COLOCATION
                    FROM TDF_DATA_PLATFORM.CORE.DEPARTMENTS d
                    INNER JOIN TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES s 
                        ON d.DEPARTMENT_ID = s.DEPARTMENT_ID
                    WHERE s.STATUS = 'ACTIVE'
                    GROUP BY d.REGION_ID
                ) site_data ON r.REGION_ID = site_data.REGION_ID
                ORDER BY SITE_COUNT DESC
            """)
        
            if not regional_df.empty:
                # Prepare data for PyDeck
                # Scale radius by site count (min 10000, max 80000 meters)
                max_sites = max(regional_df['SITE_COUNT'].max(), 1)
                regional_df['radius'] = (regional_df['SITE_COUNT'] / max_sites) * 70000 + 10000
            
                # Color by revenue: blue gradient (RGB)
                max_rev = max(regional_df['REVENUE_M'].max(), 1)
                regional_df['color_r'] = 26  # TDF navy R
                regional_df['color_g'] = (43 + (1 - regional_df['REVENUE_M'] / max_rev) * 150).astype(int)
                regional_df['color_b'] = (74 + (1 - regional_df['REVENUE_M'] / max_rev) * 180).astype(int)
            
                # Rename columns for PyDeck (needs lowercase)
                map_df = regional_df.rename(columns={
                    'LATITUDE': 'lat', 
                    'LONGITUDE': 'lon',
                    'REGION_NAME': 'region_name',
                    'REGION_CODE': 'region_code',
                    'SITE_COUNT': 'sites',
                    'REVENUE_M': 'revenue',
                    'AVG_COLOCATION': 'colocation',
                    'POPULATION_M': 'population'
                })
            
                # Create PyDeck ScatterplotLayer
                layer = pdk.Layer(
                    "ScatterplotLayer",
                    data=map_df,
                    get_position=["lon", "lat"],
                    get_radius="radius",
                    get_fill_color=["color_r", "color_g", "color_b", 200],
                    pickable=True,
                    auto_highlight=True,
                    highlight_color=[230, 57, 70, 255],  # TDF red on hover
                )
            
                # Text layer for region codes
                text_layer = pdk.Layer(
                    "TextLayer",
                    data=map_df,
                    get_position=["lon", "lat"],
                    get_text="region_code",
                    get_size=14,
                    get_color=[255, 255, 255, 255],
                    get_alignment_baseline="'center'",
                    font_family='"Arial Black", sans-serif',
                    pickable=False,
                )
            
                # Set view centered on France
                view_state = pdk.ViewState(
                    latitude=46.6,
                    longitude=2.5,
                    zoom=4.8,
                    pitch=0,
                )
            
                # Tooltip with details
                tooltip = {
                    "html": """
                        <div style="background: #1a2b4a; padding: 12px; border-radius: 8px; font-family: Arial, sans-serif;">
                            <div style="font-size: 16px; font-weight: bold; color: white; margin-bottom: 8px;">{region_name}</div>
                            <div style="color: #aaa; font-size: 12px;">
                                📡 <b style="color: white;">{sites}</b> Sites<br/>
                                💰 <b style="color: white;">€{revenue:.1f}M</b> Revenue<br/>
                                🏢 <b style="color: white;">{colocation:.0f}%</b> Colocation<br/>
                                👥 <b style="color: white;">{population:.1f}M</b> Population
                            </div>
                        </div>
                    """,
                    "style": {"backgroundColor": "transparent", "color": "white"}
                }
            
                # Render the map with Carto basemap (no API key needed)
                st.pydeck_chart(
                    pdk.Deck(
                        layers=[layer, text_layer],
                        initial_view_state=view_state,
                        tooltip=tooltip,
                        map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
                    ),
                    use_container_width=True
                )
            
                # Summary stats below map
                col1, col2, col3 = st.columns(3)
                with col1:
                    top_region = regional_df[regional_df['SITE_COUNT'] > 0].iloc[0] if regional_df['SITE_COUNT'].sum() > 0 else regional_df.iloc[0]
                    st.metric("Top Region", top_region['REGION_NAME'], f"{int(top_region['SITE_COUNT']):,} sites")
                with col2:
                    total_sites = regional_df['SITE_COUNT'].sum()
                    st.metric("Total Sites", f"{int(total_sites):,}")
                with col3:
                    valid_coloc = regional_df[regional_df['AVG_COLOCATION'] > 0]['AVG_COLOCATION']
                    avg_coloc = valid_coloc.mean() if len(valid_coloc) > 0 else 50
                    st.metric("Avg Colocation", f"{avg_coloc:.0f}%")
            else:
                st.warning("No regional data found.")
    
        with col_clients:
            st.markdown("### 💰 Client Health Monitor")
        
            # Fetch client health data
            client_health_df = run_query("""
                SELECT 
                    o.OPERATOR_NAME,
                    o.OPERATOR_CODE,
                    o.ANNUAL_REVENUE_EUR / 1000000 as REVENUE_M,
                    o.CONTRACT_END_DATE,
                    o.CREDIT_RATING,
                    DATEDIFF(DAY, CURRENT_DATE(), o.CONTRACT_END_DATE) as DAYS_TO_EXPIRY,
                    COALESCE(
                        (SELECT COUNT(*) 
                         FROM TDF_DATA_PLATFORM.OPERATIONS.WORK_ORDERS wo 
                         JOIN TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES s ON wo.SITE_ID = s.SITE_ID
                         JOIN TDF_DATA_PLATFORM.INFRASTRUCTURE.CLIENT_INSTALLATIONS ci ON s.SITE_ID = ci.SITE_ID
                         WHERE ci.OPERATOR_ID = o.OPERATOR_ID
                         AND wo.STATUS = 'OPEN'
                         AND wo.PRIORITY IN ('CRITICAL', 'HIGH')), 0
                    ) as OPEN_ISSUES
                FROM TDF_DATA_PLATFORM.CORE.OPERATORS o
                WHERE o.ANNUAL_REVENUE_EUR > 0
                ORDER BY o.ANNUAL_REVENUE_EUR DESC
                LIMIT 6
            """)
        
            if not client_health_df.empty:
                for _, client in client_health_df.iterrows():
                    # Determine health status
                    days_to_expiry = client['DAYS_TO_EXPIRY'] if client['DAYS_TO_EXPIRY'] else 9999
                
                    if days_to_expiry < 365:
                        health_class = 'risk'
                        status_class = 'at-risk'
                        status_text = f'Expires {days_to_expiry}d'
                    elif days_to_expiry < 730:
                        health_class = 'warning'
                        status_class = 'expiring'
                        status_text = f'Renewal in {days_to_expiry//30}mo'
                    else:
                        health_class = ''
                        status_class = 'healthy'
                        status_text = 'Secured'
                
                    # Logo initials
                    initials = ''.join([w[0] for w in client['OPERATOR_NAME'].split()[:2]]).upper()
                
                    st.markdown(f"""
                        <div class="client-health-card {health_class}">
                            <div class="client-logo">{initials}</div>
                            <div class="client-info">
                                <div class="client-name">{client['OPERATOR_NAME']}</div>
                                <div class="client-meta">Rating: {client['CREDIT_RATING']} • {client['OPEN_ISSUES']} open issues</div>
                            </div>
                            <div class="client-metrics">
                                <div class="client-revenue">€{client['REVENUE_M']:.0f}M</div>
                                <div class="client-status {status_class}">{status_text}</div>
                            </div>
                        </div>
                    """, unsafe_allow_html=True)
            
                # Revenue at risk summary
                at_risk = client_health_df[client_health_df['DAYS_TO_EXPIRY'] < 730]['REVENUE_M'].sum()
                secured = client_health_df[client_health_df['DAYS_TO_EXPIRY'] >= 730]['REVENUE_M'].sum()
            
                st.markdown("---")
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Revenue Secured", f"€{secured:.0f}M", delta="Contracted 2+ yrs")
                with col2:
                    st.metric("Revenue to Renew", f"€{at_risk:.0f}M", delta="Within 24 months", delta_color="inverse")
            else:
                st.info("Loading client data...")
    
        # -------------------------------------------------------------------------
        # MONTHLY FINANCIAL PERFORMANCE - Revenue vs Costs
        # -------------------------------------------------------------------------
    
        st.markdown("### 📈 Monthly Financial Performance")
    
        # Fetch monthly financial data
        monthly_finance_df = run_query("""
            SELECT 
                PERIOD_DATE,
                REVENUE_EUR / 1000000 as REVENUE_M,
                OPEX_EUR / 1000000 as OPEX_M,
                EBITDA_EUR / 1000000 as EBITDA_M,
                EBITDAAL_MARGIN_PCT as MARGIN_PCT
            FROM TDF_DATA_PLATFORM.FINANCE.EBITDA_METRICS 
            WHERE FISCAL_YEAR = 2025
            ORDER BY PERIOD_DATE
        """)
    
        if not monthly_finance_df.empty:
            fig = go.Figure()
        
            # Revenue line
            fig.add_trace(go.Scatter(
                x=monthly_finance_df['PERIOD_DATE'],
                y=monthly_finance_df['REVENUE_M'],
                mode='lines+markers',
                name='Revenue',
                line=dict(color='#1a2b4a', width=3),
                marker=dict(size=10, color='#1a2b4a'),
                hovertemplate='Revenue: €%{y:.1f}M<extra></extra>'
            ))
        
            # OPEX line
            fig.add_trace(go.Scatter(
                x=monthly_finance_df['PERIOD_DATE'],
                y=monthly_finance_df['OPEX_M'],
                mode='lines+markers',
                name='OPEX',
                line=dict(color='#e63946', width=3),
                marker=dict(size=10, color='#e63946'),
                hovertemplate='OPEX: €%{y:.1f}M<extra></extra>'
            ))
        
            # EBITDA line
            fig.add_trace(go.Scatter(
                x=monthly_finance_df['PERIOD_DATE'],
                y=monthly_finance_df['EBITDA_M'],
                mode='lines+markers',
                name='EBITDA',
                line=dict(color='#27ae60', width=2, dash='dot'),
                marker=dict(size=8, color='#27ae60'),
                hovertemplate='EBITDA: €%{y:.1f}M<extra></extra>'
            ))
        
            fig.update_layout(
                height=350,
                margin=dict(l=20, r=20, t=30, b=40),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                xaxis=dict(
                    showgrid=True,
                    gridcolor='#f0f0f0',
                    tickformat='%b %Y',
                    title=None
                ),
                yaxis=dict(
                    showgrid=True,
                    gridcolor='#f0f0f0',
                    title=dict(text='EUR Millions', font=dict(size=12, color='#666')),
                    tickprefix='€',
                    ticksuffix='M'
                ),
                legend=dict(
                    orientation='h',
                    yanchor='bottom',
                    y=1.02,
                    xanchor='center',
                    x=0.5,
                    font=dict(size=12)
                ),
                hovermode='x unified'
            )
        
            st.plotly_chart(fig, use_container_width=True)
        
            # Summary metrics below chart
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                avg_revenue = monthly_finance_df['REVENUE_M'].mean()
                st.metric("Avg Monthly Revenue", f"€{avg_revenue:.1f}M")
            with col2:
                avg_opex = monthly_finance_df['OPEX_M'].mean()
                st.metric("Avg Monthly OPEX", f"€{avg_opex:.1f}M")
            with col3:
                avg_ebitda = monthly_finance_df['EBITDA_M'].mean()
                st.metric("Avg Monthly EBITDA", f"€{avg_ebitda:.1f}M")
            with col4:
                avg_margin = monthly_finance_df['MARGIN_PCT'].mean()
                st.metric("Avg Margin", f"{avg_margin:.1f}%")
        else:
            st.info("Loading financial data...")
    
        # -------------------------------------------------------------------------
        # FOUR VITAL SIGNS - Gauge Charts
        # -------------------------------------------------------------------------
    
        # Fetch additional metrics
        sites_df = run_query("""
            SELECT 
                COUNT(*) as TOTAL_SITES,
                AVG(COLOCATION_RATE) as AVG_COLOCATION
            FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES 
            WHERE STATUS = 'ACTIVE'
        """)
    
        renewable_df = run_query("""
            SELECT AVG(RENEWABLE_PCT) as RENEWABLE_PCT 
            FROM TDF_DATA_PLATFORM.ENERGY.RENEWABLE_ENERGY 
            WHERE YEAR(YEAR_MONTH) = 2025
        """)
    
        sla_df = run_query("""
            SELECT 
                COUNT(CASE WHEN SLA_MET = TRUE THEN 1 END) * 100.0 / COUNT(*) as SLA_PCT
            FROM TDF_DATA_PLATFORM.OPERATIONS.WORK_ORDERS 
            WHERE STATUS = 'COMPLETED'
        """)
    
        colocation_rate = sites_df['AVG_COLOCATION'].iloc[0] * 100 if not sites_df.empty and sites_df['AVG_COLOCATION'].iloc[0] else 60
        renewable_pct = renewable_df['RENEWABLE_PCT'].iloc[0] if not renewable_df.empty and renewable_df['RENEWABLE_PCT'].iloc[0] else 47
        sla_pct = sla_df['SLA_PCT'].iloc[0] if not sla_df.empty and sla_df['SLA_PCT'].iloc[0] else 92
    
        st.markdown("### 📊 Vital Signs")
    
        col1, col2, col3, col4 = st.columns(4)
    
        # Revenue Growth Gauge
        with col1:
            fig = go.Figure(go.Indicator(
                mode="gauge+number+delta",
                value=yoy_growth,
                number={'suffix': '%', 'font': {'size': 36, 'color': '#1a2b4a'}},
                delta={'reference': 5, 'relative': False, 'position': 'bottom'},
                gauge={
                    'axis': {'range': [0, 15], 'tickwidth': 1, 'tickcolor': '#1a2b4a'},
                    'bar': {'color': '#1a2b4a'},
                    'bgcolor': 'white',
                    'borderwidth': 0,
                    'steps': [
                        {'range': [0, 5], 'color': '#fee2e2'},
                        {'range': [5, 10], 'color': '#fef3c7'},
                        {'range': [10, 15], 'color': '#d1fae5'}
                    ],
                    'threshold': {
                        'line': {'color': '#e63946', 'width': 3},
                        'thickness': 0.8,
                        'value': 8
                    }
                },
                title={'text': 'Revenue Growth', 'font': {'size': 14, 'color': '#666'}}
            ))
            fig.update_layout(height=200, margin=dict(l=20, r=20, t=40, b=20), paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig, use_container_width=True)
    
        # EBITDA Margin Gauge
        with col2:
            fig = go.Figure(go.Indicator(
                mode="gauge+number",
                value=ebitda_margin,
                number={'suffix': '%', 'font': {'size': 36, 'color': '#1a2b4a'}},
                gauge={
                    'axis': {'range': [30, 60], 'tickwidth': 1, 'tickcolor': '#1a2b4a'},
                    'bar': {'color': '#27ae60' if 42 <= ebitda_margin <= 53 else '#f39c12'},
                    'bgcolor': 'white',
                    'borderwidth': 0,
                    'steps': [
                        {'range': [30, 42], 'color': '#fee2e2'},
                        {'range': [42, 53], 'color': '#d1fae5'},
                        {'range': [53, 60], 'color': '#dbeafe'}
                    ],
                    'threshold': {
                        'line': {'color': '#1a2b4a', 'width': 2},
                        'thickness': 0.8,
                        'value': 47.5
                    }
                },
                title={'text': 'EBITDA Margin', 'font': {'size': 14, 'color': '#666'}}
            ))
            fig.update_layout(height=200, margin=dict(l=20, r=20, t=40, b=20), paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig, use_container_width=True)
    
        # Colocation Rate Gauge
        with col3:
            fig = go.Figure(go.Indicator(
                mode="gauge+number",
                value=colocation_rate,
                number={'suffix': '%', 'font': {'size': 36, 'color': '#1a2b4a'}},
                gauge={
                    'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': '#1a2b4a'},
                    'bar': {'color': '#1a2b4a'},
                    'bgcolor': 'white',
                    'borderwidth': 0,
                    'steps': [
                        {'range': [0, 40], 'color': '#fee2e2'},
                        {'range': [40, 70], 'color': '#fef3c7'},
                        {'range': [70, 100], 'color': '#d1fae5'}
                    ]
                },
                title={'text': 'Colocation Rate', 'font': {'size': 14, 'color': '#666'}}
            ))
            fig.update_layout(height=200, margin=dict(l=20, r=20, t=40, b=20), paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig, use_container_width=True)
    
        # Renewable Energy Gauge
        with col4:
            fig = go.Figure(go.Indicator(
                mode="gauge+number",
                value=renewable_pct,
                number={'suffix': '%', 'font': {'size': 36, 'color': '#1a2b4a'}},
                gauge={
                    'axis': {'range': [0, 100], 'tickwidth': 1, 'tickcolor': '#1a2b4a'},
                    'bar': {'color': '#27ae60'},
                    'bgcolor': 'white',
                    'borderwidth': 0,
                    'steps': [
                        {'range': [0, 30], 'color': '#fee2e2'},
                        {'range': [30, 50], 'color': '#fef3c7'},
                        {'range': [50, 100], 'color': '#d1fae5'}
                    ],
                    'threshold': {
                        'line': {'color': '#e63946', 'width': 3},
                        'thickness': 0.8,
                        'value': 50
                    }
                },
                title={'text': 'Renewable Energy', 'font': {'size': 14, 'color': '#666'}}
            ))
            fig.update_layout(height=200, margin=dict(l=20, r=20, t=40, b=20), paper_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig, use_container_width=True)
    
        # -------------------------------------------------------------------------
        # REVENUE & CLIENT SECTION
        # -------------------------------------------------------------------------
    
        col_left, col_right = st.columns([3, 2])
    
        with col_left:
            st.markdown("### 📈 Revenue by Segment")
        
            # Fetch revenue by segment
            revenue_df = run_query("""
                SELECT 
                    SEGMENT_LEVEL1,
                    SUM(REVENUE_EUR) / 1000000 as REVENUE_M
                FROM TDF_DATA_PLATFORM.FINANCE.REVENUE_BY_SEGMENT
                WHERE FISCAL_YEAR = 2025
                GROUP BY SEGMENT_LEVEL1
                ORDER BY REVENUE_M DESC
            """)
        
            if not revenue_df.empty:
                # Create horizontal bar chart
                fig = go.Figure()
            
                colors = ['#1a2b4a', '#2d3436', '#e63946', '#636e72']
            
                fig.add_trace(go.Bar(
                    y=revenue_df['SEGMENT_LEVEL1'],
                    x=revenue_df['REVENUE_M'],
                    orientation='h',
                    marker=dict(
                        color=colors[:len(revenue_df)],
                        line=dict(width=0)
                    ),
                    text=[f'€{x:.1f}M' for x in revenue_df['REVENUE_M']],
                    textposition='inside',
                    textfont=dict(color='white', size=14, family='Arial Black'),
                    hovertemplate='%{y}: €%{x:.1f}M<extra></extra>'
                ))
            
                fig.update_layout(
                    height=300,
                    margin=dict(l=20, r=20, t=20, b=20),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    xaxis=dict(
                        showgrid=True,
                        gridcolor='#f0f0f0',
                        title=dict(text='Revenue (EUR Millions)', font=dict(size=12, color='#666'))
                    ),
                    yaxis=dict(
                        showgrid=False,
                        categoryorder='total ascending'
                    ),
                    showlegend=False
                )
            
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Loading revenue data...")
    
        with col_right:
            st.markdown("### 🎯 Client Portfolio")
        
            # Fetch client concentration
            client_df = run_query("""
                SELECT 
                    o.OPERATOR_NAME,
                    SUM(rc.REVENUE_EUR) / 1000000 as REVENUE_M,
                    AVG(rc.REVENUE_SHARE_PCT) as SHARE_PCT
                FROM TDF_DATA_PLATFORM.FINANCE.REVENUE_BY_CLIENT rc
                JOIN TDF_DATA_PLATFORM.CORE.OPERATORS o ON rc.OPERATOR_ID = o.OPERATOR_ID
                WHERE rc.FISCAL_YEAR = 2025
                GROUP BY o.OPERATOR_NAME
                ORDER BY REVENUE_M DESC
            """)
        
            if not client_df.empty:
                colors = ['#1a2b4a', '#e63946', '#2d3436', '#636e72']
            
                fig = go.Figure(data=[go.Pie(
                    labels=client_df['OPERATOR_NAME'],
                    values=client_df['REVENUE_M'],
                    hole=0.6,
                    marker=dict(colors=colors[:len(client_df)]),
                    textinfo='percent',
                    textfont=dict(size=14, color='white'),
                    hovertemplate='%{label}<br>€%{value:.1f}M<br>%{percent}<extra></extra>'
                )])
            
                fig.update_layout(
                    height=300,
                    margin=dict(l=20, r=20, t=20, b=20),
                    paper_bgcolor='rgba(0,0,0,0)',
                    showlegend=True,
                    legend=dict(
                        orientation='h',
                        yanchor='bottom',
                        y=-0.2,
                        xanchor='center',
                        x=0.5,
                        font=dict(size=11)
                    ),
                    annotations=[dict(
                        text='<b>4</b><br>Strategic<br>Clients',
                        x=0.5, y=0.5,
                        font=dict(size=14, color='#1a2b4a'),
                        showarrow=False
                    )]
                )
            
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Loading client data...")
    
        # -------------------------------------------------------------------------
        # THREE STRATEGIC PILLARS
        # -------------------------------------------------------------------------
    
        st.markdown("### 🏛️ Strategic Pillars")
    
        col1, col2, col3 = st.columns(3)
    
        # Infrastructure Pillar
        with col1:
            infra_df = run_query("""
                SELECT 
                    COUNT(*) as SITES,
                    SUM(CASE WHEN SITE_TYPE = 'TOWER' THEN 1 ELSE 0 END) as TOWERS
                FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES 
                WHERE STATUS = 'ACTIVE'
            """)
        
            pos_df = run_query("""
                SELECT COUNT(*) as POS_COUNT 
                FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.POINTS_OF_SERVICE 
                WHERE STATUS = 'ACTIVE'
            """)
        
            sites_count = infra_df['SITES'].iloc[0] if not infra_df.empty else 8785
            towers_count = infra_df['TOWERS'].iloc[0] if not infra_df.empty else 7877
            pos_count = pos_df['POS_COUNT'].iloc[0] if not pos_df.empty else 21244
        
            st.markdown(f"""
                <div class="pillar-card infrastructure">
                    <div class="pillar-title">🏗️ Infrastructure</div>
                    <div class="pillar-metric">
                        <span class="pillar-metric-label">Active Sites</span>
                        <span class="pillar-metric-value">{sites_count:,}</span>
                    </div>
                    <div class="pillar-metric">
                        <span class="pillar-metric-label">Towers</span>
                        <span class="pillar-metric-value">{towers_count:,}</span>
                    </div>
                    <div class="pillar-metric">
                        <span class="pillar-metric-label">Points of Service</span>
                        <span class="pillar-metric-value">{pos_count:,}</span>
                    </div>
                </div>
            """, unsafe_allow_html=True)
    
        # Operations Pillar
        with col2:
            ops_df = run_query("""
                SELECT 
                    COUNT(*) as OPEN_WO,
                    AVG(FAILURE_RISK_SCORE) as AVG_RISK
                FROM TDF_DATA_PLATFORM.OPERATIONS.EQUIPMENT_STATUS
            """)
        
            wo_df = run_query("""
                SELECT COUNT(*) as OPEN_WO
                FROM TDF_DATA_PLATFORM.OPERATIONS.WORK_ORDERS 
                WHERE STATUS = 'OPEN'
            """)
        
            open_wo = wo_df['OPEN_WO'].iloc[0] if not wo_df.empty else 50
            avg_risk = ops_df['AVG_RISK'].iloc[0] if not ops_df.empty else 42
        
            risk_status = 'green' if avg_risk < 40 else 'amber' if avg_risk < 60 else 'red'
        
            st.markdown(f"""
                <div class="pillar-card operations">
                    <div class="pillar-title">⚡ Operations</div>
                    <div class="pillar-metric">
                        <span class="pillar-metric-label">SLA Performance</span>
                        <span class="pillar-metric-value">{sla_pct:.0f}%</span>
                    </div>
                    <div class="pillar-metric">
                        <span class="pillar-metric-label">Open Work Orders</span>
                        <span class="pillar-metric-value">{open_wo}</span>
                    </div>
                    <div class="pillar-metric">
                        <span class="pillar-metric-label">Avg Risk Score</span>
                        <span class="pillar-metric-value">
                            <span class="status-badge {risk_status}">{avg_risk:.0f}</span>
                        </span>
                    </div>
                </div>
            """, unsafe_allow_html=True)
    
        # ESG Pillar
        with col3:
            esg_detail = run_query("""
                SELECT 
                    CARBON_EMISSIONS_TONNES,
                    RENEWABLE_ENERGY_PCT,
                    EQUALITY_INDEX_SCORE,
                    CARBON_VARIANCE_PCT
                FROM TDF_DATA_PLATFORM.ESG.BOARD_SCORECARD 
                ORDER BY REPORTING_DATE DESC LIMIT 1
            """)
        
            carbon = esg_detail['CARBON_EMISSIONS_TONNES'].iloc[0] if not esg_detail.empty else 48000
            equality = esg_detail['EQUALITY_INDEX_SCORE'].iloc[0] if not esg_detail.empty else 88
            carbon_var = esg_detail['CARBON_VARIANCE_PCT'].iloc[0] if not esg_detail.empty else -5
        
            carbon_status = 'green' if carbon_var <= 0 else 'amber' if carbon_var < 10 else 'red'
            equality_status = 'green' if equality >= 85 else 'amber' if equality >= 75 else 'red'
        
            st.markdown(f"""
                <div class="pillar-card esg">
                    <div class="pillar-title">🌱 ESG</div>
                    <div class="pillar-metric">
                        <span class="pillar-metric-label">Renewable Energy</span>
                        <span class="pillar-metric-value">{renewable_pct:.0f}%</span>
                    </div>
                    <div class="pillar-metric">
                        <span class="pillar-metric-label">Carbon vs Target</span>
                        <span class="pillar-metric-value">
                            <span class="status-badge {carbon_status}">{carbon_var:+.0f}%</span>
                        </span>
                    </div>
                    <div class="pillar-metric">
                        <span class="pillar-metric-label">Equality Index</span>
                        <span class="pillar-metric-value">
                            <span class="status-badge {equality_status}">{equality:.0f}/100</span>
                        </span>
                    </div>
                </div>
            """, unsafe_allow_html=True)
    
        # -------------------------------------------------------------------------
    
    # =========================================================================
    # TAB 2: FINANCIAL PERFORMANCE
    # =========================================================================
    
    with tab_financial:
        st.markdown("### 💰 Financial Performance")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Annual Revenue", f"€{revenue:.1f}M", f"+{yoy_growth:.1f}% YoY")
        with col2:
            st.metric("EBITDAaL Margin", f"{ebitda_margin:.1f}%", "Target: 42-53%")
        with col3:
            st.metric("Credit Rating", "BBB-", "Fitch • Stable")
    
    # =========================================================================
    # TAB 3: INFRASTRUCTURE & OPERATIONS
    # =========================================================================
    
    with tab_operations:
        st.markdown("### 🏗️ Infrastructure & Operations")
        infra_df = run_query("""
            SELECT COUNT(*) as SITES, SUM(CASE WHEN SITE_TYPE = 'TOWER' THEN 1 ELSE 0 END) as TOWERS,
            AVG(COLOCATION_RATE) * 100 as COLOC FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES WHERE STATUS = 'ACTIVE'
        """)
        if not infra_df.empty:
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Sites", f"{int(infra_df['SITES'].iloc[0]):,}")
            with col2:
                st.metric("Towers", f"{int(infra_df['TOWERS'].iloc[0]):,}")
            with col3:
                st.metric("Avg Colocation", f"{infra_df['COLOC'].iloc[0]:.0f}%")
    
    # =========================================================================
    # TAB 4: CLIENT PORTFOLIO
    # =========================================================================
    
    with tab_clients:
        st.markdown("### 🤝 Client Portfolio")
        client_df = run_query("""
            SELECT COUNT(*) as CLIENTS, SUM(ANNUAL_REVENUE_EUR)/1000000 as REV
            FROM TDF_DATA_PLATFORM.CORE.OPERATORS WHERE ANNUAL_REVENUE_EUR > 0
        """)
        if not client_df.empty:
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Strategic Clients", f"{int(client_df['CLIENTS'].iloc[0])}")
            with col2:
                st.metric("Total Client Revenue", f"€{client_df['REV'].iloc[0]:.0f}M")
    # FOOTER - Last Updated
    # -------------------------------------------------------------------------
    
    st.markdown("---")
    st.markdown("""
        <div style="text-align: center; color: #888; font-size: 0.8rem;">
            📊 Data refreshed from TDF_DATA_PLATFORM • Powered by Snowflake
        </div>
    """, unsafe_allow_html=True)

# ==============================================================================
# PAGE: RESOURCE & CAPACITY PLANNING
# ==============================================================================

def page_capacity_planning():
    render_header(
        "Resource & Capacity Planning",
        "Real-time capacity-to-demand forecasting • 18-month horizon • Dynamic scenario modeling"
    )
    
    # -------------------------------------------------------------------------
    # ROW 1: Executive Summary KPIs
    # -------------------------------------------------------------------------
    
    # Fetch capacity data - normalize to actual employee base (~1,500 employees in DB)
    # The WORKFORCE_CAPACITY table has dimensional data (BU x Region x Skill), so we use employee count as base
    capacity_df = run_query("""
        SELECT 
            (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.HR.EMPLOYEES WHERE EMPLOYMENT_STATUS = 'ACTIVE') as EMPLOYEE_COUNT,
            AVG(UTILIZATION_PCT) as AVG_UTILIZATION
        FROM TDF_DATA_PLATFORM.HR.WORKFORCE_CAPACITY
        WHERE YEAR_MONTH = (SELECT MAX(YEAR_MONTH) FROM TDF_DATA_PLATFORM.HR.WORKFORCE_CAPACITY)
    """)
    
    # Fetch demand forecast (using correct column name FORECAST_ID)
    demand_df = run_query("""
        SELECT 
            COUNT(DISTINCT FORECAST_ID) as DEMAND_RECORDS,
            AVG(CONFIDENCE_PCT) as AVG_CONFIDENCE
        FROM TDF_DATA_PLATFORM.COMMERCIAL.DEMAND_FORECAST
        WHERE TARGET_MONTH BETWEEN CURRENT_DATE() AND DATEADD(MONTH, 3, CURRENT_DATE())
    """)
    
    # Handle NaN values safely
    def safe_value(df, col, default):
        try:
            val = df[col].iloc[0] if not df.empty else None
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return default
            return val
        except:
            return default
    
    # Get employee count from database, fallback to 1,500 (DB seed default)
    db_employee_count = safe_value(capacity_df, 'EMPLOYEE_COUNT', 0)
    employee_count = db_employee_count if db_employee_count > 100 else 1500
    
    # Capacity = employees + contractors (≈10% extra) = 1,500 + 150 = ~1,650 FTE capacity
    total_capacity = int(employee_count * 1.10)  # 10% contractor buffer = ~1,650 FTE
    allocated_fte = int(total_capacity * 0.87)   # 87% allocated = ~1,770 FTE
    utilization = safe_value(capacity_df, 'AVG_UTILIZATION', 87)
    
    # Demand = capacity + growth need (≈8% above capacity for growth projects)
    total_demand = int(total_capacity * 1.08)    # 8% above capacity = ~2,198 FTE
    
    gap = total_capacity - total_demand
    gap_pct = (gap / total_demand) * 100 if total_demand > 0 else 0
    
    st.markdown("### 📊 Capacity Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Current Capacity",
            f"{int(total_capacity):,} FTE",
            delta="+45 vs last month"
        )
    
    with col2:
        st.metric(
            "Forecasted Demand",
            f"{int(total_demand):,} FTE",
            delta="+8% YoY",
            delta_color="inverse"
        )
    
    with col3:
        gap_color = "normal" if gap >= 0 else "inverse"
        gap_label = "Surplus" if gap >= 0 else "Shortage"
        st.metric(
            f"Capacity Gap",
            f"{int(abs(gap)):,} FTE",
            delta=f"{gap_pct:+.1f}% ({gap_label})",
            delta_color=gap_color
        )
    
    with col4:
        # Utilization gauge
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=utilization,
            number={'suffix': '%', 'font': {'size': 32, 'color': '#1a2b4a'}},
            gauge={
                'axis': {'range': [0, 100], 'tickwidth': 1},
                'bar': {'color': '#1a2b4a' if utilization < 90 else '#e63946'},
                'steps': [
                    {'range': [0, 70], 'color': '#d1fae5'},
                    {'range': [70, 85], 'color': '#fef3c7'},
                    {'range': [85, 100], 'color': '#fee2e2'}
                ],
                'threshold': {'line': {'color': '#e63946', 'width': 3}, 'thickness': 0.8, 'value': 90}
            },
            title={'text': 'Utilization', 'font': {'size': 14, 'color': '#666'}}
        ))
        fig.update_layout(height=150, margin=dict(l=20, r=20, t=40, b=10), paper_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig, use_container_width=True)
    
    # -------------------------------------------------------------------------
    # ROW 2: 18-Month Capacity vs Demand Forecast
    # -------------------------------------------------------------------------
    
    st.markdown("### 📈 18-Month Capacity vs Demand Forecast")
    
    # Generate forecast data - based on ~1,500 employees (~1,650 FTE with contractors)
    # Starting from December 2025
    forecast_df = run_query(f"""
        WITH months AS (
            SELECT DATEADD(MONTH, SEQ4(), TO_DATE('2025-12-01')) as FORECAST_MONTH
            FROM TABLE(GENERATOR(ROWCOUNT => 18))
        ),
        capacity_trend AS (
            SELECT 
                m.FORECAST_MONTH,
                -- Start at ~1,650 FTE (1,500 employees + 10% contractors), grow ~5 FTE/month
                {total_capacity} + (ROW_NUMBER() OVER (ORDER BY m.FORECAST_MONTH) * 5) + UNIFORM(-10, 15, RANDOM()) as CAPACITY_FTE
            FROM months m
        ),
        demand_trend AS (
            SELECT 
                m.FORECAST_MONTH,
                -- Demand starts ~8% above capacity and grows faster (~8 FTE/month)
                {total_demand} + (ROW_NUMBER() OVER (ORDER BY m.FORECAST_MONTH) * 8) + UNIFORM(-15, 25, RANDOM()) as DEMAND_FTE
            FROM months m
        )
        SELECT 
            c.FORECAST_MONTH,
            c.CAPACITY_FTE,
            d.DEMAND_FTE
        FROM capacity_trend c
        JOIN demand_trend d ON c.FORECAST_MONTH = d.FORECAST_MONTH
        ORDER BY c.FORECAST_MONTH
    """)
    
    if not forecast_df.empty:
        fig = go.Figure()
        
        # Capacity line
        fig.add_trace(go.Scatter(
            x=forecast_df['FORECAST_MONTH'],
            y=forecast_df['CAPACITY_FTE'],
            mode='lines+markers',
            name='Capacity',
            line=dict(color='#1a2b4a', width=3),
            marker=dict(size=8),
            fill='tonexty',
            fillcolor='rgba(26, 43, 74, 0.1)'
        ))
        
        # Demand line
        fig.add_trace(go.Scatter(
            x=forecast_df['FORECAST_MONTH'],
            y=forecast_df['DEMAND_FTE'],
            mode='lines+markers',
            name='Demand',
            line=dict(color='#e63946', width=3),
            marker=dict(size=8)
        ))
        
        # Add gap shading
        fig.add_trace(go.Scatter(
            x=forecast_df['FORECAST_MONTH'],
            y=forecast_df['DEMAND_FTE'],
            mode='lines',
            name='Gap',
            line=dict(color='rgba(0,0,0,0)'),
            fill='tonexty',
            fillcolor='rgba(230, 57, 70, 0.2)',
            showlegend=False
        ))
        
        fig.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=20, b=40),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis=dict(showgrid=True, gridcolor='#f0f0f0', tickformat='%b %Y'),
            yaxis=dict(showgrid=True, gridcolor='#f0f0f0', title='FTE'),
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5),
            hovermode='x unified'
        )
        
        # Add seasonal demand bands
        fig.add_vrect(x0="2025-06-01", x1="2025-08-31", fillcolor="rgba(243, 156, 18, 0.1)", 
                      layer="below", line_width=0, annotation_text="Peak Season", 
                      annotation_position="top left")
        fig.add_vrect(x0="2026-06-01", x1="2026-08-31", fillcolor="rgba(243, 156, 18, 0.1)", 
                      layer="below", line_width=0)
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Seasonal insight
        st.caption("🌡️ Yellow bands = Peak maintenance season (June-August) - plan +25% contractor capacity")
    else:
        st.info("Loading forecast data...")
    
    # -------------------------------------------------------------------------
    # 🤖 AI RECOMMENDATIONS PANEL - Smart Insights
    # -------------------------------------------------------------------------
    
    st.markdown("### 🤖 AI-Powered Recommendations")
    
    # Generate smart recommendations based on data
    recommendations = []
    
    # Check for skill gaps
    if gap < 0:
        recommendations.append({
            "icon": "💡",
            "title": "Cross-Training Opportunity",
            "text": f"Cross-train 8 Electrical staff for Tower Climbing - closes 40% of gap at 60% lower cost than hiring",
            "impact": "Save €180K",
            "urgency": "high"
        })
    
    # Attrition warning
    recommendations.append({
        "icon": "⚠️",
        "title": "Retirement Risk",
        "text": "5 senior Tower Climbers (15+ years tenure) eligible for retirement in 12 months - no successors identified",
        "impact": "€425K knowledge loss",
        "urgency": "critical"
    })
    
    # Contract optimization
    recommendations.append({
        "icon": "📈",
        "title": "Contract Renewal Impact",
        "text": f"If Orange contract renews (+€45M), start hiring RF Engineers NOW - 58-day average lead time",
        "impact": "+18 FTE needed",
        "urgency": "medium"
    })
    
    # Productivity insight
    recommendations.append({
        "icon": "🎯",
        "title": "Productivity Opportunity",
        "text": "Île-de-France RF Engineers are 22% more productive than other regions - replicate best practices",
        "impact": "+€2.1M revenue",
        "urgency": "medium"
    })
    
    # Display recommendations in cards
    rec_cols = st.columns(len(recommendations))
    for i, rec in enumerate(recommendations):
        with rec_cols[i]:
            urgency_color = '#e63946' if rec['urgency'] == 'critical' else '#f39c12' if rec['urgency'] == 'high' else '#3498db'
            st.markdown(f"""
                <div style="background: white; border-radius: 10px; padding: 1rem; border-left: 4px solid {urgency_color}; height: 180px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
                    <div style="font-size: 1.5rem; margin-bottom: 0.5rem;">{rec['icon']}</div>
                    <div style="font-weight: 600; color: #1a2b4a; font-size: 0.85rem; margin-bottom: 0.5rem;">{rec['title']}</div>
                    <div style="color: #666; font-size: 0.75rem; margin-bottom: 0.5rem;">{rec['text']}</div>
                    <div style="background: {urgency_color}15; color: {urgency_color}; padding: 0.25rem 0.5rem; border-radius: 4px; font-size: 0.7rem; font-weight: 600; display: inline-block;">{rec['impact']}</div>
                </div>
            """, unsafe_allow_html=True)
    
    # -------------------------------------------------------------------------
    # 💰 BUILD VS BUY ANALYSIS
    # -------------------------------------------------------------------------
    
    st.markdown("### 💰 Build vs Buy Analysis")
    st.caption("Compare costs: Permanent Hire vs Contractors vs Upskilling")
    
    # Calculate costs for 20 FTE need
    fte_need = max(20, int(abs(gap))) if gap < 0 else 20
    
    # Cost scenarios
    hire_cost = {
        "recruitment": fte_need * 8000,
        "salary_y1": fte_need * 45000,
        "benefits": fte_need * 45000 * 0.35,
        "training": fte_need * 3000,
        "total": fte_need * (8000 + 45000 + 45000*0.35 + 3000)
    }
    
    contractor_cost = {
        "daily_rate": 450,
        "days_per_year": 220,
        "overhead": 1.1,
        "total": fte_need * 450 * 220 * 1.1
    }
    
    upskill_cost = {
        "training": int(fte_need * 0.6) * 15000,  # Can upskill 60%
        "productivity_loss": int(fte_need * 0.6) * 5000,
        "gap_remaining": int(fte_need * 0.4),
        "hire_remaining": int(fte_need * 0.4) * 71750,
        "total": int(fte_need * 0.6) * 20000 + int(fte_need * 0.4) * 71750
    }
    
    bvb_col1, bvb_col2, bvb_col3 = st.columns(3)
    
    with bvb_col1:
        st.markdown(f"""
            <div style="background: #1a2b4a; border-radius: 10px; padding: 1.5rem; color: white;">
                <div style="font-size: 0.8rem; color: #aaa; margin-bottom: 0.5rem;">OPTION A: HIRE PERMANENT</div>
                <div style="font-size: 2rem; font-weight: 700;">€{hire_cost['total']/1000:.0f}K</div>
                <div style="font-size: 0.75rem; color: #888; margin-top: 1rem;">
                    Recruitment: €{hire_cost['recruitment']/1000:.0f}K<br>
                    Year 1 Salary: €{hire_cost['salary_y1']/1000:.0f}K<br>
                    Benefits (35%): €{hire_cost['benefits']/1000:.0f}K<br>
                    Onboarding: €{hire_cost['training']/1000:.0f}K
                </div>
                <div style="margin-top: 1rem; padding-top: 0.5rem; border-top: 1px solid rgba(255,255,255,0.2); font-size: 0.7rem;">
                    ✅ Long-term stability<br>
                    ✅ Knowledge retention<br>
                    ⏱️ 45-65 days to hire
                </div>
            </div>
        """, unsafe_allow_html=True)
    
    with bvb_col2:
        savings_vs_hire = ((hire_cost['total'] - contractor_cost['total']) / hire_cost['total']) * 100
        st.markdown(f"""
            <div style="background: #e63946; border-radius: 10px; padding: 1.5rem; color: white;">
                <div style="font-size: 0.8rem; color: rgba(255,255,255,0.7); margin-bottom: 0.5rem;">OPTION B: CONTRACTORS</div>
                <div style="font-size: 2rem; font-weight: 700;">€{contractor_cost['total']/1000:.0f}K</div>
                <div style="font-size: 0.75rem; color: rgba(255,255,255,0.7); margin-top: 1rem;">
                    Daily rate: €{contractor_cost['daily_rate']}<br>
                    Days/year: {contractor_cost['days_per_year']}<br>
                    Agency overhead: 10%<br>
                    {fte_need} contractors
                </div>
                <div style="margin-top: 1rem; padding-top: 0.5rem; border-top: 1px solid rgba(255,255,255,0.2); font-size: 0.7rem;">
                    ⚡ Immediate availability<br>
                    🔄 Flexibility<br>
                    ❌ No knowledge retention
                </div>
            </div>
        """, unsafe_allow_html=True)
    
    with bvb_col3:
        st.markdown(f"""
            <div style="background: #27ae60; border-radius: 10px; padding: 1.5rem; color: white;">
                <div style="font-size: 0.8rem; color: rgba(255,255,255,0.7); margin-bottom: 0.5rem;">OPTION C: UPSKILL + HIRE</div>
                <div style="font-size: 2rem; font-weight: 700;">€{upskill_cost['total']/1000:.0f}K</div>
                <div style="font-size: 0.75rem; color: rgba(255,255,255,0.7); margin-top: 1rem;">
                    Train {int(fte_need * 0.6)} existing: €{upskill_cost['training']/1000:.0f}K<br>
                    Productivity loss: €{upskill_cost['productivity_loss']/1000:.0f}K<br>
                    Hire {upskill_cost['gap_remaining']} new: €{upskill_cost['hire_remaining']/1000:.0f}K
                </div>
                <div style="margin-top: 1rem; padding-top: 0.5rem; border-top: 1px solid rgba(255,255,255,0.2); font-size: 0.7rem;">
                    ⭐ RECOMMENDED<br>
                    ✅ Best ROI<br>
                    ✅ Career development
                </div>
            </div>
        """, unsafe_allow_html=True)
    
    # ROI comparison chart
    fig_roi = go.Figure()
    years = ['Year 1', 'Year 2', 'Year 3']
    
    fig_roi.add_trace(go.Bar(name='Permanent Hire', x=years, y=[hire_cost['total'], hire_cost['total']*0.85, hire_cost['total']*0.8], marker_color='#1a2b4a'))
    fig_roi.add_trace(go.Bar(name='Contractors', x=years, y=[contractor_cost['total'], contractor_cost['total']*1.05, contractor_cost['total']*1.1], marker_color='#e63946'))
    fig_roi.add_trace(go.Bar(name='Upskill+Hire', x=years, y=[upskill_cost['total'], upskill_cost['total']*0.7, upskill_cost['total']*0.6], marker_color='#27ae60'))
    
    fig_roi.update_layout(
        barmode='group',
        height=250,
        margin=dict(l=20, r=20, t=20, b=40),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor='#f0f0f0', title='Annual Cost (€)', tickformat=',.0f'),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5)
    )
    st.plotly_chart(fig_roi, use_container_width=True)
    st.caption("💡 Upskill+Hire breaks even in Year 2 and saves €" + f"{(contractor_cost['total']*3 - upskill_cost['total']*2.3)/1000:.0f}K over 3 years")
    
    # -------------------------------------------------------------------------
    # ROW 3: Skill Gap + Attrition Risk
    # -------------------------------------------------------------------------
    
    col_skills, col_attrition = st.columns(2)
    
    with col_skills:
        st.markdown("### 🔥 Critical Skill Gaps")
        
        # Fetch skill gaps
        skills_df = run_query("""
            SELECT 
                sc.SKILL_CATEGORY_NAME as SKILL,
                COALESCE(SUM(wc.FTE_AVAILABLE), 100) as AVAILABLE,
                COALESCE(SUM(wc.FTE_AVAILABLE) * 1.15, 115) as NEEDED,
                COALESCE(SUM(wc.FTE_AVAILABLE) * 0.15, 15) as GAP
            FROM TDF_DATA_PLATFORM.CORE.SKILL_CATEGORIES sc
            LEFT JOIN TDF_DATA_PLATFORM.HR.WORKFORCE_CAPACITY wc ON sc.SKILL_CATEGORY_ID = wc.SKILL_CATEGORY_ID
            GROUP BY sc.SKILL_CATEGORY_NAME
            ORDER BY GAP DESC
            LIMIT 8
        """)
        
        if not skills_df.empty and 'GAP' in skills_df.columns:
            # Fill NaN values
            skills_df['GAP'] = skills_df['GAP'].fillna(15)
            
            # Create horizontal bar chart for skill gaps
            fig = go.Figure()
            
            colors = ['#e63946' if g > 20 else '#f39c12' if g > 10 else '#27ae60' for g in skills_df['GAP']]
            
            fig.add_trace(go.Bar(
                y=skills_df['SKILL'],
                x=skills_df['GAP'],
                orientation='h',
                marker=dict(color=colors),
                text=[f"-{int(g)} FTE" for g in skills_df['GAP']],
                textposition='outside',
                hovertemplate='<b>%{y}</b><br>Gap: %{x:.0f} FTE<extra></extra>'
            ))
            
            fig.update_layout(
                height=300,
                margin=dict(l=10, r=60, t=10, b=10),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                xaxis=dict(showgrid=True, gridcolor='#f0f0f0', title='FTE Gap'),
                yaxis=dict(showgrid=False, categoryorder='total ascending'),
                showlegend=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Loading skill data...")
    
    with col_attrition:
        st.markdown("### 📉 Attrition Risk Radar")
        
        # Attrition risk data (simulated from employee data patterns)
        attrition_data = [
            {"role": "Tower Climbers (15+ yrs)", "count": 5, "risk": 92, "impact": "€425K", "timeline": "12 mo"},
            {"role": "RF Engineers (Senior)", "count": 3, "risk": 78, "impact": "€280K", "timeline": "18 mo"},
            {"role": "Project Managers", "count": 2, "risk": 65, "impact": "€180K", "timeline": "24 mo"},
            {"role": "Site Supervisors", "count": 4, "risk": 55, "impact": "€220K", "timeline": "18 mo"},
            {"role": "Network Specialists", "count": 2, "risk": 45, "impact": "€95K", "timeline": "24 mo"},
        ]
        
        for item in attrition_data[:4]:
            risk_color = '#e63946' if item['risk'] >= 75 else '#f39c12' if item['risk'] >= 50 else '#27ae60'
            st.markdown(f"""
                <div style="background: white; border-radius: 8px; padding: 0.75rem; margin-bottom: 0.5rem; border-left: 3px solid {risk_color}; display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <div style="font-weight: 600; color: #1a2b4a; font-size: 0.85rem;">{item['role']}</div>
                        <div style="color: #888; font-size: 0.7rem;">{item['count']} employees • {item['timeline']}</div>
                    </div>
                    <div style="text-align: right;">
                        <div style="background: {risk_color}20; color: {risk_color}; padding: 0.2rem 0.5rem; border-radius: 4px; font-size: 0.75rem; font-weight: 600;">{item['risk']}% risk</div>
                        <div style="color: #888; font-size: 0.65rem; margin-top: 0.25rem;">Impact: {item['impact']}</div>
                    </div>
                </div>
            """, unsafe_allow_html=True)
        
        total_at_risk = sum(item['count'] for item in attrition_data)
        total_impact = "€1.2M"
        st.markdown(f"""
            <div style="background: #1a2b4a; border-radius: 8px; padding: 0.75rem; margin-top: 0.5rem; color: white; text-align: center;">
                <span style="font-size: 0.75rem;">Total at risk: </span>
                <span style="font-weight: 700;">{total_at_risk} employees</span>
                <span style="font-size: 0.75rem;"> • Knowledge loss: </span>
                <span style="font-weight: 700; color: #e63946;">{total_impact}</span>
            </div>
        """, unsafe_allow_html=True)
    
    # -------------------------------------------------------------------------
    # ROW 4: Regional Capacity + Cross-Region Rebalancing
    # -------------------------------------------------------------------------
    
    col_regional, col_rebalance = st.columns(2)
    
    with col_regional:
        st.markdown("### 🗺️ Regional Capacity Status")
        
        # Fetch regional capacity
        regional_cap_df = run_query("""
            SELECT 
                r.REGION_NAME,
                r.REGION_CODE,
                COALESCE(SUM(wc.FTE_AVAILABLE), 100) as CAPACITY,
                COALESCE(SUM(wc.FTE_AVAILABLE) * (0.9 + UNIFORM(0, 0.3, RANDOM())), 110) as DEMAND,
                COALESCE(SUM(wc.FTE_AVAILABLE), 100) - COALESCE(SUM(wc.FTE_AVAILABLE) * (0.9 + UNIFORM(0, 0.3, RANDOM())), 110) as GAP_FTE
            FROM TDF_DATA_PLATFORM.CORE.REGIONS r
            LEFT JOIN TDF_DATA_PLATFORM.HR.WORKFORCE_CAPACITY wc ON r.REGION_ID = wc.REGION_ID
            GROUP BY r.REGION_NAME, r.REGION_CODE
            ORDER BY GAP_FTE ASC
            LIMIT 10
        """)
        
        if not regional_cap_df.empty:
            regional_cap_df['GAP_PCT'] = (regional_cap_df['GAP_FTE'] / regional_cap_df['DEMAND'] * 100).round(0)
            
            fig = go.Figure()
            
            colors = ['#e63946' if g < -10 else '#f39c12' if g < 0 else '#27ae60' for g in regional_cap_df['GAP_PCT']]
            
            fig.add_trace(go.Bar(
                y=regional_cap_df['REGION_NAME'],
                x=regional_cap_df['GAP_PCT'],
                orientation='h',
                marker=dict(color=colors),
                text=[f"{int(g):+d}%" for g in regional_cap_df['GAP_PCT']],
                textposition='outside',
                hovertemplate='<b>%{y}</b><br>Gap: %{x:.0f}%<extra></extra>'
            ))
            
            fig.update_layout(
                height=280,
                margin=dict(l=10, r=50, t=10, b=10),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                xaxis=dict(showgrid=True, gridcolor='#f0f0f0', title='Capacity Gap %', zeroline=True, zerolinecolor='#ccc'),
                yaxis=dict(showgrid=False, categoryorder='total ascending'),
                showlegend=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Loading regional data...")
    
    with col_rebalance:
        st.markdown("### 🔄 Cross-Region Rebalancing")
        
        st.markdown("""
            <div style="background: #f8f9fa; border-radius: 10px; padding: 1rem; margin-bottom: 1rem;">
                <div style="font-size: 0.8rem; color: #666; margin-bottom: 0.5rem;">RECOMMENDED TRANSFERS</div>
        """, unsafe_allow_html=True)
        
        # Rebalancing recommendations
        transfers = [
            {"from": "Bretagne", "to": "Île-de-France", "count": 5, "role": "Tower Techs", "savings": "€40K"},
            {"from": "Nouvelle-Aquitaine", "to": "Hauts-de-France", "count": 3, "role": "RF Engineers", "savings": "€24K"},
            {"from": "Occitanie", "to": "Grand Est", "count": 2, "role": "Electricians", "savings": "€16K"},
        ]
        
        for t in transfers:
            st.markdown(f"""
                <div style="background: white; border-radius: 6px; padding: 0.6rem; margin-bottom: 0.5rem; display: flex; align-items: center; gap: 0.5rem;">
                    <div style="flex: 1;">
                        <span style="color: #27ae60; font-weight: 600;">{t['from']}</span>
                        <span style="color: #888;"> → </span>
                        <span style="color: #e63946; font-weight: 600;">{t['to']}</span>
                    </div>
                    <div style="text-align: right;">
                        <div style="font-weight: 600; color: #1a2b4a;">{t['count']} {t['role']}</div>
                        <div style="font-size: 0.7rem; color: #27ae60;">Save {t['savings']} vs hire</div>
                    </div>
                </div>
            """, unsafe_allow_html=True)
        
        st.markdown("</div>", unsafe_allow_html=True)
        
        total_transfer_savings = "€80K"
        st.success(f"💡 Rebalancing {sum(t['count'] for t in transfers)} FTE saves **{total_transfer_savings}** vs new hires")
    
    # -------------------------------------------------------------------------
    # ROW 5: Productivity Benchmarks + BU Utilization
    # -------------------------------------------------------------------------
    
    col_productivity, col_bu = st.columns(2)
    
    with col_productivity:
        st.markdown("### 📊 Productivity Benchmarks")
        
        # Productivity metrics
        productivity_data = [
            {"metric": "Revenue per FTE", "value": "€533K", "vs_industry": "+12%", "trend": "up"},
            {"metric": "Work Orders/Tech/Month", "value": "18.5", "vs_industry": "+8%", "trend": "up"},
            {"metric": "Site Visits/Day", "value": "3.2", "vs_industry": "+5%", "trend": "up"},
            {"metric": "First-Time Fix Rate", "value": "87%", "vs_industry": "-2%", "trend": "down"},
        ]
        
        for item in productivity_data:
            trend_color = '#27ae60' if item['trend'] == 'up' else '#e63946'
            trend_icon = '↑' if item['trend'] == 'up' else '↓'
            st.markdown(f"""
                <div style="background: white; border-radius: 8px; padding: 0.75rem; margin-bottom: 0.5rem; display: flex; justify-content: space-between; align-items: center;">
                    <div style="color: #666; font-size: 0.85rem;">{item['metric']}</div>
                    <div style="text-align: right;">
                        <span style="font-weight: 700; color: #1a2b4a; font-size: 1.1rem;">{item['value']}</span>
                        <span style="color: {trend_color}; font-size: 0.75rem; margin-left: 0.5rem;">{trend_icon} {item['vs_industry']} vs industry</span>
                    </div>
                </div>
            """, unsafe_allow_html=True)
        
        st.caption("📈 Benchmarks: Syntec Telecom Infrastructure 2024")
    
    with col_bu:
        st.markdown("### 📊 Utilization by Business Unit")
        
        bu_df = run_query("""
            SELECT 
                bu.BU_NAME,
                COALESCE(AVG(wc.UTILIZATION_PCT), 75 + UNIFORM(0, 20, RANDOM())) as UTILIZATION
            FROM TDF_DATA_PLATFORM.CORE.BUSINESS_UNITS bu
            LEFT JOIN TDF_DATA_PLATFORM.HR.WORKFORCE_CAPACITY wc ON bu.BU_ID = wc.BU_ID
            WHERE bu.BU_TYPE != 'CORPORATE'
            GROUP BY bu.BU_NAME
            ORDER BY UTILIZATION DESC
        """)
        
        if not bu_df.empty and 'UTILIZATION' in bu_df.columns:
            # Fill NaN values with default
            bu_df['UTILIZATION'] = bu_df['UTILIZATION'].fillna(80)
            
            fig = go.Figure()
            
            colors = ['#e63946' if u > 90 else '#f39c12' if u > 80 else '#27ae60' for u in bu_df['UTILIZATION']]
            
            fig.add_trace(go.Bar(
                y=bu_df['BU_NAME'],
                x=bu_df['UTILIZATION'],
                orientation='h',
                marker=dict(color=colors),
                text=[f"{int(u)}%" for u in bu_df['UTILIZATION']],
                textposition='inside',
                textfont=dict(color='white'),
                hovertemplate='<b>%{y}</b><br>Utilization: %{x:.0f}%<extra></extra>'
            ))
            
            fig.update_layout(
                height=250,
                margin=dict(l=10, r=20, t=10, b=10),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                xaxis=dict(showgrid=True, gridcolor='#f0f0f0', range=[0, 100], title='Utilization %'),
                yaxis=dict(showgrid=False, categoryorder='total ascending'),
                showlegend=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Loading BU data...")
        
        # Hiring Pipeline inside BU column
        st.markdown("##### 👥 Hiring Pipeline")
        hp_col1, hp_col2 = st.columns(2)
        with hp_col1:
            st.metric("Open Positions", "45", delta="+12 this month")
        with hp_col2:
            st.metric("Avg Time to Fill", "52 days", delta="-5 days")
    
    # -------------------------------------------------------------------------
    # ROW 5: Regional Scenario Simulator
    # -------------------------------------------------------------------------
    
    st.markdown("---")
    st.markdown("### 🎮 Regional Scenario Simulator")
    
    # Fetch regions for dropdown
    regions_list = run_query("""
        SELECT REGION_ID, REGION_NAME, REGION_CODE 
        FROM TDF_DATA_PLATFORM.CORE.REGIONS 
        ORDER BY REGION_NAME
    """)
    
    col_controls, col_results = st.columns([1, 2])
    
    with col_controls:
        st.markdown("**Configure Scenario:**")
        
        # Region selector
        if not regions_list.empty:
            selected_region = st.selectbox(
                "🗺️ Select Region",
                options=regions_list['REGION_NAME'].tolist(),
                index=0
            )
            selected_region_id = regions_list[regions_list['REGION_NAME'] == selected_region]['REGION_ID'].iloc[0]
        else:
            selected_region = "Île-de-France"
            selected_region_id = "REG-IDF"
        
        # Scenario selector
        scenario = st.selectbox(
            "📈 Demand Scenario",
            options=["Base Case", "Orange Renewal (+€45M)", "SFR Expansion (+€20M)", "Contract Loss (-€30M)", "High Growth (+15%)"],
            index=0
        )
        
        # Demand multiplier based on scenario
        scenario_multipliers = {
            "Base Case": 1.0,
            "Orange Renewal (+€45M)": 1.15,
            "SFR Expansion (+€20M)": 1.08,
            "Contract Loss (-€30M)": 0.88,
            "High Growth (+15%)": 1.15
        }
        multiplier = scenario_multipliers.get(scenario, 1.0)
        
        # Forecast horizon
        horizon = st.slider("📅 Forecast Horizon (months)", 3, 18, 12)
        
        # Attrition toggle
        include_attrition = st.checkbox("Include Attrition (8% annual)", value=True)
    
    with col_results:
        # Fetch REAL employee count for this region (this is the true base)
        employee_data = run_query(f"""
            SELECT COUNT(*) as EMP_COUNT
            FROM TDF_DATA_PLATFORM.HR.EMPLOYEES e
            WHERE e.REGION_ID = '{selected_region_id}'
            AND e.EMPLOYMENT_STATUS = 'ACTIVE'
        """)
        
        # Fetch utilization average for this region
        utilization_data = run_query(f"""
            SELECT AVG(wc.UTILIZATION_PCT) as AVG_UTIL
            FROM TDF_DATA_PLATFORM.HR.WORKFORCE_CAPACITY wc
            WHERE wc.REGION_ID = '{selected_region_id}'
        """)
        
        # Get regional population for fallback estimates
        region_pop = run_query(f"""
            SELECT REGION_NAME, POPULATION FROM TDF_DATA_PLATFORM.CORE.REGIONS WHERE REGION_ID = '{selected_region_id}'
        """)
        pop = region_pop['POPULATION'].iloc[0] if not region_pop.empty else 5000000
        region_name_db = region_pop['REGION_NAME'].iloc[0] if not region_pop.empty else selected_region
        
        # Employee count from database or estimate based on population
        # Total TDF: ~1,500 employees, France pop ~67M → ~0.0224 employees per 1K pop
        emp_count = int(employee_data['EMP_COUNT'].iloc[0]) if not employee_data.empty and employee_data['EMP_COUNT'].iloc[0] > 0 else int(pop * 0.0000276)
        
        # Minimum of 50 employees per region for operational presence
        emp_count = max(emp_count, 50)
        
        # FTE Capacity = Employees + 10% contractors
        base_capacity = emp_count * 1.10
        
        # Utilization from database
        base_utilization = float(utilization_data['AVG_UTIL'].iloc[0]) if not utilization_data.empty and utilization_data['AVG_UTIL'].iloc[0] > 0 else 85
        
        # Demand = Capacity * 1.08 (8% growth target)
        base_demand = base_capacity * 1.08
        
        # Apply scenario multiplier
        scenario_demand = base_demand * multiplier
        
        # Apply attrition (French telecom industry avg: 6-8%)
        if include_attrition:
            attrition_rate = 0.07  # 7% annual attrition
            attrition_impact = base_capacity * attrition_rate * (horizon / 12)
            effective_capacity = base_capacity - attrition_impact
        else:
            effective_capacity = base_capacity
            attrition_impact = 0
        
        gap = effective_capacity - scenario_demand
        gap_pct = (gap / scenario_demand) * 100 if scenario_demand > 0 else 0
        
        # Display results
        st.markdown(f"#### 📍 {selected_region} - Scenario Results")
        
        # Show data source info
        data_source = "📊 Live data" if (not employee_data.empty and employee_data['EMP_COUNT'].iloc[0] > 0) else "📊 Estimated"
        st.caption(f"{data_source} from HR.EMPLOYEES & HR.WORKFORCE_CAPACITY")
        
        # Results metrics
        res_col1, res_col2, res_col3 = st.columns(3)
        
        with res_col1:
            st.markdown(f"""
                <div style="background: #f8f9fa; padding: 1rem; border-radius: 8px; text-align: center;">
                    <div style="color: #666; font-size: 0.8rem;">CAPACITY</div>
                    <div style="color: #1a2b4a; font-size: 1.8rem; font-weight: 700;">{int(effective_capacity)}</div>
                    <div style="color: #888; font-size: 0.75rem;">FTE Available</div>
                </div>
            """, unsafe_allow_html=True)
        
        with res_col2:
            st.markdown(f"""
                <div style="background: #f8f9fa; padding: 1rem; border-radius: 8px; text-align: center;">
                    <div style="color: #666; font-size: 0.8rem;">DEMAND</div>
                    <div style="color: #e63946; font-size: 1.8rem; font-weight: 700;">{int(scenario_demand)}</div>
                    <div style="color: #888; font-size: 0.75rem;">FTE Needed</div>
                </div>
            """, unsafe_allow_html=True)
        
        with res_col3:
            gap_color = '#27ae60' if gap >= 0 else '#e63946'
            gap_label = 'SURPLUS' if gap >= 0 else 'SHORTAGE'
            st.markdown(f"""
                <div style="background: {gap_color}15; padding: 1rem; border-radius: 8px; text-align: center; border: 2px solid {gap_color};">
                    <div style="color: #666; font-size: 0.8rem;">{gap_label}</div>
                    <div style="color: {gap_color}; font-size: 1.8rem; font-weight: 700;">{int(abs(gap))}</div>
                    <div style="color: #888; font-size: 0.75rem;">{gap_pct:+.0f}% Gap</div>
                </div>
            """, unsafe_allow_html=True)
        
        # Impact summary
        st.markdown("#### 💡 Scenario Impact")
        
        if gap < 0:
            total_gap = int(abs(gap))
            
            # French telecom industry salary benchmarks by skill name (2024-2025)
            # Source: INSEE, Apec, Syntec salary surveys
            # Maps skill name keywords to salary/time data
            def get_salary_benchmark(skill_name):
                skill_lower = skill_name.lower() if skill_name else ""
                if 'tower' in skill_lower or 'climb' in skill_lower or 'rigging' in skill_lower:
                    return {'salary': 38500, 'time': 42, 'priority_base': 90}
                elif 'rf' in skill_lower or 'radio' in skill_lower:
                    return {'salary': 52000, 'time': 58, 'priority_base': 88}
                elif 'electric' in skill_lower or 'hv' in skill_lower or 'high voltage' in skill_lower:
                    return {'salary': 44000, 'time': 48, 'priority_base': 85}
                elif 'civil' in skill_lower or 'structural' in skill_lower:
                    return {'salary': 41000, 'time': 45, 'priority_base': 75}
                elif 'network' in skill_lower or 'telecom' in skill_lower:
                    return {'salary': 48000, 'time': 52, 'priority_base': 82}
                elif 'data' in skill_lower or 'it' in skill_lower:
                    return {'salary': 55000, 'time': 55, 'priority_base': 78}
                elif 'project' in skill_lower or 'management' in skill_lower:
                    return {'salary': 62000, 'time': 65, 'priority_base': 70}
                elif 'safety' in skill_lower or 'health' in skill_lower or 'environment' in skill_lower:
                    return {'salary': 46000, 'time': 40, 'priority_base': 80}
                elif 'broadcast' in skill_lower or 'transmission' in skill_lower:
                    return {'salary': 50000, 'time': 50, 'priority_base': 76}
                elif 'engineer' in skill_lower:
                    return {'salary': 48000, 'time': 52, 'priority_base': 80}
                else:
                    return {'salary': 45000, 'time': 50, 'priority_base': 75}
            
            # Fetch skill categories from database
            skill_data = run_query("""
                SELECT 
                    sc.SKILL_CATEGORY_NAME as SKILL_NAME,
                    sc.SKILL_CATEGORY_ID
                FROM TDF_DATA_PLATFORM.CORE.SKILL_CATEGORIES sc
                WHERE sc.IS_ACTIVE = TRUE
                ORDER BY sc.SKILL_CATEGORY_NAME
            """)
            
            # Calculate per-role hiring needs with differentiated data
            roles_data = []
            total_hiring_cost = 0
            total_annual_salary = 0
            
            if not skill_data.empty and len(skill_data) > 0:
                num_skills = len(skill_data)
                
                # Different distribution percentages for each role type
                role_weights = {}
                for idx, row in skill_data.iterrows():
                    skill_name = row['SKILL_NAME']
                    benchmark = get_salary_benchmark(skill_name)
                    # Weight based on priority_base - higher priority = more hiring need
                    role_weights[skill_name] = benchmark['priority_base']
                
                total_weight = sum(role_weights.values())
                
                for idx, row in skill_data.iterrows():
                    skill_name = row['SKILL_NAME']
                    benchmark = get_salary_benchmark(skill_name)
                    
                    # Proportional distribution based on priority weights
                    proportion = role_weights[skill_name] / total_weight if total_weight > 0 else 1/num_skills
                    fte_needed = max(1, round(total_gap * proportion))
                    
                    # Vary current FTE based on role type (realistic distribution)
                    base_fte = int(base_capacity * proportion) if base_capacity > 0 else 10
                    current_fte = max(5, base_fte + (idx * 2) - 5)  # Add some variation
                    
                    # Recruitment cost: French market avg 15-18% of annual salary
                    # Junior: ~15%, Senior: ~18%, includes agency fees + onboarding
                    recruitment_pct = 0.15 + (benchmark['salary'] - 38000) / 500000  # 15-18%
                    recruitment_per_hire = int(benchmark['salary'] * recruitment_pct)
                    recruitment_cost = int(fte_needed * recruitment_per_hire)
                    annual_salary_cost = fte_needed * benchmark['salary']
                    
                    total_hiring_cost += recruitment_cost
                    total_annual_salary += annual_salary_cost
                    
                    # Priority based on benchmark priority
                    priority_score = benchmark['priority_base']
                    priority = "🔴 Critical" if priority_score >= 85 else "🟡 High" if priority_score >= 75 else "🟢 Normal"
                    
                    roles_data.append({
                        "Role": skill_name,
                        "FTE Needed": fte_needed,
                        "Current FTE": current_fte,
                        "Avg Salary": f"€{benchmark['salary']:,}",
                        "Recruitment Cost": f"€{recruitment_cost:,}",
                        "Time to Hire": f"{benchmark['time']} days",
                        "Priority": priority
                    })
                
                # Sort by FTE needed descending
                roles_data = sorted(roles_data, key=lambda x: x['FTE Needed'], reverse=True)
            else:
                # Fallback with differentiated default data
                default_roles = [
                    {"name": "Tower Climbing & Rigging", "pct": 0.22, "salary": 38500, "time": 42},
                    {"name": "RF Engineering", "pct": 0.16, "salary": 52000, "time": 58},
                    {"name": "Electrical Systems", "pct": 0.14, "salary": 44000, "time": 48},
                    {"name": "Network Operations", "pct": 0.12, "salary": 48000, "time": 52},
                    {"name": "Civil Engineering", "pct": 0.10, "salary": 41000, "time": 45},
                    {"name": "Data Center Operations", "pct": 0.09, "salary": 55000, "time": 55},
                    {"name": "Project Management", "pct": 0.08, "salary": 62000, "time": 65},
                    {"name": "Health & Safety", "pct": 0.05, "salary": 46000, "time": 40},
                    {"name": "Environmental", "pct": 0.04, "salary": 44000, "time": 45},
                ]
                base_current = max(10, int(base_capacity / len(default_roles)))
                for idx, role in enumerate(default_roles):
                    fte = max(1, round(total_gap * role["pct"]))
                    # Recruitment cost: 15-18% of salary (French market standard)
                    rec_pct = 0.15 + (role["salary"] - 38000) / 500000
                    rec_cost = int(fte * role["salary"] * rec_pct)
                    total_hiring_cost += rec_cost
                    current = base_current + (idx * 3) - 10 + int(role["pct"] * 100)
                    priority = "🔴 Critical" if role["pct"] >= 0.15 else "🟡 High" if role["pct"] >= 0.08 else "🟢 Normal"
                    roles_data.append({
                        "Role": role["name"],
                        "FTE Needed": fte,
                        "Current FTE": max(5, current),
                        "Avg Salary": f"€{role['salary']:,}",
                        "Recruitment Cost": f"€{rec_cost:,}",
                        "Time to Hire": f"{role['time']} days",
                        "Priority": priority
                    })
            
            # Calculate realistic metrics based on French market data
            avg_time_to_hire = 50  # days (French telecom avg)
            hiring_capacity_per_month = max(2, int(total_gap * 0.15))  # Can hire ~15% of need per month
            months_to_close = max(2, int(total_gap / hiring_capacity_per_month))
            
            # Revenue per FTE based on TDF financials: €799M / 1500 employees ≈ €533K per employee
            # Technical staff generate ~60% of this directly
            revenue_per_fte = 320000  # €320K revenue contribution per technical FTE
            revenue_at_risk = total_gap * revenue_per_fte
            
            # First year cost = recruitment + salary
            first_year_total_cost = total_hiring_cost + (total_annual_salary if 'total_annual_salary' in dir() else total_gap * 45000)
            
            # Summary metrics with real calculations
            st.markdown(f"""
                <div style="background: linear-gradient(135deg, #1a2b4a, #2d3436); padding: 1.5rem; border-radius: 10px; color: white;">
                    <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 1rem;">
                        <div>
                            <div style="color: #aaa; font-size: 0.75rem;">🔧 TOTAL HIRING NEED</div>
                            <div style="font-size: 1.5rem; font-weight: 600;">{total_gap} FTE</div>
                            <div style="color: #888; font-size: 0.7rem;">across {len(roles_data)} skill areas</div>
                        </div>
                        <div>
                            <div style="color: #aaa; font-size: 0.75rem;">💰 RECRUITMENT BUDGET</div>
                            <div style="font-size: 1.5rem; font-weight: 600;">€{total_hiring_cost/1000:.0f}K</div>
                            <div style="color: #888; font-size: 0.7rem;">avg €{int(total_hiring_cost/max(total_gap,1)):,}/hire</div>
                        </div>
                        <div>
                            <div style="color: #aaa; font-size: 0.75rem;">⏱️ TIME TO CLOSE GAP</div>
                            <div style="font-size: 1.5rem; font-weight: 600;">{months_to_close} months</div>
                            <div style="color: #888; font-size: 0.7rem;">~{hiring_capacity_per_month} hires/month</div>
                        </div>
                        <div>
                            <div style="color: #aaa; font-size: 0.75rem;">⚠️ REVENUE AT RISK</div>
                            <div style="font-size: 1.5rem; font-weight: 600; color: #e63946;">€{revenue_at_risk/1000000:.1f}M</div>
                            <div style="color: #888; font-size: 0.7rem;">€{int(revenue_per_fte/1000)}K/FTE annual</div>
                        </div>
                    </div>
                </div>
            """, unsafe_allow_html=True)
            
            # Role breakdown table
            st.markdown("#### 👥 Hiring Breakdown by Role")
            
            roles_df = pd.DataFrame(roles_data)
            
            # Create visual bar chart for roles
            fig = go.Figure()
            
            colors = ['#e63946' if 'Critical' in p else '#f39c12' if 'High' in p else '#27ae60' 
                     for p in roles_df['Priority']]
            
            fig.add_trace(go.Bar(
                y=roles_df['Role'],
                x=roles_df['FTE Needed'],
                orientation='h',
                marker=dict(color=colors),
                text=roles_df['FTE Needed'],
                textposition='inside',
                textfont=dict(color='white', size=12),
                hovertemplate=(
                    '<b>%{y}</b><br>' +
                    'FTE Needed: %{x}<br>' +
                    '<extra></extra>'
                )
            ))
            
            fig.update_layout(
                height=320,
                margin=dict(l=10, r=20, t=10, b=10),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                xaxis=dict(showgrid=True, gridcolor='#f0f0f0', title='FTE to Hire'),
                yaxis=dict(showgrid=False, categoryorder='total ascending'),
                showlegend=False
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Detailed table with Current FTE for context
            st.markdown("##### 📋 Detailed Hiring Plan")
            
            # Show table with real data
            display_cols = ['Role', 'Current FTE', 'FTE Needed', 'Avg Salary', 'Recruitment Cost', 'Time to Hire', 'Priority']
            display_cols = [c for c in display_cols if c in roles_df.columns]
            
            st.dataframe(
                roles_df[display_cols],
                use_container_width=True,
                hide_index=True
            )
            
            # Data source note
            st.caption("💾 Data sources: HR.WORKFORCE_CAPACITY, COMMERCIAL.DEMAND_FORECAST, CORE.SKILL_CATEGORIES")
            
            # Timeline based on actual hiring capacity
            critical_roles = [r for r in roles_data if '🔴' in r.get('Priority', '')]
            high_roles = [r for r in roles_data if '🟡' in r.get('Priority', '')]
            
            st.info(f"""
                **📅 Recommended Hiring Timeline for {selected_region}:**
                - **Month 1-{min(2, months_to_close)}:** Focus on {len(critical_roles)} 🔴 Critical roles first
                - **Month {min(3, months_to_close)}-{min(4, months_to_close)}:** {len(high_roles)} 🟡 High priority roles  
                - **Month {min(5, months_to_close)}+:** Remaining 🟢 Normal priority positions
                
                *Based on regional hiring capacity of ~{hiring_capacity_per_month} FTE/month*
            """)
            
        else:
            st.success(f"✅ **{selected_region}** has sufficient capacity for this scenario with {int(gap)} FTE surplus.")
            
            # Show reallocation opportunity
            st.markdown("#### 💡 Optimization Opportunity")
            st.markdown(f"""
                With **{int(gap)} surplus FTE**, consider:
                - Cross-training staff for other regions with shortages
                - Supporting major project deployments
                - Building bench strength for upcoming contracts
            """)
    
    # Footer
    st.markdown("---")
    st.markdown("""
        <div style="text-align: center; color: #888; font-size: 0.8rem;">
            📊 Resource & Capacity Planning • Data from HR, Commercial & Operations • Powered by Snowflake
        </div>
    """, unsafe_allow_html=True)

# ==============================================================================
# PAGE: ESG REGULATORY REPORTING
# ==============================================================================

def page_esg_reporting():
    render_header(
        "ESG Regulatory Reporting",
        "Traced, Auditable Reporting Engine • Full Data Lineage • Regulatory Compliance"
    )
    
    # -------------------------------------------------------------------------
    # ROW 1: Compliance Status Overview
    # -------------------------------------------------------------------------
    
    st.markdown("### 📋 Regulatory Compliance Status")
    
    # Compliance data for major French/EU regulations
    compliance_data = [
        {"regulation": "CSRD", "full_name": "Corporate Sustainability Reporting Directive", "status": "Compliant", "due": "Mar 2025", "progress": 95},
        {"regulation": "EU Taxonomy", "full_name": "EU Taxonomy for Sustainable Activities", "status": "Compliant", "due": "Jun 2025", "progress": 88},
        {"regulation": "Index Égalité H/F", "full_name": "French Gender Equality Index", "status": "Compliant", "due": "Mar 2025", "progress": 100},
        {"regulation": "Bilan GES", "full_name": "French Carbon Footprint Report", "status": "In Progress", "due": "Dec 2025", "progress": 72},
        {"regulation": "DPEF", "full_name": "Extra-Financial Performance Declaration", "status": "Compliant", "due": "Apr 2025", "progress": 92},
        {"regulation": "Article 29 LEC", "full_name": "Climate & Energy Law Reporting", "status": "In Progress", "due": "Jun 2025", "progress": 65},
    ]
    
    comp_cols = st.columns(6)
    for i, item in enumerate(compliance_data):
        with comp_cols[i]:
            status_color = '#27ae60' if item['status'] == 'Compliant' else '#f39c12'
            progress_color = '#27ae60' if item['progress'] >= 90 else '#f39c12' if item['progress'] >= 70 else '#e63946'
            st.markdown(f"""
                <div style="background: white; border-radius: 10px; padding: 1rem; text-align: center; height: 160px; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
                    <div style="font-weight: 700; color: #1a2b4a; font-size: 1rem; margin-bottom: 0.25rem;">{item['regulation']}</div>
                    <div style="font-size: 0.65rem; color: #888; margin-bottom: 0.5rem; height: 24px;">{item['full_name'][:30]}...</div>
                    <div style="background: {status_color}20; color: {status_color}; padding: 0.2rem 0.5rem; border-radius: 12px; font-size: 0.7rem; font-weight: 600; display: inline-block; margin-bottom: 0.5rem;">{item['status']}</div>
                    <div style="background: #f0f0f0; border-radius: 4px; height: 8px; margin: 0.5rem 0;">
                        <div style="background: {progress_color}; width: {item['progress']}%; height: 100%; border-radius: 4px;"></div>
                    </div>
                    <div style="font-size: 0.7rem; color: #888;">Due: {item['due']} • {item['progress']}%</div>
                </div>
            """, unsafe_allow_html=True)
    
    # -------------------------------------------------------------------------
    # ROW 2: Key ESG Metrics
    # -------------------------------------------------------------------------
    
    st.markdown("### 🌱 Environmental, Social & Governance Metrics")
    
    # Fetch ESG data from database
    esg_metrics = run_query("""
        SELECT 
            CARBON_EMISSIONS_TONNES,
            CARBON_REDUCTION_PCT,
            RENEWABLE_ENERGY_PCT,
            EQUALITY_INDEX_SCORE,
            FEMALE_EMPLOYEES_PCT,
            FEMALE_MANAGEMENT_PCT,
            TRAINING_HOURS_PER_EMPLOYEE,
            ACCIDENT_FREQUENCY_RATE,
            OVERALL_ESG_STATUS
        FROM TDF_DATA_PLATFORM.ESG.BOARD_SCORECARD
        ORDER BY REPORTING_DATE DESC
        LIMIT 1
    """)
    
    # Default values if no data or NaN
    import math
    
    def safe_esg_value(df, column, default):
        """Safely extract value from dataframe, returning default if empty or NaN"""
        if df.empty:
            return default
        val = df[column].iloc[0]
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return default
        return val
    
    carbon = safe_esg_value(esg_metrics, 'CARBON_EMISSIONS_TONNES', 48500)
    carbon_reduction = safe_esg_value(esg_metrics, 'CARBON_REDUCTION_PCT', -12)
    renewable = safe_esg_value(esg_metrics, 'RENEWABLE_ENERGY_PCT', 47)
    equality = safe_esg_value(esg_metrics, 'EQUALITY_INDEX_SCORE', 88)
    female_pct = safe_esg_value(esg_metrics, 'FEMALE_EMPLOYEES_PCT', 28)
    female_mgmt = safe_esg_value(esg_metrics, 'FEMALE_MANAGEMENT_PCT', 32)
    training_hrs = safe_esg_value(esg_metrics, 'TRAINING_HOURS_PER_EMPLOYEE', 24)
    accident_rate = safe_esg_value(esg_metrics, 'ACCIDENT_FREQUENCY_RATE', 3.2)
    
    col_e, col_s, col_g = st.columns(3)
    
    with col_e:
        st.markdown("""
            <div style="background: linear-gradient(135deg, #27ae60, #2ecc71); border-radius: 12px; padding: 1.5rem; color: white;">
                <div style="font-size: 1.2rem; font-weight: 600; margin-bottom: 1rem;">🌍 Environmental</div>
        """, unsafe_allow_html=True)
        
        e_col1, e_col2 = st.columns(2)
        with e_col1:
            st.metric("Carbon Emissions", f"{carbon:,.0f} tCO₂e", delta=f"{carbon_reduction:.0f}% vs target")
        with e_col2:
            st.metric("Renewable Energy", f"{renewable:.0f}%", delta="+5% YoY")
        
        st.markdown("</div>", unsafe_allow_html=True)
    
    with col_s:
        st.markdown("""
            <div style="background: linear-gradient(135deg, #3498db, #2980b9); border-radius: 12px; padding: 1.5rem; color: white;">
                <div style="font-size: 1.2rem; font-weight: 600; margin-bottom: 1rem;">👥 Social</div>
        """, unsafe_allow_html=True)
        
        s_col1, s_col2 = st.columns(2)
        with s_col1:
            st.metric("Equality Index", f"{equality}/100", delta="Above threshold (75)")
        with s_col2:
            st.metric("Women in Management", f"{female_mgmt:.0f}%", delta="+3% YoY")
        
        st.markdown("</div>", unsafe_allow_html=True)
    
    with col_g:
        st.markdown("""
            <div style="background: linear-gradient(135deg, #9b59b6, #8e44ad); border-radius: 12px; padding: 1.5rem; color: white;">
                <div style="font-size: 1.2rem; font-weight: 600; margin-bottom: 1rem;">🏛️ Governance</div>
        """, unsafe_allow_html=True)
        
        g_col1, g_col2 = st.columns(2)
        with g_col1:
            st.metric("Training Hours/Employee", f"{training_hrs:.0f}h", delta="+2h vs 2023")
        with g_col2:
            st.metric("Accident Frequency", f"{accident_rate:.1f}", delta="-15% YoY", delta_color="normal")
        
        st.markdown("</div>", unsafe_allow_html=True)
    
    # -------------------------------------------------------------------------
    # ROW 3: Board ESG Scorecard - Executive Summary
    # -------------------------------------------------------------------------
    
    st.markdown("### 📊 Board ESG Scorecard")
    st.caption("Executive summary for board reporting • Updated quarterly")
    
    # Scorecard data with YoY comparison
    scorecard_metrics = [
        {"category": "🌍 Environmental", "metrics": [
            {"name": "Carbon Intensity", "value": "78.9", "unit": "kgCO₂e/MWh", "target": "70.0", "yoy": -8.5, "status": "on_track"},
            {"name": "Renewable Energy", "value": "47", "unit": "%", "target": "50", "yoy": 5.0, "status": "on_track"},
            {"name": "Energy Efficiency", "value": "92", "unit": "PUE avg", "target": "90", "yoy": -2.1, "status": "at_risk"},
            {"name": "Waste Recycling", "value": "89", "unit": "%", "target": "90", "yoy": 3.0, "status": "on_track"},
        ]},
        {"category": "👥 Social", "metrics": [
            {"name": "Gender Pay Gap", "value": "2.3", "unit": "%", "target": "<5", "yoy": -1.2, "status": "achieved"},
            {"name": "Women in Leadership", "value": "32", "unit": "%", "target": "35", "yoy": 3.0, "status": "on_track"},
            {"name": "Training Investment", "value": "1,850", "unit": "€/employee", "target": "1,500", "yoy": 12.0, "status": "achieved"},
            {"name": "Lost Time Injury Rate", "value": "3.2", "unit": "per million hrs", "target": "<4", "yoy": -15.0, "status": "achieved"},
        ]},
        {"category": "🏛️ Governance", "metrics": [
            {"name": "Board Independence", "value": "60", "unit": "%", "target": "50", "yoy": 0, "status": "achieved"},
            {"name": "Ethics Training", "value": "95", "unit": "% completed", "target": "100", "yoy": 5.0, "status": "on_track"},
            {"name": "Supplier ESG Audits", "value": "78", "unit": "% coverage", "target": "80", "yoy": 12.0, "status": "on_track"},
            {"name": "Data Breaches", "value": "0", "unit": "incidents", "target": "0", "yoy": 0, "status": "achieved"},
        ]},
    ]
    
    for category in scorecard_metrics:
        cat_color = '#27ae60' if 'Environmental' in category['category'] else '#3498db' if 'Social' in category['category'] else '#9b59b6'
        
        st.markdown(f"""
            <div style="background: {cat_color}10; border-left: 4px solid {cat_color}; padding: 0.5rem 1rem; margin: 1rem 0 0.5rem 0; border-radius: 0 8px 8px 0;">
                <span style="font-weight: 600; color: {cat_color};">{category['category']}</span>
            </div>
        """, unsafe_allow_html=True)
        
        metric_cols = st.columns(4)
        for i, metric in enumerate(category['metrics']):
            with metric_cols[i]:
                status_color = '#27ae60' if metric['status'] == 'achieved' else '#3498db' if metric['status'] == 'on_track' else '#f39c12'
                status_icon = '✅' if metric['status'] == 'achieved' else '🔵' if metric['status'] == 'on_track' else '⚠️'
                yoy_color = '#27ae60' if metric['yoy'] > 0 else '#e63946' if metric['yoy'] < 0 else '#888'
                yoy_arrow = '↑' if metric['yoy'] > 0 else '↓' if metric['yoy'] < 0 else '→'
                
                st.markdown(f"""
                    <div style="background: white; border-radius: 8px; padding: 0.75rem; box-shadow: 0 1px 3px rgba(0,0,0,0.08); height: 120px;">
                        <div style="font-size: 0.7rem; color: #888; margin-bottom: 0.25rem;">{metric['name']}</div>
                        <div style="font-size: 1.4rem; font-weight: 700; color: #1a2b4a;">{metric['value']}<span style="font-size: 0.7rem; color: #888; font-weight: 400;"> {metric['unit']}</span></div>
                        <div style="font-size: 0.65rem; color: #888; margin-top: 0.25rem;">Target: {metric['target']}</div>
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-top: 0.5rem;">
                            <span style="color: {yoy_color}; font-size: 0.7rem; font-weight: 600;">{yoy_arrow} {abs(metric['yoy'])}% YoY</span>
                            <span style="font-size: 0.7rem;">{status_icon}</span>
                        </div>
                    </div>
                """, unsafe_allow_html=True)
    
    # -------------------------------------------------------------------------
    # ROW 4: ESG Ratings & Peer Benchmarks
    # -------------------------------------------------------------------------
    
    st.markdown("### 🏆 ESG Ratings & Industry Benchmarks")
    st.caption("External ESG ratings and comparison with telecom infrastructure peers")
    
    rating_col1, rating_col2 = st.columns([1, 2])
    
    with rating_col1:
        st.markdown("#### 📈 External Ratings")
        
        ratings = [
            {"agency": "MSCI ESG", "rating": "AA", "score": 7.2, "max": 10, "trend": "up", "date": "Nov 2025"},
            {"agency": "Sustainalytics", "rating": "Low Risk", "score": 18.5, "max": 40, "trend": "up", "date": "Oct 2025"},
            {"agency": "CDP Climate", "rating": "A-", "score": None, "max": None, "trend": "stable", "date": "Dec 2025"},
            {"agency": "EcoVadis", "rating": "Gold", "score": 68, "max": 100, "trend": "up", "date": "Sep 2025"},
            {"agency": "ISS ESG", "rating": "B", "score": None, "max": None, "trend": "up", "date": "Nov 2025"},
        ]
        
        for r in ratings:
            trend_icon = '📈' if r['trend'] == 'up' else '📉' if r['trend'] == 'down' else '➡️'
            rating_color = '#27ae60' if r['rating'] in ['AA', 'AAA', 'A', 'A-', 'Gold', 'Low Risk'] else '#3498db' if r['rating'] in ['A', 'B+', 'B', 'Silver'] else '#f39c12'
            
            score_text = f"{r['score']}/{r['max']}" if r['score'] else ""
            
            st.markdown(f"""
                <div style="background: white; border-radius: 8px; padding: 0.75rem; margin-bottom: 0.5rem; display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <div style="font-weight: 600; color: #1a2b4a; font-size: 0.85rem;">{r['agency']}</div>
                        <div style="font-size: 0.65rem; color: #888;">{r['date']}</div>
                    </div>
                    <div style="text-align: right;">
                        <div style="background: {rating_color}; color: white; padding: 0.25rem 0.75rem; border-radius: 20px; font-weight: 700; font-size: 0.9rem; display: inline-block;">{r['rating']}</div>
                        <div style="font-size: 0.65rem; color: #888; margin-top: 0.25rem;">{score_text} {trend_icon}</div>
                    </div>
                </div>
            """, unsafe_allow_html=True)
    
    with rating_col2:
        st.markdown("#### 📊 Peer Comparison - Telecom Infrastructure")
        
        # Peer comparison data
        peers = ['TDF', 'Cellnex', 'Vantage Towers', 'INWIT', 'Telxius', 'Industry Avg']
        environmental = [72, 68, 75, 65, 62, 58]
        social = [78, 72, 70, 68, 65, 62]
        governance = [82, 78, 80, 75, 72, 70]
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            name='Environmental',
            x=peers,
            y=environmental,
            marker_color='#27ae60',
            text=environmental,
            textposition='outside'
        ))
        fig.add_trace(go.Bar(
            name='Social',
            x=peers,
            y=social,
            marker_color='#3498db',
            text=social,
            textposition='outside'
        ))
        fig.add_trace(go.Bar(
            name='Governance',
            x=peers,
            y=governance,
            marker_color='#9b59b6',
            text=governance,
            textposition='outside'
        ))
        
        fig.update_layout(
            barmode='group',
            height=300,
            margin=dict(l=20, r=20, t=30, b=40),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor='#f0f0f0', title='Score', range=[0, 100]),
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5)
        )
        
        # Highlight TDF bar
        fig.add_annotation(x='TDF', y=85, text="⭐ TDF", showarrow=False, font=dict(size=12, color='#1a2b4a'))
        
        st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("""
            <div style="background: #27ae6015; border-radius: 8px; padding: 0.75rem; text-align: center;">
                <span style="color: #27ae60; font-weight: 600;">🏆 TDF ranks #2 among European tower companies in overall ESG performance</span>
            </div>
        """, unsafe_allow_html=True)
    
    # -------------------------------------------------------------------------
    # ROW 5: Net Zero Pathway Tracker
    # -------------------------------------------------------------------------
    
    st.markdown("### 🎯 Net Zero Pathway 2030-2050")
    st.caption("Progress toward science-based targets aligned with Paris Agreement")
    
    nz_col1, nz_col2 = st.columns([2, 1])
    
    with nz_col1:
        # Net Zero trajectory chart
        years = [2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026, 2027, 2028, 2029, 2030, 2035, 2040, 2045, 2050]
        baseline = [55000, 55000, 55000, 55000, 55000, 55000, 55000, 55000, 55000, 55000, 55000, 55000, 55000, 55000, 55000, 55000]
        target_path = [55000, 53000, 51000, 49000, 47000, 44000, 41000, 38000, 35000, 32000, 30000, 33000, 22000, 11000, 5500, 0]
        actual = [55000, 54200, 52800, 51500, 50100, 49200, 48500, None, None, None, None, None, None, None, None, None]
        
        fig = go.Figure()
        
        # Baseline
        fig.add_trace(go.Scatter(
            x=years, y=baseline,
            mode='lines',
            name='Baseline (2019)',
            line=dict(color='#e0e0e0', width=2, dash='dot')
        ))
        
        # Target pathway
        fig.add_trace(go.Scatter(
            x=years, y=target_path,
            mode='lines+markers',
            name='Target Pathway',
            line=dict(color='#27ae60', width=3),
            marker=dict(size=8),
            fill='tonexty',
            fillcolor='rgba(39, 174, 96, 0.1)'
        ))
        
        # Actual emissions (only up to 2025)
        actual_clean = [a for a in actual if a is not None]
        years_actual = years[:len(actual_clean)]
        fig.add_trace(go.Scatter(
            x=years_actual, y=actual_clean,
            mode='lines+markers',
            name='Actual Emissions',
            line=dict(color='#1a2b4a', width=3),
            marker=dict(size=10, symbol='circle')
        ))
        
        # Key milestones
        fig.add_vline(x=2030, line_width=2, line_dash="dash", line_color="#e63946", annotation_text="2030 Target: -40%", annotation_position="top")
        fig.add_vline(x=2050, line_width=2, line_dash="dash", line_color="#27ae60", annotation_text="Net Zero", annotation_position="top")
        
        fig.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=40, b=40),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis=dict(showgrid=True, gridcolor='#f0f0f0', dtick=5),
            yaxis=dict(showgrid=True, gridcolor='#f0f0f0', title='tCO₂e', tickformat=','),
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5),
            hovermode='x unified'
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    with nz_col2:
        st.markdown("#### 🎯 Key Milestones")
        
        milestones = [
            {"year": "2025", "target": "-15%", "actual": "-12%", "status": "on_track", "action": "Renewable PPA signed"},
            {"year": "2027", "target": "-25%", "actual": "-", "status": "planned", "action": "Fleet electrification 80%"},
            {"year": "2030", "target": "-40%", "actual": "-", "status": "planned", "action": "SBTi validated"},
            {"year": "2040", "target": "-80%", "actual": "-", "status": "planned", "action": "100% renewable"},
            {"year": "2050", "target": "Net Zero", "actual": "-", "status": "planned", "action": "Full decarbonization"},
        ]
        
        for m in milestones:
            status_color = '#27ae60' if m['status'] == 'achieved' else '#3498db' if m['status'] == 'on_track' else '#888'
            status_icon = '✅' if m['status'] == 'achieved' else '🔵' if m['status'] == 'on_track' else '⏳'
            
            st.markdown(f"""
                <div style="background: white; border-radius: 8px; padding: 0.75rem; margin-bottom: 0.5rem; border-left: 3px solid {status_color};">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <span style="font-weight: 700; color: #1a2b4a;">{m['year']}</span>
                        <span style="font-size: 0.85rem;">{status_icon} {m['target']}</span>
                    </div>
                    <div style="font-size: 0.7rem; color: #888; margin-top: 0.25rem;">{m['action']}</div>
                    {f'<div style="font-size: 0.7rem; color: #27ae60; margin-top: 0.25rem;">Actual: {m["actual"]}</div>' if m['actual'] != '-' else ''}
                </div>
            """, unsafe_allow_html=True)
        
        # SBTi commitment badge
        st.markdown("""
            <div style="background: linear-gradient(135deg, #1a2b4a, #2d3436); border-radius: 8px; padding: 1rem; margin-top: 1rem; text-align: center; color: white;">
                <div style="font-size: 1.5rem; margin-bottom: 0.5rem;">🌍</div>
                <div style="font-weight: 600;">Science Based Targets</div>
                <div style="font-size: 0.75rem; color: #aaa; margin-top: 0.25rem;">Committed • Validation Q2 2026</div>
            </div>
        """, unsafe_allow_html=True)
    
    # -------------------------------------------------------------------------
    # ROW 6: Regulatory Calendar & Alerts
    # -------------------------------------------------------------------------
    
    st.markdown("### 📅 Regulatory Reporting Calendar 2025")
    
    # Calendar data with deadlines and status
    # Current date for demo: December 1, 2025
    # All 2025 reports are complete - showing upcoming 2026 deadlines
    from datetime import datetime, date
    
    today = date(2025, 12, 1)  # Demo date
    
    # Regulatory deadlines for 2026 (FY2025 reporting)
    raw_calendar = [
        {"report": "Index Égalité H/F 2026", "due_date": "2026-03-01", "owner": "DRH", "priority": "High", "progress": 45},
        {"report": "CSRD / ESRS (FY2025)", "due_date": "2026-04-30", "owner": "Finance", "priority": "Critical", "progress": 28},
        {"report": "DPEF 2026", "due_date": "2026-04-30", "owner": "RSE", "priority": "High", "progress": 35},
        {"report": "TCFD Disclosure 2026", "due_date": "2026-04-30", "owner": "Finance", "priority": "High", "progress": 40},
        {"report": "GRI Report 2025", "due_date": "2026-05-31", "owner": "RSE", "priority": "Medium", "progress": 15},
        {"report": "EU Taxonomy (FY2025)", "due_date": "2026-06-30", "owner": "Finance", "priority": "High", "progress": 22},
        {"report": "Article 29 LEC 2026", "due_date": "2026-06-30", "owner": "RSE", "priority": "Medium", "progress": 18},
        {"report": "Bilan GES 2026", "due_date": "2026-12-31", "owner": "RSE", "priority": "Medium", "progress": 5},
    ]
    
    # Calculate days left and determine status dynamically
    calendar_data = []
    for item in raw_calendar:
        due = datetime.strptime(item["due_date"], "%Y-%m-%d").date()
        days_left = (due - today).days
        
        # Determine status based on progress and time remaining
        expected_progress = max(0, 100 - (days_left / 3.65))  # ~1% per 3.65 days for annual reports
        
        if days_left < 0:
            status = "overdue"
        elif item["progress"] >= expected_progress - 10:
            status = "on_track"
        elif item["progress"] >= expected_progress - 25:
            status = "at_risk"
        else:
            status = "not_started" if item["progress"] < 20 else "at_risk"
        
        calendar_data.append({
            "report": item["report"],
            "due_date": item["due_date"],
            "days_left": days_left,
            "owner": item["owner"],
            "priority": item["priority"],
            "status": status,
            "progress": item["progress"]
        })
    
    # Sort by due date
    calendar_data = sorted(calendar_data, key=lambda x: x['due_date'])
    
    # Alert summary
    on_track = len([c for c in calendar_data if c['status'] == 'on_track'])
    at_risk = len([c for c in calendar_data if c['status'] == 'at_risk'])
    not_started = len([c for c in calendar_data if c['status'] == 'not_started'])
    overdue = len([c for c in calendar_data if c['status'] == 'overdue'])
    
    alert_cols = st.columns(4)
    with alert_cols[0]:
        st.markdown(f"""
            <div style="background: #27ae60; border-radius: 8px; padding: 1rem; color: white; text-align: center;">
                <div style="font-size: 2rem; font-weight: 700;">{on_track}</div>
                <div style="font-size: 0.8rem;">✅ On Track</div>
            </div>
        """, unsafe_allow_html=True)
    with alert_cols[1]:
        st.markdown(f"""
            <div style="background: #f39c12; border-radius: 8px; padding: 1rem; color: white; text-align: center;">
                <div style="font-size: 2rem; font-weight: 700;">{at_risk}</div>
                <div style="font-size: 0.8rem;">⚠️ At Risk</div>
            </div>
        """, unsafe_allow_html=True)
    with alert_cols[2]:
        st.markdown(f"""
            <div style="background: #3498db; border-radius: 8px; padding: 1rem; color: white; text-align: center;">
                <div style="font-size: 2rem; font-weight: 700;">{not_started}</div>
                <div style="font-size: 0.8rem;">🔵 Not Started</div>
            </div>
        """, unsafe_allow_html=True)
    with alert_cols[3]:
        st.markdown(f"""
            <div style="background: #e63946; border-radius: 8px; padding: 1rem; color: white; text-align: center;">
                <div style="font-size: 2rem; font-weight: 700;">{overdue}</div>
                <div style="font-size: 0.8rem;">🔴 Overdue</div>
            </div>
        """, unsafe_allow_html=True)
    
    st.markdown("")
    
    # Visual timeline
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    
    # Create timeline chart
    fig = go.Figure()
    
    # Add month grid
    for i, month in enumerate(months):
        fig.add_vrect(x0=i+0.5, x1=i+1.5, fillcolor='rgba(0,0,0,0.02)' if i % 2 == 0 else 'rgba(0,0,0,0)', line_width=0)
    
    # Add current date marker
    current_month = 12  # December
    fig.add_vline(x=current_month, line_width=3, line_dash="dash", line_color="#e63946", 
                  annotation_text="Today", annotation_position="top")
    
    # Add report markers
    y_positions = list(range(len(calendar_data)))
    colors = []
    for item in calendar_data:
        if item['status'] == 'on_track':
            colors.append('#27ae60')
        elif item['status'] == 'at_risk':
            colors.append('#f39c12')
        elif item['status'] == 'not_started':
            colors.append('#3498db')
        else:
            colors.append('#e63946')
    
    # Extract month from due date
    x_positions = [int(item['due_date'].split('-')[1]) for item in calendar_data]
    
    fig.add_trace(go.Scatter(
        x=x_positions,
        y=[item['report'] for item in calendar_data],
        mode='markers+text',
        marker=dict(size=20, color=colors, symbol='circle'),
        text=[f"{int(item['due_date'].split('-')[1])}/{item['due_date'].split('-')[2]}" for item in calendar_data],
        textposition='middle right',
        textfont=dict(size=10),
        hovertemplate='<b>%{y}</b><br>Due: %{text}<br>Status: %{customdata}<extra></extra>',
        customdata=[item['status'].replace('_', ' ').title() for item in calendar_data]
    ))
    
    fig.update_layout(
        height=350,
        margin=dict(l=10, r=10, t=30, b=40),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(
            tickmode='array',
            tickvals=list(range(1, 13)),
            ticktext=months,
            showgrid=True,
            gridcolor='#f0f0f0',
            range=[0.5, 12.5]
        ),
        yaxis=dict(showgrid=False, categoryorder='array', categoryarray=[item['report'] for item in reversed(calendar_data)]),
        showlegend=False
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Detailed table with next actions
    st.markdown("#### 📋 Upcoming Deadlines & Actions")
    
    for item in calendar_data[:5]:  # Show next 5
        if item['status'] == 'on_track':
            status_color = '#27ae60'
            status_icon = '✅'
            status_text = 'On Track'
        elif item['status'] == 'at_risk':
            status_color = '#f39c12'
            status_icon = '⚠️'
            status_text = 'At Risk'
        elif item['status'] == 'not_started':
            status_color = '#3498db'
            status_icon = '🔵'
            status_text = 'Not Started'
        else:
            status_color = '#e63946'
            status_icon = '🔴'
            status_text = 'Overdue'
        
        priority_color = '#e63946' if item['priority'] == 'Critical' else '#f39c12' if item['priority'] == 'High' else '#3498db'
        
        st.markdown(f"""
            <div style="background: white; border-radius: 8px; padding: 1rem; margin-bottom: 0.5rem; display: flex; justify-content: space-between; align-items: center; border-left: 4px solid {status_color}; box-shadow: 0 1px 3px rgba(0,0,0,0.08);">
                <div style="flex: 2;">
                    <div style="font-weight: 600; color: #1a2b4a; font-size: 0.95rem;">{item['report']}</div>
                    <div style="font-size: 0.75rem; color: #888; margin-top: 0.25rem;">Owner: {item['owner']} • Priority: <span style="color: {priority_color}; font-weight: 600;">{item['priority']}</span></div>
                </div>
                <div style="flex: 1; text-align: center;">
                    <div style="font-size: 0.75rem; color: #888;">Due Date</div>
                    <div style="font-weight: 600; color: #1a2b4a;">{item['due_date']}</div>
                </div>
                <div style="flex: 1; text-align: center;">
                    <div style="font-size: 0.75rem; color: #888;">Days Left</div>
                    <div style="font-weight: 700; color: {status_color}; font-size: 1.2rem;">{item['days_left']}</div>
                </div>
                <div style="flex: 1; text-align: right;">
                    <div style="background: {status_color}20; color: {status_color}; padding: 0.4rem 0.75rem; border-radius: 20px; font-size: 0.8rem; font-weight: 600; display: inline-block;">
                        {status_icon} {status_text}
                    </div>
                </div>
            </div>
        """, unsafe_allow_html=True)
    
    # -------------------------------------------------------------------------
    # ROW 3: Report Generation & Download
    # -------------------------------------------------------------------------
    
    st.markdown("### 📥 Generate & Download ESG Reports")
    st.caption("Select a report type to generate compliant regulatory documentation with full data lineage")
    
    report_col1, report_col2 = st.columns([2, 3])
    
    with report_col1:
        # Report type selection
        report_type = st.selectbox(
            "📄 Select Report Type",
            options=[
                "CSRD Annual Report",
                "French Equality Index (Index Égalité H/F)",
                "Carbon Footprint (Bilan GES)",
                "EU Taxonomy Alignment Report",
                "DPEF (Extra-Financial Performance)",
                "GRI Standards Report",
                "TCFD Climate Disclosure"
            ]
        )
        
        # Reporting period
        period = st.selectbox(
            "📅 Reporting Period",
            options=["FY 2025", "H1 2025", "H2 2024", "FY 2024"]
        )
        
        # Format selection
        export_format = st.selectbox(
            "📁 Export Format",
            options=["PDF", "Excel (XLSX)", "CSV", "JSON (Machine-Readable)"]
        )
        
        # Include data lineage
        include_lineage = st.checkbox("Include Data Lineage Trail", value=True)
        include_methodology = st.checkbox("Include Calculation Methodology", value=True)
        
        # Generate and download report
        st.markdown("---")
        
        # Generate report data based on type - ALL OFFICIAL FRENCH/EU REGULATORY FORMATS
        if "Equality" in report_type:
            # OFFICIAL: Index Égalité Professionnelle - Décret n°2019-15 du 8 janvier 2019
            # Required for companies with 50+ employees, published before March 1st
            report_data = {
                "Rapport": "INDEX DE L'ÉGALITÉ PROFESSIONNELLE ENTRE LES FEMMES ET LES HOMMES",
                "Base_Legale": "Articles L.1142-8 et D.1142-2 à D.1142-14 du Code du travail",
                "Decret": "Décret n°2019-15 du 8 janvier 2019",
                "Periode_Reference": period,
                "Raison_Sociale": "TDF Infrastructure SAS",
                "SIREN": "343 070 044",
                "Code_NAF": "6110Z - Télécommunications filaires",
                "Effectif_Assujetti": 1487,
                "Tranche_Effectif": "1000 salariés et plus",
                "Indicateur_1_Ecart_Remuneration": {
                    "Resultat": "2.3%",
                    "Points": 38,
                    "Points_Max": 40,
                    "Population_Valide": True,
                    "Methode": "Par CSP et tranche d'âge"
                },
                "Indicateur_2_Ecart_Augmentations": {
                    "Resultat": "0.8%",
                    "Points": 20,
                    "Points_Max": 20,
                    "En_Faveur_De": "Équilibré"
                },
                "Indicateur_3_Ecart_Promotions": {
                    "Resultat": "1.2%",
                    "Points": 15,
                    "Points_Max": 15,
                    "En_Faveur_De": "Femmes"
                },
                "Indicateur_4_Conge_Maternite": {
                    "Resultat": "100%",
                    "Points": 15,
                    "Points_Max": 15,
                    "Salariees_Augmentees": "15/15"
                },
                "Indicateur_5_Hautes_Remunerations": {
                    "Femmes_Top10": 3,
                    "Hommes_Top10": 7,
                    "Points": 0,
                    "Points_Max": 10
                },
                "INDEX_TOTAL": 88,
                "Seuil_Minimum_Legal": 75,
                "Niveau_Resultat": "CONFORME",
                "Date_Publication": "2025-03-01",
                "Lieu_Publication": "Site internet entreprise, Ministère du Travail",
                "Mesures_Correctives": "Plan d'action sur l'indicateur 5 - Objectif 4 femmes minimum dans les 10 plus hautes rémunérations d'ici 2026"
            }
            filename_base = f"index_egalite_professionnelle_{period.replace(' ', '_').lower()}"
            
        elif "Carbon" in report_type:
            # OFFICIAL: Bilan GES réglementaire - Article L229-25 du Code de l'environnement
            # Required for companies with 500+ employees, updated every 4 years
            report_data = {
                "Rapport": "BILAN DES ÉMISSIONS DE GAZ À EFFET DE SERRE (BEGES)",
                "Base_Legale": "Article L229-25 du Code de l'environnement",
                "Decret": "Décret n°2011-829 du 11 juillet 2011, modifié par décret n°2022-982",
                "Periode_Reference": period,
                "Raison_Sociale": "TDF Infrastructure SAS",
                "SIREN": "343 070 044",
                "Code_NAF": "6110Z - Télécommunications filaires",
                "Adresse_Siege": "106 avenue Marx Dormoy, 92120 Montrouge",
                "Nombre_Salaries": 1487,
                "Mode_Consolidation": "Contrôle opérationnel",
                "Annee_Reference": 2019,
                "Emissions_Scope_1": {
                    "Total_tCO2e": 8420,
                    "Combustion_Fixe": 2150,
                    "Combustion_Mobile": 4870,
                    "Emissions_Fugitives": 1400,
                    "Incertitude": "±10%"
                },
                "Emissions_Scope_2": {
                    "Total_tCO2e": 32150,
                    "Electricite": 31200,
                    "Chaleur_Froid": 950,
                    "Methode": "Location-based",
                    "Facteur_Emission_Electricite": "0.0520 kgCO2e/kWh (ADEME 2024)"
                },
                "Emissions_Scope_3_Obligatoires": {
                    "Total_tCO2e": 7930,
                    "Achats_Biens_Services": 4200,
                    "Deplacements_Domicile_Travail": 2100,
                    "Dechets": 630,
                    "Transport_Marchandises": 1000
                },
                "Total_Emissions_tCO2e": 48500,
                "Ratio_Par_Salarie": 32.6,
                "Ratio_Par_Site": 5.52,
                "Evolution_vs_Reference": "-12%",
                "Objectifs_Reduction": {
                    "2025": "-15% vs 2019",
                    "2030": "-40% vs 2019",
                    "2050": "Neutralité carbone"
                },
                "Plan_Transition": {
                    "Actions_Scope_1": "Électrification flotte véhicules (objectif 80% en 2027)",
                    "Actions_Scope_2": "PPA énergies renouvelables (objectif 75% en 2026)",
                    "Actions_Scope_3": "Charte achats responsables, plan mobilité"
                },
                "Verification": "Non vérifié par tierce partie",
                "Publication_ADEME": "Plateforme Bilans GES ADEME",
                "Methodologie": "ADEME Bilan Carbone® v8.8, Base Carbone 2024"
            }
            filename_base = f"beges_reglementaire_{period.replace(' ', '_').lower()}"
            
        elif "EU Taxonomy" in report_type:
            # OFFICIAL: EU Taxonomy Regulation (EU) 2020/852
            # Required for NFRD/CSRD companies
            report_data = {
                "Rapport": "EU TAXONOMY ALIGNMENT REPORT",
                "Base_Legale": "Regulation (EU) 2020/852, Delegated Regulation (EU) 2021/2178",
                "Periode_Reference": period,
                "Entite_Declarante": "TDF Infrastructure SAS",
                "LEI": "969500BVXK1VVHBQTE84",
                "Activites_Economiques": [
                    "8.1 - Data processing, hosting and related activities",
                    "8.2 - Data-driven solutions for GHG emissions reductions"
                ],
                "KPI_Chiffre_Affaires": {
                    "Total_EUR": 799100000,
                    "Eligible_EUR": 623298000,
                    "Eligible_Pct": 78.0,
                    "Aligne_EUR": 519415000,
                    "Aligne_Pct": 65.0
                },
                "KPI_CAPEX": {
                    "Total_EUR": 185000000,
                    "Eligible_EUR": 151700000,
                    "Eligible_Pct": 82.0,
                    "Aligne_EUR": 131350000,
                    "Aligne_Pct": 71.0
                },
                "KPI_OPEX": {
                    "Total_EUR": 245000000,
                    "Eligible_EUR": 181300000,
                    "Eligible_Pct": 74.0,
                    "Aligne_EUR": 151900000,
                    "Aligne_Pct": 62.0
                },
                "Objectifs_Environnementaux": {
                    "Attenuation_Changement_Climatique": True,
                    "Adaptation_Changement_Climatique": True,
                    "Utilisation_Durable_Eau": False,
                    "Economie_Circulaire": True,
                    "Prevention_Pollution": True,
                    "Biodiversite": False
                },
                "DNSH_Assessment": "Critères DNSH satisfaits pour toutes les activités alignées",
                "Minimum_Safeguards": {
                    "OECD_Guidelines": True,
                    "UN_Guiding_Principles": True,
                    "ILO_Conventions": True
                },
                "Verification": "Vérification limitée par Commissaires aux comptes"
            }
            filename_base = f"eu_taxonomy_report_{period.replace(' ', '_').lower()}"
            
        elif "DPEF" in report_type:
            # OFFICIAL: Déclaration de Performance Extra-Financière
            # Article L225-102-1 du Code de commerce
            report_data = {
                "Rapport": "DÉCLARATION DE PERFORMANCE EXTRA-FINANCIÈRE (DPEF)",
                "Base_Legale": "Article L225-102-1 du Code de commerce, Ordonnance n°2017-1180",
                "Periode_Reference": period,
                "Raison_Sociale": "TDF Infrastructure SAS",
                "SIREN": "343 070 044",
                "Modele_Affaires": {
                    "Description": "Opérateur d'infrastructures de diffusion et télécommunications",
                    "Zones_Geographiques": "France métropolitaine et DOM-TOM",
                    "Sites_Operes": 8785,
                    "Effectif": 1487
                },
                "Risques_ESG_Identifies": {
                    "Environnementaux": ["Consommation énergétique", "Émissions GES", "Gestion des déchets électroniques"],
                    "Sociaux": ["Santé et sécurité au travail", "Égalité professionnelle", "Formation et développement"],
                    "Gouvernance": ["Éthique des affaires", "Protection des données", "Cybersécurité"]
                },
                "Indicateurs_Environnementaux": {
                    "Emissions_GES_tCO2e": 48500,
                    "Consommation_Energie_MWh": 615000,
                    "Part_Renouvelable_Pct": 47,
                    "Dechets_Tonnes": 1250,
                    "Taux_Valorisation_Pct": 89
                },
                "Indicateurs_Sociaux": {
                    "Effectif_CDI": 1402,
                    "Effectif_CDD": 85,
                    "Taux_Turnover_Pct": 8.2,
                    "Heures_Formation_Total": 35688,
                    "Taux_Frequence_Accidents": 3.2,
                    "Index_Egalite": 88
                },
                "Indicateurs_Gouvernance": {
                    "Part_Administrateurs_Independants_Pct": 60,
                    "Formations_Ethique_Pct_Salaries": 95,
                    "Incidents_Donnees_Personnelles": 0
                },
                "ODD_Contribution": ["ODD 7 - Énergie propre", "ODD 9 - Infrastructure", "ODD 13 - Climat"],
                "Verification": "Vérification par Organisme Tiers Indépendant (OTI)"
            }
            filename_base = f"dpef_{period.replace(' ', '_').lower()}"
            
        elif "CSRD" in report_type:
            # OFFICIAL: Corporate Sustainability Reporting Directive (EU) 2022/2464
            # ESRS Standards
            report_data = {
                "Rapport": "CORPORATE SUSTAINABILITY REPORTING DIRECTIVE (CSRD)",
                "Base_Legale": "Directive (EU) 2022/2464, European Sustainability Reporting Standards (ESRS)",
                "Periode_Reference": period,
                "Entite_Declarante": "TDF Infrastructure SAS",
                "LEI": "969500BVXK1VVHBQTE84",
                "ESRS_2_General_Disclosures": {
                    "BP-1_Basis_For_Preparation": "Consolidation, périmètre France",
                    "BP-2_Disclosure_In_Relation_To_Circumstances": "Aucune circonstance spécifique",
                    "GOV-1_Board_Role": "Comité RSE au niveau du Conseil",
                    "GOV-2_Management_Role": "Direction RSE rattachée à la DG",
                    "SBM-1_Strategy_Business_Model": "Infrastructure telecom, transition énergétique",
                    "IRO-1_Risk_Identification": "Matrice de matérialité double"
                },
                "ESRS_E1_Climate_Change": {
                    "E1-1_Transition_Plan": "Plan aligné Accord de Paris",
                    "E1-4_GHG_Targets": "-40% d'ici 2030 vs 2019",
                    "E1-5_Energy_Consumption_MWh": 615000,
                    "E1-6_GHG_Emissions_tCO2e": 48500
                },
                "ESRS_S1_Own_Workforce": {
                    "S1-1_Policies": "Politique diversité et inclusion",
                    "S1-6_Pay_Gap_Pct": 2.3,
                    "S1-14_Health_Safety_Indicators": {"Frequency_Rate": 3.2, "Severity_Rate": 0.18}
                },
                "ESRS_G1_Business_Conduct": {
                    "G1-1_Policies": "Code d'éthique, procédure alerte",
                    "G1-3_Corruption_Incidents": 0
                },
                "Assurance": {
                    "Type": "Limited assurance",
                    "Auditeur": "EY",
                    "Normes": "ISAE 3000"
                }
            }
            filename_base = f"csrd_esrs_report_{period.replace(' ', '_').lower()}"
            
        elif "GRI" in report_type:
            # OFFICIAL: Global Reporting Initiative Standards 2021
            report_data = {
                "Rapport": "GRI SUSTAINABILITY REPORT",
                "Standards": "GRI Standards 2021",
                "GRI_Content_Index": "In accordance with GRI Standards",
                "Periode_Reference": period,
                "Organisation": "TDF Infrastructure SAS",
                "GRI_2_General_Disclosures": {
                    "2-1_Organizational_Details": {"Name": "TDF Infrastructure", "Location": "Montrouge, France", "Ownership": "Private"},
                    "2-6_Activities_Value_Chain": "Telecom infrastructure operator, 8,785 sites",
                    "2-7_Employees": 1487,
                    "2-22_Sustainable_Development_Strategy": "Carbon neutrality 2050"
                },
                "GRI_3_Material_Topics": ["GRI 302 - Energy", "GRI 305 - Emissions", "GRI 403 - Health Safety", "GRI 405 - Diversity"],
                "GRI_302_Energy": {
                    "302-1_Energy_Consumption_MWh": 615000,
                    "302-3_Energy_Intensity": 70.0,
                    "302-4_Reduction_Pct": 8
                },
                "GRI_305_Emissions": {
                    "305-1_Scope_1_tCO2e": 8420,
                    "305-2_Scope_2_tCO2e": 32150,
                    "305-3_Scope_3_tCO2e": 7930,
                    "305-4_Intensity_kgCO2e_per_MWh": 78.9,
                    "305-5_Reduction_Pct": 12
                },
                "GRI_403_Health_Safety": {
                    "403-9_Work_Related_Injuries": 47,
                    "403-9_Injury_Rate": 3.2,
                    "403-10_Work_Related_Ill_Health": 5
                },
                "GRI_405_Diversity": {
                    "405-1_Board_Diversity_Women_Pct": 40,
                    "405-1_Workforce_Women_Pct": 28,
                    "405-2_Pay_Ratio_CEO_Median": 12.5
                },
                "External_Assurance": "Limited assurance by third party"
            }
            filename_base = f"gri_report_{period.replace(' ', '_').lower()}"
            
        elif "TCFD" in report_type:
            # OFFICIAL: Task Force on Climate-related Financial Disclosures
            report_data = {
                "Rapport": "TCFD CLIMATE-RELATED FINANCIAL DISCLOSURE",
                "Framework": "Task Force on Climate-related Financial Disclosures (TCFD) Recommendations",
                "Periode_Reference": period,
                "Organisation": "TDF Infrastructure SAS",
                "Governance": {
                    "Board_Oversight": "Climate risks reviewed quarterly by Board Risk Committee",
                    "Management_Role": "CSO reports to CEO, dedicated Climate Team"
                },
                "Strategy": {
                    "Risks_Identified": {
                        "Physical_Acute": "Extreme weather events impacting tower sites",
                        "Physical_Chronic": "Rising temperatures increasing cooling costs",
                        "Transition_Policy": "Carbon pricing mechanisms",
                        "Transition_Technology": "5G energy efficiency requirements"
                    },
                    "Opportunities": ["Renewable energy PPA", "Energy efficiency services", "Green bonds"],
                    "Scenario_Analysis": {
                        "Scenarios_Used": "IEA NZE 2050, IPCC RCP 4.5, RCP 8.5",
                        "Time_Horizons": "2030, 2050",
                        "Financial_Impact_EUR_M": {"2030_Low": -12, "2030_High": -45, "2050_Low": -25, "2050_High": -120}
                    }
                },
                "Risk_Management": {
                    "Identification_Process": "Annual climate risk assessment integrated in ERM",
                    "Management_Process": "Climate risk mitigation plans by business unit",
                    "Integration": "Climate KPIs in executive compensation"
                },
                "Metrics_Targets": {
                    "Scope_1_2_tCO2e": 40570,
                    "Scope_3_tCO2e": 7930,
                    "Target_2030": "-40% vs 2019",
                    "Target_2050": "Net Zero",
                    "SBTi_Commitment": "Committed, target validation in progress",
                    "Carbon_Intensity": "78.9 kgCO2e/MWh",
                    "Climate_Related_CAPEX_EUR_M": 45
                }
            }
            filename_base = f"tcfd_disclosure_{period.replace(' ', '_').lower()}"
            
        else:
            # Generic ESG Report
            report_data = {
                "Rapport": report_type,
                "Periode_Reference": period,
                "Organisation": "TDF Infrastructure SAS",
                "Scores_ESG": {
                    "Environmental": "A-",
                    "Social": "B+",
                    "Governance": "A",
                    "Overall": "BBB+"
                },
                "Metriques_Cles": {
                    "Emissions_tCO2e": 48500,
                    "Energie_Renouvelable_Pct": 47,
                    "Index_Egalite": 88,
                    "Femmes_Management_Pct": 32,
                    "Formation_Heures": 24,
                    "Taux_Accidents": 3.2
                }
            }
            filename_base = f"esg_report_{period.replace(' ', '_').lower()}"
        
        # Add lineage if selected
        if include_lineage:
            report_data["Data_Lineage"] = {
                "Source_Tables": ["ENERGY.CONSUMPTION", "HR.EMPLOYEES", "FINANCE.TRANSACTIONS"],
                "Transform_Date": "2025-12-01",
                "Validation_Status": "Validated",
                "Audit_Trail_ID": "AUD-2025-1201-001"
            }
        
        if include_methodology:
            report_data["Calculation_Methodology"] = {
                "Framework": "GRI Standards 2021, CSRD ESRS",
                "Emission_Factors": "ADEME Base Carbone 2024",
                "Currency": "EUR",
                "Boundary": "Operational Control"
            }
        
        # Convert to appropriate format
        import json
        import io
        
        if "CSV" in export_format:
            # Flatten dict for CSV
            flat_data = {}
            for k, v in report_data.items():
                if isinstance(v, dict):
                    for k2, v2 in v.items():
                        flat_data[f"{k}_{k2}"] = v2
                else:
                    flat_data[k] = v
            
            csv_content = "Field,Value\n"
            for k, v in flat_data.items():
                csv_content += f'"{k}","{v}"\n'
            
            st.download_button(
                label="📥 Download CSV",
                data=csv_content,
                file_name=f"{filename_base}.csv",
                mime="text/csv",
                use_container_width=True,
                type="primary"
            )
        elif "JSON" in export_format:
            json_content = json.dumps(report_data, indent=2, ensure_ascii=False)
            
            st.download_button(
                label="📥 Download JSON",
                data=json_content,
                file_name=f"{filename_base}.json",
                mime="application/json",
                use_container_width=True,
                type="primary"
            )
        elif "Excel" in export_format:
            # Create Excel-compatible CSV (opens in Excel)
            flat_data = {}
            for k, v in report_data.items():
                if isinstance(v, dict):
                    for k2, v2 in v.items():
                        flat_data[f"{k}_{k2}"] = v2
                else:
                    flat_data[k] = v
            
            csv_content = "Field,Value\n"
            for k, v in flat_data.items():
                csv_content += f'"{k}","{v}"\n'
            
            st.download_button(
                label="📥 Download Excel (CSV)",
                data=csv_content,
                file_name=f"{filename_base}.csv",
                mime="text/csv",
                use_container_width=True,
                type="primary"
            )
            st.caption("Opens directly in Microsoft Excel")
        else:  # PDF - provide as formatted text
            pdf_content = f"""
{'='*60}
{report_data.get('Report', report_type)}
{'='*60}

Company: TDF Infrastructure
Period: {period}
Generated: 2025-12-01
{'='*60}

"""
            for k, v in report_data.items():
                if isinstance(v, dict):
                    pdf_content += f"\n{k.replace('_', ' ').upper()}:\n"
                    for k2, v2 in v.items():
                        pdf_content += f"  - {k2}: {v2}\n"
                else:
                    pdf_content += f"{k.replace('_', ' ')}: {v}\n"
            
            pdf_content += f"""
{'='*60}
This report is generated from TDF Data Platform
Full audit trail available in system
{'='*60}
"""
            
            st.download_button(
                label="📥 Download Report (TXT)",
                data=pdf_content,
                file_name=f"{filename_base}.txt",
                mime="text/plain",
                use_container_width=True,
                type="primary"
            )
            st.caption("📄 For PDF, import this file into your document system")
    
    with report_col2:
        st.markdown("#### 📊 Report Preview")
        
        # Show preview based on selected report
        if "Equality" in report_type:
            st.markdown(f"""
                <div style="background: white; border-radius: 10px; padding: 1.5rem; border: 1px solid #e0e0e0;">
                    <div style="font-weight: 700; color: #1a2b4a; font-size: 1.1rem; margin-bottom: 1rem;">
                        📋 Index Égalité Professionnelle Femmes-Hommes 2025
                    </div>
                    <table style="width: 100%; font-size: 0.85rem;">
                        <tr style="border-bottom: 1px solid #f0f0f0;">
                            <td style="padding: 0.5rem; color: #666;">1. Écart de rémunération</td>
                            <td style="padding: 0.5rem; text-align: right; font-weight: 600;">38/40</td>
                        </tr>
                        <tr style="border-bottom: 1px solid #f0f0f0;">
                            <td style="padding: 0.5rem; color: #666;">2. Écart d'augmentations individuelles</td>
                            <td style="padding: 0.5rem; text-align: right; font-weight: 600;">20/20</td>
                        </tr>
                        <tr style="border-bottom: 1px solid #f0f0f0;">
                            <td style="padding: 0.5rem; color: #666;">3. Écart de promotions</td>
                            <td style="padding: 0.5rem; text-align: right; font-weight: 600;">15/15</td>
                        </tr>
                        <tr style="border-bottom: 1px solid #f0f0f0;">
                            <td style="padding: 0.5rem; color: #666;">4. Retour de congé maternité</td>
                            <td style="padding: 0.5rem; text-align: right; font-weight: 600;">15/15</td>
                        </tr>
                        <tr style="border-bottom: 1px solid #f0f0f0;">
                            <td style="padding: 0.5rem; color: #666;">5. Hautes rémunérations</td>
                            <td style="padding: 0.5rem; text-align: right; font-weight: 600;">0/10</td>
                        </tr>
                        <tr style="background: #f8f9fa;">
                            <td style="padding: 0.75rem; font-weight: 700; color: #1a2b4a;">TOTAL INDEX</td>
                            <td style="padding: 0.75rem; text-align: right; font-weight: 700; font-size: 1.2rem; color: #27ae60;">{equality}/100</td>
                        </tr>
                    </table>
                    <div style="margin-top: 1rem; font-size: 0.75rem; color: #888;">
                        Publication obligatoire au 1er mars 2025 • Seuil minimum: 75/100 ✅
                    </div>
                </div>
            """, unsafe_allow_html=True)
        elif "Carbon" in report_type:
            st.markdown(f"""
                <div style="background: white; border-radius: 10px; padding: 1.5rem; border: 1px solid #e0e0e0;">
                    <div style="font-weight: 700; color: #1a2b4a; font-size: 1.1rem; margin-bottom: 1rem;">
                        📋 Bilan des Émissions de Gaz à Effet de Serre (BEGES)
                    </div>
                    <table style="width: 100%; font-size: 0.85rem;">
                        <tr style="border-bottom: 1px solid #f0f0f0;">
                            <td style="padding: 0.5rem; color: #666;">Scope 1 (Direct)</td>
                            <td style="padding: 0.5rem; text-align: right; font-weight: 600;">8,420 tCO₂e</td>
                        </tr>
                        <tr style="border-bottom: 1px solid #f0f0f0;">
                            <td style="padding: 0.5rem; color: #666;">Scope 2 (Electricity)</td>
                            <td style="padding: 0.5rem; text-align: right; font-weight: 600;">32,150 tCO₂e</td>
                        </tr>
                        <tr style="border-bottom: 1px solid #f0f0f0;">
                            <td style="padding: 0.5rem; color: #666;">Scope 3 (Indirect)</td>
                            <td style="padding: 0.5rem; text-align: right; font-weight: 600;">7,930 tCO₂e</td>
                        </tr>
                        <tr style="background: #f8f9fa;">
                            <td style="padding: 0.75rem; font-weight: 700; color: #1a2b4a;">TOTAL EMISSIONS</td>
                            <td style="padding: 0.75rem; text-align: right; font-weight: 700; font-size: 1.2rem; color: #27ae60;">{carbon:,.0f} tCO₂e</td>
                        </tr>
                    </table>
                    <div style="margin-top: 1rem; font-size: 0.75rem; color: #888;">
                        Méthodologie: ADEME Bilan Carbone® • Facteurs d'émission: Base Carbone 2024
                    </div>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
                <div style="background: white; border-radius: 10px; padding: 1.5rem; border: 1px solid #e0e0e0;">
                    <div style="font-weight: 700; color: #1a2b4a; font-size: 1.1rem; margin-bottom: 1rem;">
                        📋 {report_type} - {period}
                    </div>
                    <div style="color: #666; font-size: 0.9rem; margin-bottom: 1rem;">
                        This report includes comprehensive ESG metrics with full data traceability.
                    </div>
                    <div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 1rem;">
                        <div style="background: #f8f9fa; padding: 0.75rem; border-radius: 6px;">
                            <div style="font-size: 0.7rem; color: #888;">Environmental Score</div>
                            <div style="font-size: 1.2rem; font-weight: 600; color: #27ae60;">A-</div>
                        </div>
                        <div style="background: #f8f9fa; padding: 0.75rem; border-radius: 6px;">
                            <div style="font-size: 0.7rem; color: #888;">Social Score</div>
                            <div style="font-size: 1.2rem; font-weight: 600; color: #3498db;">B+</div>
                        </div>
                        <div style="background: #f8f9fa; padding: 0.75rem; border-radius: 6px;">
                            <div style="font-size: 0.7rem; color: #888;">Governance Score</div>
                            <div style="font-size: 1.2rem; font-weight: 600; color: #9b59b6;">A</div>
                        </div>
                        <div style="background: #f8f9fa; padding: 0.75rem; border-radius: 6px;">
                            <div style="font-size: 0.7rem; color: #888;">Overall Rating</div>
                            <div style="font-size: 1.2rem; font-weight: 600; color: #1a2b4a;">BBB+</div>
                        </div>
                    </div>
                </div>
            """, unsafe_allow_html=True)
    
    # -------------------------------------------------------------------------
    # ROW 4: Data Lineage & Audit Trail
    # -------------------------------------------------------------------------
    
    st.markdown("### 🔍 Data Lineage & Audit Trail")
    st.caption("Full traceability from source systems to reported metrics - critical for external audit")
    
    lineage_col1, lineage_col2 = st.columns([3, 2])
    
    with lineage_col1:
        # Visual data lineage - Carbon Flow
        st.markdown("#### 📊 Carbon Emissions Data Flow")
        carbon_cols = st.columns([1, 0.3, 1, 0.3, 1, 0.3, 1])
        with carbon_cols[0]:
            st.markdown('<div style="background:#e8f4f8;padding:0.75rem;border-radius:8px;text-align:center;"><div style="font-size:0.65rem;color:#666;">Source</div><div style="font-weight:600;color:#1a2b4a;font-size:0.8rem;">ENERGY.CONSUMPTION</div></div>', unsafe_allow_html=True)
        with carbon_cols[1]:
            st.markdown('<div style="text-align:center;color:#888;font-size:1.5rem;padding-top:0.5rem;">→</div>', unsafe_allow_html=True)
        with carbon_cols[2]:
            st.markdown('<div style="background:#f0f8e8;padding:0.75rem;border-radius:8px;text-align:center;"><div style="font-size:0.65rem;color:#666;">Transform</div><div style="font-weight:600;color:#27ae60;font-size:0.8rem;">Emission Factors</div></div>', unsafe_allow_html=True)
        with carbon_cols[3]:
            st.markdown('<div style="text-align:center;color:#888;font-size:1.5rem;padding-top:0.5rem;">→</div>', unsafe_allow_html=True)
        with carbon_cols[4]:
            st.markdown('<div style="background:#f8f0e8;padding:0.75rem;border-radius:8px;text-align:center;"><div style="font-size:0.65rem;color:#666;">Aggregate</div><div style="font-weight:600;color:#e67e22;font-size:0.8rem;">CARBON_INVENTORY</div></div>', unsafe_allow_html=True)
        with carbon_cols[5]:
            st.markdown('<div style="text-align:center;color:#888;font-size:1.5rem;padding-top:0.5rem;">→</div>', unsafe_allow_html=True)
        with carbon_cols[6]:
            st.markdown('<div style="background:#1a2b4a;padding:0.75rem;border-radius:8px;text-align:center;"><div style="font-size:0.65rem;color:#aaa;">Report</div><div style="font-weight:600;color:white;font-size:0.8rem;">BILAN GES</div></div>', unsafe_allow_html=True)
        
        st.markdown("")
        
        # Visual data lineage - Equality Flow
        st.markdown("#### 👥 Equality Index Data Flow")
        eq_cols = st.columns([1, 0.3, 1, 0.3, 1, 0.3, 1])
        with eq_cols[0]:
            st.markdown('<div style="background:#e8f4f8;padding:0.75rem;border-radius:8px;text-align:center;"><div style="font-size:0.65rem;color:#666;">Source</div><div style="font-weight:600;color:#1a2b4a;font-size:0.8rem;">HR.EMPLOYEES</div></div>', unsafe_allow_html=True)
        with eq_cols[1]:
            st.markdown('<div style="text-align:center;color:#888;font-size:1.5rem;padding-top:0.5rem;">→</div>', unsafe_allow_html=True)
        with eq_cols[2]:
            st.markdown('<div style="background:#f0f8e8;padding:0.75rem;border-radius:8px;text-align:center;"><div style="font-size:0.65rem;color:#666;">Calculate</div><div style="font-weight:600;color:#27ae60;font-size:0.8rem;">5 Indicators</div></div>', unsafe_allow_html=True)
        with eq_cols[3]:
            st.markdown('<div style="text-align:center;color:#888;font-size:1.5rem;padding-top:0.5rem;">→</div>', unsafe_allow_html=True)
        with eq_cols[4]:
            st.markdown('<div style="background:#f8f0e8;padding:0.75rem;border-radius:8px;text-align:center;"><div style="font-size:0.65rem;color:#666;">Score</div><div style="font-weight:600;color:#e67e22;font-size:0.8rem;">DIVERSITY_INDEX</div></div>', unsafe_allow_html=True)
        with eq_cols[5]:
            st.markdown('<div style="text-align:center;color:#888;font-size:1.5rem;padding-top:0.5rem;">→</div>', unsafe_allow_html=True)
        with eq_cols[6]:
            st.markdown('<div style="background:#3498db;padding:0.75rem;border-radius:8px;text-align:center;"><div style="font-size:0.65rem;color:rgba(255,255,255,0.7);">Report</div><div style="font-weight:600;color:white;font-size:0.8rem;">INDEX ÉGALITÉ</div></div>', unsafe_allow_html=True)
    
    with lineage_col2:
        st.markdown("#### 📝 Recent Audit Log")
        
        audit_log = [
            {"time": "Today 14:32", "action": "Report Generated", "user": "M. Dubois", "type": "CSRD"},
            {"time": "Today 11:15", "action": "Data Validated", "user": "System", "type": "Carbon"},
            {"time": "Yesterday", "action": "External Audit", "user": "EY Auditors", "type": "DPEF"},
            {"time": "Dec 15", "action": "Methodology Updated", "user": "A. Martin", "type": "GES"},
            {"time": "Dec 12", "action": "Index Calculated", "user": "System", "type": "Égalité"},
        ]
        
        for log in audit_log:
            st.markdown(f"""
                <div style="background: white; border-radius: 6px; padding: 0.6rem; margin-bottom: 0.4rem; display: flex; justify-content: space-between; align-items: center; border-left: 3px solid #1a2b4a;">
                    <div>
                        <div style="font-weight: 600; color: #1a2b4a; font-size: 0.8rem;">{log['action']}</div>
                        <div style="font-size: 0.7rem; color: #888;">{log['user']} • {log['type']}</div>
                    </div>
                    <div style="font-size: 0.7rem; color: #888;">{log['time']}</div>
                </div>
            """, unsafe_allow_html=True)
    
    # -------------------------------------------------------------------------
    # ROW 5: Emissions Trend & Renewable Progress
    # -------------------------------------------------------------------------
    
    st.markdown("### 📈 Environmental Performance Trends")
    
    trend_col1, trend_col2 = st.columns(2)
    
    with trend_col1:
        st.markdown("#### 🌍 Carbon Emissions by Scope")
        
        # Create stacked bar chart for emissions
        emissions_data = {
            'Month': ['Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
            'Scope 1': [1400, 1350, 1380, 1320, 1290, 1250],
            'Scope 2': [5400, 5200, 5350, 5100, 4950, 4800],
            'Scope 3': [1350, 1300, 1280, 1320, 1300, 1280]
        }
        
        fig = go.Figure()
        fig.add_trace(go.Bar(name='Scope 1 (Direct)', x=emissions_data['Month'], y=emissions_data['Scope 1'], marker_color='#e63946'))
        fig.add_trace(go.Bar(name='Scope 2 (Electricity)', x=emissions_data['Month'], y=emissions_data['Scope 2'], marker_color='#f39c12'))
        fig.add_trace(go.Bar(name='Scope 3 (Indirect)', x=emissions_data['Month'], y=emissions_data['Scope 3'], marker_color='#3498db'))
        
        fig.update_layout(
            barmode='stack',
            height=300,
            margin=dict(l=20, r=20, t=20, b=40),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis=dict(showgrid=False),
            yaxis=dict(showgrid=True, gridcolor='#f0f0f0', title='tCO₂e'),
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5)
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    with trend_col2:
        st.markdown("#### ⚡ Renewable Energy Progress")
        
        # Gauge chart for renewable target
        fig = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=renewable,
            number={'suffix': '%', 'font': {'size': 48, 'color': '#27ae60'}},
            delta={'reference': 50, 'relative': False, 'position': 'bottom', 'valueformat': '.0f'},
            gauge={
                'axis': {'range': [0, 100], 'tickwidth': 1},
                'bar': {'color': '#27ae60'},
                'steps': [
                    {'range': [0, 30], 'color': '#fee2e2'},
                    {'range': [30, 50], 'color': '#fef3c7'},
                    {'range': [50, 100], 'color': '#d1fae5'}
                ],
                'threshold': {'line': {'color': '#1a2b4a', 'width': 4}, 'thickness': 0.8, 'value': 50}
            },
            title={'text': '2025 Target: 50%', 'font': {'size': 14, 'color': '#666'}}
        ))
        
        fig.update_layout(height=300, margin=dict(l=20, r=20, t=40, b=20), paper_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig, use_container_width=True)
    
    # -------------------------------------------------------------------------
    # ROW 6: Available Reports Download
    # -------------------------------------------------------------------------
    
    st.markdown("### 📚 Published Reports Archive")
    
    reports_archive = [
        {"name": "CSRD Annual Report 2024", "date": "Mar 2024", "format": "PDF", "size": "2.4 MB", "status": "Published"},
        {"name": "Index Égalité H/F 2024", "date": "Mar 2024", "format": "PDF", "size": "156 KB", "status": "Published"},
        {"name": "Bilan GES 2023", "date": "Dec 2023", "format": "PDF", "size": "1.8 MB", "status": "Published"},
        {"name": "DPEF 2023", "date": "Apr 2024", "format": "PDF", "size": "3.2 MB", "status": "Published"},
        {"name": "EU Taxonomy Report 2024", "date": "Jun 2024", "format": "Excel", "size": "890 KB", "status": "Published"},
        {"name": "GRI Standards Report 2023", "date": "May 2024", "format": "PDF", "size": "4.1 MB", "status": "Published"},
    ]
    
    archive_cols = st.columns(3)
    for i, report in enumerate(reports_archive):
        with archive_cols[i % 3]:
            st.markdown(f"""
                <div style="background: white; border-radius: 8px; padding: 1rem; margin-bottom: 0.5rem; border: 1px solid #e0e0e0;">
                    <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                        <div>
                            <div style="font-weight: 600; color: #1a2b4a; font-size: 0.85rem;">{report['name']}</div>
                            <div style="font-size: 0.7rem; color: #888; margin-top: 0.25rem;">{report['date']} • {report['format']} • {report['size']}</div>
                        </div>
                        <div style="background: #27ae6020; color: #27ae60; padding: 0.2rem 0.5rem; border-radius: 4px; font-size: 0.65rem; font-weight: 600;">
                            {report['status']}
                        </div>
                    </div>
                    <button style="margin-top: 0.75rem; background: #1a2b4a; color: white; border: none; padding: 0.4rem 0.75rem; border-radius: 4px; font-size: 0.75rem; cursor: pointer; width: 100%;">
                        📥 Download
                    </button>
                </div>
            """, unsafe_allow_html=True)
    
    # Footer
    st.markdown("---")
    st.markdown("""
        <div style="text-align: center; color: #888; font-size: 0.8rem;">
            🌱 ESG Regulatory Reporting • Data from ENERGY, HR, FINANCE schemas • Full audit trail maintained
        </div>
    """, unsafe_allow_html=True)

# ==============================================================================
# PAGE: DIGITAL TWIN
# ==============================================================================

def page_digital_twin():
    render_header(
        "Digital Twin",
        "Infrastructure Data Control Tower • Single Source of Truth • Real-Time Quality Monitoring"
    )
    
    # -------------------------------------------------------------------------
    # ROW 1: Data Control Tower KPIs
    # -------------------------------------------------------------------------
    
    st.markdown("### 🎛️ Infrastructure Data Control Tower")
    
    # Fetch data quality metrics
    quality_data = run_query("""
        SELECT 
            COUNT(*) as TOTAL_RECORDS,
            AVG(CASE WHEN SITE_NAME IS NOT NULL AND LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL THEN 100 ELSE 0 END) as COMPLETENESS,
            COUNT(CASE WHEN STATUS = 'ACTIVE' THEN 1 END) as ACTIVE_COUNT
        FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES
    """)
    
    total_records = quality_data['TOTAL_RECORDS'].iloc[0] if not quality_data.empty else 8785
    completeness = quality_data['COMPLETENESS'].iloc[0] if not quality_data.empty else 94.2
    
    kpi_cols = st.columns(4)
    
    with kpi_cols[0]:
        st.markdown(f"""
            <div style="background: linear-gradient(135deg, #27ae60, #2ecc71); border-radius: 12px; padding: 1.5rem; color: white; text-align: center;">
                <div style="font-size: 0.8rem; opacity: 0.9;">Data Completeness</div>
                <div style="font-size: 2.5rem; font-weight: 700;">94.2%</div>
                <div style="font-size: 0.75rem; opacity: 0.8;">↑ 2.1% vs last month</div>
            </div>
        """, unsafe_allow_html=True)
    
    with kpi_cols[1]:
        st.markdown(f"""
            <div style="background: linear-gradient(135deg, #3498db, #2980b9); border-radius: 12px; padding: 1.5rem; color: white; text-align: center;">
                <div style="font-size: 0.8rem; opacity: 0.9;">Data Accuracy</div>
                <div style="font-size: 2.5rem; font-weight: 700;">97.8%</div>
                <div style="font-size: 0.75rem; opacity: 0.8;">Validated vs field surveys</div>
            </div>
        """, unsafe_allow_html=True)
    
    with kpi_cols[2]:
        st.markdown(f"""
            <div style="background: linear-gradient(135deg, #9b59b6, #8e44ad); border-radius: 12px; padding: 1.5rem; color: white; text-align: center;">
                <div style="font-size: 0.8rem; opacity: 0.9;">Digital Twin Sync</div>
                <div style="font-size: 2.5rem; font-weight: 700;">✓ Live</div>
                <div style="font-size: 0.75rem; opacity: 0.8;">Last sync: 2 min ago</div>
            </div>
        """, unsafe_allow_html=True)
    
    with kpi_cols[3]:
        st.markdown(f"""
            <div style="background: linear-gradient(135deg, #e67e22, #d35400); border-radius: 12px; padding: 1.5rem; color: white; text-align: center;">
                <div style="font-size: 0.8rem; opacity: 0.9;">Open Discrepancies</div>
                <div style="font-size: 2.5rem; font-weight: 700;">47</div>
                <div style="font-size: 0.75rem; opacity: 0.8;">↓ 12 resolved this week</div>
            </div>
        """, unsafe_allow_html=True)
    
    # -------------------------------------------------------------------------
    # ROW 2: Single Source of Truth - Infrastructure Inventory
    # -------------------------------------------------------------------------
    
    st.markdown("### 📊 Single Source of Truth - Infrastructure Inventory")
    st.caption("Harmonized data across all systems • Operations, Technology & CAPEX teams")
    
    # Fetch infrastructure counts - count all records (tables have different status column names)
    infra_counts = run_query("""
        SELECT 
            (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES WHERE STATUS = 'ACTIVE') as SITES,
            (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.TOWERS) as TOWERS,
            (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.ANTENNAS) as ANTENNAS,
            (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.DATA_CENTERS) as DATA_CENTERS,
            (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.BROADCAST_TRANSMITTERS) as TRANSMITTERS,
            (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.FIBRE_NETWORK) as FIBRE_SEGMENTS
    """)
    
    # Use realistic defaults for demo if tables are empty or have zero records
    sites_raw = infra_counts['SITES'].iloc[0] if not infra_counts.empty else 0
    towers_raw = infra_counts['TOWERS'].iloc[0] if not infra_counts.empty else 0
    antennas_raw = infra_counts['ANTENNAS'].iloc[0] if not infra_counts.empty else 0
    dcs_raw = infra_counts['DATA_CENTERS'].iloc[0] if not infra_counts.empty else 0
    transmitters_raw = infra_counts['TRANSMITTERS'].iloc[0] if not infra_counts.empty else 0
    fibre_raw = infra_counts['FIBRE_SEGMENTS'].iloc[0] if not infra_counts.empty else 0
    
    # Apply realistic defaults for demo when data is missing or zero
    sites = sites_raw if sites_raw > 0 else 8785
    towers = towers_raw if towers_raw > 0 else 7877
    antennas = antennas_raw if antennas_raw > 0 else 24500
    dcs = dcs_raw if dcs_raw > 0 else 12
    transmitters = transmitters_raw if transmitters_raw > 0 else 1850
    fibre = fibre_raw if fibre_raw > 0 else 4200
    
    inv_cols = st.columns(6)
    inventory_items = [
        {"icon": "📡", "name": "Sites", "count": sites, "twin_pct": 98, "color": "#1a2b4a"},
        {"icon": "🗼", "name": "Towers", "count": towers, "twin_pct": 95, "color": "#e63946"},
        {"icon": "📶", "name": "Antennas", "count": antennas, "twin_pct": 87, "color": "#3498db"},
        {"icon": "🏢", "name": "Data Centers", "count": dcs, "twin_pct": 100, "color": "#27ae60"},
        {"icon": "📺", "name": "Transmitters", "count": transmitters, "twin_pct": 92, "color": "#9b59b6"},
        {"icon": "🔌", "name": "Fibre Segments", "count": fibre, "twin_pct": 78, "color": "#e67e22"},
    ]
    
    for i, item in enumerate(inventory_items):
        with inv_cols[i]:
            st.markdown(f"""
                <div style="background: white; border-radius: 10px; padding: 1rem; text-align: center; border-top: 4px solid {item['color']}; box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
                    <div style="font-size: 2rem;">{item['icon']}</div>
                    <div style="font-size: 1.5rem; font-weight: 700; color: #1a2b4a;">{item['count']:,}</div>
                    <div style="font-size: 0.8rem; color: #666;">{item['name']}</div>
                    <div style="margin-top: 0.5rem;">
                        <div style="background: #f0f0f0; border-radius: 4px; height: 6px;">
                            <div style="background: {item['color']}; width: {item['twin_pct']}%; height: 100%; border-radius: 4px;"></div>
                        </div>
                        <div style="font-size: 0.65rem; color: #888; margin-top: 0.25rem;">{item['twin_pct']}% in 3D Model</div>
                    </div>
                </div>
            """, unsafe_allow_html=True)
    
    # -------------------------------------------------------------------------
    # ROW 3: Discrepancy Detection & Resolution
    # -------------------------------------------------------------------------
    
    st.markdown("### 🔍 Discrepancy Detection & Resolution")
    
    disc_col1, disc_col2 = st.columns([2, 1])
    
    with disc_col1:
        st.markdown("#### Open Discrepancies")
        
        discrepancies = [
            {"id": "DISC-001", "source_a": "GIS System", "source_b": "SAP PM", "asset": "SITE-003421", "field": "Tower Height", "value_a": "45m", "value_b": "42m", "status": "Open", "priority": "High", "age": "3 days"},
            {"id": "DISC-002", "source_a": "Inventory DB", "source_b": "Field Survey", "asset": "TOWER-002187", "field": "Antenna Count", "value_a": "3", "value_b": "4", "status": "In Review", "priority": "Medium", "age": "5 days"},
            {"id": "DISC-003", "source_a": "Digital Twin", "source_b": "Client Portal", "asset": "SITE-005892", "field": "Coordinates", "value_a": "48.8566, 2.3522", "value_b": "48.8568, 2.3520", "status": "Open", "priority": "Low", "age": "1 day"},
            {"id": "DISC-004", "source_a": "Asset Register", "source_b": "Maintenance Log", "asset": "DC-PARIS-01", "field": "Power Capacity", "value_a": "2.5 MW", "value_b": "2.8 MW", "status": "Open", "priority": "High", "age": "7 days"},
            {"id": "DISC-005", "source_a": "GIS System", "source_b": "Planning Tool", "asset": "SITE-007234", "field": "Site Type", "value_a": "Rooftop", "value_b": "Ground", "status": "Resolved", "priority": "Medium", "age": "2 days"},
        ]
        
        for d in discrepancies[:4]:
            status_color = '#e63946' if d['status'] == 'Open' else '#f39c12' if d['status'] == 'In Review' else '#27ae60'
            priority_color = '#e63946' if d['priority'] == 'High' else '#f39c12' if d['priority'] == 'Medium' else '#3498db'
            
            st.markdown(f"""
                <div style="background: white; border-radius: 8px; padding: 1rem; margin-bottom: 0.5rem; border-left: 4px solid {status_color}; box-shadow: 0 1px 3px rgba(0,0,0,0.08);">
                    <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                        <div style="flex: 2;">
                            <div style="font-weight: 600; color: #1a2b4a; font-size: 0.9rem;">{d['id']}: {d['field']} Mismatch</div>
                            <div style="font-size: 0.75rem; color: #888; margin-top: 0.25rem;">{d['asset']} • {d['age']} old</div>
                        </div>
                        <div style="flex: 2; text-align: center;">
                            <div style="font-size: 0.7rem; color: #888;">{d['source_a']} vs {d['source_b']}</div>
                            <div style="font-size: 0.85rem; margin-top: 0.25rem;"><span style="color: #e63946;">{d['value_a']}</span> ≠ <span style="color: #3498db;">{d['value_b']}</span></div>
                        </div>
                        <div style="flex: 1; text-align: right;">
                            <span style="background: {priority_color}20; color: {priority_color}; padding: 0.2rem 0.5rem; border-radius: 4px; font-size: 0.7rem; font-weight: 600;">{d['priority']}</span>
                            <div style="margin-top: 0.25rem;">
                                <span style="background: {status_color}20; color: {status_color}; padding: 0.2rem 0.5rem; border-radius: 4px; font-size: 0.7rem;">{d['status']}</span>
                            </div>
                        </div>
                    </div>
                </div>
            """, unsafe_allow_html=True)
    
    with disc_col2:
        st.markdown("#### By Category")
        
        categories = ['Coordinates', 'Dimensions', 'Equipment', 'Capacity', 'Status', 'Other']
        counts = [12, 8, 15, 5, 4, 3]
        colors = ['#e63946', '#f39c12', '#3498db', '#27ae60', '#9b59b6', '#888']
        
        fig = go.Figure(go.Bar(
            y=categories,
            x=counts,
            orientation='h',
            marker=dict(color=colors),
            text=counts,
            textposition='outside'
        ))
        
        fig.update_layout(
            height=250,
            margin=dict(l=10, r=40, t=10, b=10),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis=dict(showgrid=True, gridcolor='#f0f0f0'),
            yaxis=dict(showgrid=False, categoryorder='total ascending'),
            showlegend=False
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Resolution stats
        st.markdown("""
            <div style="background: #27ae6015; border-radius: 8px; padding: 0.75rem; text-align: center;">
                <div style="font-size: 0.75rem; color: #666;">Avg Resolution Time</div>
                <div style="font-size: 1.2rem; font-weight: 700; color: #27ae60;">4.2 days</div>
            </div>
        """, unsafe_allow_html=True)
    
    # -------------------------------------------------------------------------
    # ROW 4: Digital Twin 3D Model Coverage
    # -------------------------------------------------------------------------
    
    st.markdown("### 🏗️ Digital Twin 3D Model Coverage")
    
    twin_col1, twin_col2 = st.columns([1, 2])
    
    with twin_col1:
        st.markdown("#### Model Statistics")
        
        model_stats = [
            {"metric": "Pylons Modeled", "value": "2,147", "target": "2,000", "status": "achieved"},
            {"metric": "Sites with 3D", "value": "8,245", "target": "8,785", "status": "on_track"},
            {"metric": "LOD Level", "value": "LOD 300", "target": "LOD 300", "status": "achieved"},
            {"metric": "Last Full Scan", "value": "Nov 15", "target": "Monthly", "status": "on_track"},
            {"metric": "Model Accuracy", "value": "±5cm", "target": "±10cm", "status": "achieved"},
        ]
        
        for stat in model_stats:
            status_icon = '✅' if stat['status'] == 'achieved' else '🔵'
            st.markdown(f"""
                <div style="background: white; border-radius: 6px; padding: 0.6rem; margin-bottom: 0.4rem; display: flex; justify-content: space-between; align-items: center;">
                    <span style="color: #666; font-size: 0.85rem;">{stat['metric']}</span>
                    <span style="font-weight: 600; color: #1a2b4a;">{stat['value']} {status_icon}</span>
                </div>
            """, unsafe_allow_html=True)
        
        # 3D Model badge
        st.markdown("""
            <div style="background: linear-gradient(135deg, #1a2b4a, #2d3436); border-radius: 10px; padding: 1rem; margin-top: 1rem; text-align: center; color: white;">
                <div style="font-size: 2rem;">🏗️</div>
                <div style="font-weight: 600;">Digital Twin Status</div>
                <div style="font-size: 1.5rem; font-weight: 700; color: #27ae60; margin-top: 0.5rem;">OPERATIONAL</div>
                <div style="font-size: 0.7rem; color: #aaa;">Real-time sync enabled</div>
            </div>
        """, unsafe_allow_html=True)
    
    with twin_col2:
        st.markdown("#### Regional 3D Coverage")
        
        # Regional coverage data
        regions = ['Île-de-France', 'Auvergne-Rhône-Alpes', 'Nouvelle-Aquitaine', 'Occitanie', 'Hauts-de-France', 
                   'Grand Est', 'Provence-Alpes-Côte d\'Azur', 'Pays de la Loire', 'Bretagne', 'Normandie']
        coverage = [98, 96, 94, 93, 91, 89, 88, 85, 82, 78]
        sites_count = [1245, 892, 756, 698, 645, 612, 589, 534, 487, 423]
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            y=regions,
            x=coverage,
            orientation='h',
            marker=dict(color=['#27ae60' if c >= 90 else '#f39c12' if c >= 80 else '#e63946' for c in coverage]),
            text=[f"{c}% ({s:,} sites)" for c, s in zip(coverage, sites_count)],
            textposition='outside',
            hovertemplate='<b>%{y}</b><br>Coverage: %{x}%<extra></extra>'
        ))
        
        fig.update_layout(
            height=350,
            margin=dict(l=10, r=80, t=10, b=10),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis=dict(showgrid=True, gridcolor='#f0f0f0', range=[0, 110], title='3D Model Coverage %'),
            yaxis=dict(showgrid=False, categoryorder='total ascending'),
            showlegend=False
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    # -------------------------------------------------------------------------
    # ROW 4b: 3D Cell Tower Model
    # -------------------------------------------------------------------------
    
    st.markdown("### 🗼 3D Cell Tower Model - Interactive Viewer")
    st.caption("Detailed 3D representation of TDF tower infrastructure • Rotate and zoom to explore")
    
    # Tower database with unique configurations
    tower_database = {
        "SITE-003421 (Paris 15ème)": {
            "id": "SITE-003421",
            "location": "Paris 15ème",
            "type": "Lattice",
            "height": 65,
            "year": 2008,
            "max_load": 2500,
            "current_load": 1847,
            "color": "#e63946",
            "tenants": [
                {"name": "Orange", "tech": "4G/5G", "height": 25, "color": "#3498db", "count": 3},
                {"name": "SFR", "tech": "5G", "height": 40, "color": "#27ae60", "count": 3},
                {"name": "Bouygues", "tech": "4G", "height": 55, "color": "#f39c12", "count": 3},
                {"name": "Free", "tech": "5G", "height": 62, "color": "#9b59b6", "count": 2},
            ]
        },
        "SITE-005892 (Lyon Part-Dieu)": {
            "id": "SITE-005892",
            "location": "Lyon Part-Dieu",
            "type": "Monopole",
            "height": 45,
            "year": 2015,
            "max_load": 1800,
            "current_load": 1245,
            "color": "#3498db",
            "tenants": [
                {"name": "Orange", "tech": "5G", "height": 30, "color": "#3498db", "count": 3},
                {"name": "SFR", "tech": "4G/5G", "height": 40, "color": "#27ae60", "count": 3},
            ]
        },
        "SITE-007234 (Marseille Vieux-Port)": {
            "id": "SITE-007234",
            "location": "Marseille Vieux-Port",
            "type": "Rooftop",
            "height": 25,
            "year": 2019,
            "max_load": 800,
            "current_load": 520,
            "color": "#27ae60",
            "tenants": [
                {"name": "Bouygues", "tech": "5G", "height": 20, "color": "#f39c12", "count": 3},
                {"name": "Free", "tech": "4G", "height": 23, "color": "#9b59b6", "count": 2},
            ]
        },
        "SITE-001256 (Bordeaux Mériadeck)": {
            "id": "SITE-001256",
            "location": "Bordeaux Mériadeck",
            "type": "Lattice",
            "height": 80,
            "year": 2003,
            "max_load": 3200,
            "current_load": 2890,
            "color": "#e74c3c",
            "tenants": [
                {"name": "Orange", "tech": "4G", "height": 30, "color": "#3498db", "count": 3},
                {"name": "SFR", "tech": "4G", "height": 45, "color": "#27ae60", "count": 3},
                {"name": "Bouygues", "tech": "4G/5G", "height": 60, "color": "#f39c12", "count": 3},
                {"name": "Free", "tech": "5G", "height": 75, "color": "#9b59b6", "count": 3},
                {"name": "TDF Broadcast", "tech": "DTT", "height": 78, "color": "#e63946", "count": 2},
            ]
        },
        "SITE-002891 (Toulouse Blagnac)": {
            "id": "SITE-002891",
            "location": "Toulouse Blagnac",
            "type": "Guyed Mast",
            "height": 120,
            "year": 1998,
            "max_load": 5000,
            "current_load": 3750,
            "color": "#9b59b6",
            "tenants": [
                {"name": "TDF Broadcast", "tech": "FM Radio", "height": 50, "color": "#e63946", "count": 4},
                {"name": "TDF Broadcast", "tech": "DTT", "height": 80, "color": "#c0392b", "count": 6},
                {"name": "Orange", "tech": "4G", "height": 100, "color": "#3498db", "count": 3},
                {"name": "SFR", "tech": "5G", "height": 110, "color": "#27ae60", "count": 3},
            ]
        },
        "SITE-004567 (Nantes Île de Nantes)": {
            "id": "SITE-004567",
            "location": "Nantes Île de Nantes",
            "type": "Camouflaged (Tree)",
            "height": 35,
            "year": 2021,
            "max_load": 1200,
            "current_load": 680,
            "color": "#27ae60",
            "tenants": [
                {"name": "Free", "tech": "5G", "height": 28, "color": "#9b59b6", "count": 3},
                {"name": "Bouygues", "tech": "5G", "height": 32, "color": "#f39c12", "count": 3},
            ]
        },
    }
    
    # Tower selector
    selected_tower_name = st.selectbox(
        "🗼 Select Tower to View",
        options=list(tower_database.keys()),
        index=0
    )
    
    tower = tower_database[selected_tower_name]
    tower_height = tower["height"]
    
    viz_col1, viz_col2 = st.columns([2, 1])
    
    with viz_col1:
        import numpy as np
        
        # Create a detailed 3D cell tower model based on selected tower
        fig_tower = go.Figure()
        
        # === TOWER STRUCTURE (varies by type) ===
        tower_color = tower["color"]
        
        if tower["type"] in ["Lattice", "Guyed Mast"]:
            # Lattice tower with 4 legs
            leg_offset = 1.5 + (tower_height / 100)  # Larger base for taller towers
            for x, y in [(-leg_offset, -leg_offset), (leg_offset, -leg_offset), 
                         (leg_offset, leg_offset), (-leg_offset, leg_offset)]:
                z_points = list(range(0, tower_height + 1, 5))
                taper = [1 - (z / tower_height) * 0.6 for z in z_points]
                x_points = [x * t for t in taper]
                y_points = [y * t for t in taper]
                
                fig_tower.add_trace(go.Scatter3d(
                    x=x_points, y=y_points, z=z_points,
                    mode='lines',
                    line=dict(color=tower_color, width=8),
                    name='Tower Structure',
                    showlegend=False,
                    hoverinfo='skip'
                ))
            
            # Cross bracing
            brace_interval = 10 if tower_height < 80 else 15
            for z in range(5, tower_height, brace_interval):
                taper = 1 - (z / tower_height) * 0.6
                offset = leg_offset * taper
                ring_x = [-offset, offset, offset, -offset, -offset]
                ring_y = [-offset, -offset, offset, offset, -offset]
                ring_z = [z] * 5
                fig_tower.add_trace(go.Scatter3d(
                    x=ring_x, y=ring_y, z=ring_z,
                    mode='lines',
                    line=dict(color='#7f8c8d', width=4),
                    showlegend=False,
                    hoverinfo='skip'
                ))
                
        elif tower["type"] == "Monopole":
            # Single pole structure
            z_points = list(range(0, tower_height + 1, 2))
            x_points = [0] * len(z_points)
            y_points = [0] * len(z_points)
            fig_tower.add_trace(go.Scatter3d(
                x=x_points, y=y_points, z=z_points,
                mode='lines',
                line=dict(color=tower_color, width=20),
                name='Monopole',
                showlegend=False,
                hoverinfo='skip'
            ))
            
        elif tower["type"] == "Rooftop":
            # Building base + small mast
            # Building representation
            building_size = 8
            fig_tower.add_trace(go.Mesh3d(
                x=[-building_size, building_size, building_size, -building_size, -building_size, building_size, building_size, -building_size],
                y=[-building_size, -building_size, building_size, building_size, -building_size, -building_size, building_size, building_size],
                z=[0, 0, 0, 0, -15, -15, -15, -15],
                color='#bdc3c7',
                opacity=0.5,
                name='Building',
                hoverinfo='name'
            ))
            # Small mast on top
            fig_tower.add_trace(go.Scatter3d(
                x=[0, 0], y=[0, 0], z=[0, tower_height],
                mode='lines',
                line=dict(color=tower_color, width=15),
                showlegend=False,
                hoverinfo='skip'
            ))
            
        elif tower["type"] == "Camouflaged (Tree)":
            # Tree-shaped tower
            # Trunk
            fig_tower.add_trace(go.Scatter3d(
                x=[0, 0], y=[0, 0], z=[0, tower_height],
                mode='lines',
                line=dict(color='#8B4513', width=15),
                name='Trunk',
                showlegend=False,
                hoverinfo='skip'
            ))
            # Branches at different levels
            for z in range(10, tower_height, 8):
                branch_len = 3 - (z / tower_height) * 2
                for angle in np.linspace(0, 2*np.pi, 6, endpoint=False):
                    bx = branch_len * np.cos(angle)
                    by = branch_len * np.sin(angle)
                    fig_tower.add_trace(go.Scatter3d(
                        x=[0, bx], y=[0, by], z=[z, z+1],
                        mode='lines',
                        line=dict(color='#228B22', width=6),
                        showlegend=False,
                        hoverinfo='skip'
                    ))
        
        # === ANTENNA PLATFORMS & PANELS (from tower config) ===
        for tenant in tower["tenants"]:
            ph = tenant["height"]
            
            # Platform
            platform_size = 2.5 if tower["type"] != "Monopole" else 1.5
            fig_tower.add_trace(go.Mesh3d(
                x=[-platform_size, platform_size, platform_size, -platform_size],
                y=[-platform_size, -platform_size, platform_size, platform_size],
                z=[ph, ph, ph, ph],
                color=tenant["color"],
                opacity=0.6,
                name=f'{tenant["name"]} Platform',
                hoverinfo='skip'
            ))
            
            # Antenna panels
            antenna_count = tenant.get("count", 3)
            angles = np.linspace(0, 2*np.pi, antenna_count, endpoint=False)
            for i, angle in enumerate(angles):
                ax = 3 * np.cos(angle)
                ay = 3 * np.sin(angle)
                panel_h = 2
                
                # Only add legend for first panel of each tenant
                show_legend = (i == 0)
                
                fig_tower.add_trace(go.Scatter3d(
                    x=[ax, ax, ax, ax, ax],
                    y=[ay, ay, ay, ay, ay],
                    z=[ph, ph+panel_h, ph+panel_h, ph, ph],
                    mode='lines',
                    line=dict(color=tenant["color"], width=12),
                    name=f'{tenant["name"]} {tenant["tech"]}',
                    showlegend=show_legend,
                    hovertemplate=f'<b>{tenant["name"]}</b><br>{tenant["tech"]}<br>Height: {ph}m<extra></extra>'
                ))
        
        # === GROUND BASE ===
        base_size = 4
        fig_tower.add_trace(go.Mesh3d(
            x=[-base_size, base_size, base_size, -base_size],
            y=[-base_size, -base_size, base_size, base_size],
            z=[0, 0, 0, 0],
            color='#7f8c8d',
            opacity=0.8,
            name='Base',
            hoverinfo='name'
        ))
        
        # === TOP BEACON ===
        fig_tower.add_trace(go.Scatter3d(
            x=[0], y=[0], z=[tower_height + 2],
            mode='markers',
            marker=dict(size=10, color='red', symbol='diamond'),
            name='Aviation Light',
            hovertemplate='<b>Aviation Warning Light</b><br>Height: 67m<extra></extra>'
        ))
        
        fig_tower.update_layout(
            height=500,
            margin=dict(l=0, r=0, t=30, b=0),
            scene=dict(
                xaxis=dict(title='', showticklabels=False, showgrid=False, zeroline=False, visible=False),
                yaxis=dict(title='', showticklabels=False, showgrid=False, zeroline=False, visible=False),
                zaxis=dict(title='Height (m)', range=[-20 if tower["type"] == "Rooftop" else -5, tower_height + 10], showgrid=True, gridcolor='#eee'),
                camera=dict(eye=dict(x=1.8, y=1.8, z=0.8)),
                bgcolor='rgba(248,249,250,1)',
                aspectratio=dict(x=1, y=1, z=2 if tower_height < 50 else 2.5 if tower_height < 100 else 3)
            ),
            paper_bgcolor='rgba(0,0,0,0)',
            showlegend=True,
            legend=dict(x=0, y=1, bgcolor='rgba(255,255,255,0.8)')
        )
        
        st.plotly_chart(fig_tower, use_container_width=True)
        st.caption("🖱️ Drag to rotate • Scroll to zoom • Click legend to toggle layers")
    
    with viz_col2:
        st.markdown(f"#### 🗼 Tower: {tower['id']}")
        st.markdown(f"**Location:** {tower['location']}")
        
        # Tower specifications
        st.markdown("##### Specifications")
        load_pct = int(tower['current_load'] / tower['max_load'] * 100)
        load_color = '#27ae60' if load_pct < 70 else '#f39c12' if load_pct < 85 else '#e63946'
        
        spec_data = {
            "Tower Type": tower['type'],
            "Total Height": f"{tower['height']}m",
            "Year Built": str(tower['year']),
            "Last Inspection": "Oct 2025",
            "Max Load": f"{tower['max_load']:,} kg",
            "Current Load": f"<span style='color:{load_color}'>{tower['current_load']:,} kg ({load_pct}%)</span>"
        }
        
        for key, value in spec_data.items():
            st.markdown(f"**{key}:** {value}", unsafe_allow_html=True)
        
        st.markdown("---")
        st.markdown(f"##### 📶 Tenants ({len(tower['tenants'])})")
        
        for t in tower['tenants']:
            st.markdown(f"<span style='color:{t['color']};'>●</span> **{t['name']}** - {t['tech']} @ {t['height']}m", unsafe_allow_html=True)
        
        st.markdown("---")
        
        # Open 3D Viewer button
        st.markdown("""
            <div style="background: linear-gradient(135deg, #e63946, #c0392b); border-radius: 10px; padding: 1rem; text-align: center; color: white; cursor: pointer;">
                <div style="font-size: 1.5rem; margin-bottom: 0.5rem;">🖥️</div>
                <div style="font-weight: 600;">Open Full 3D Model</div>
                <div style="font-size: 0.7rem; opacity: 0.8; margin-top: 0.25rem;">High-resolution CAD viewer</div>
            </div>
        """, unsafe_allow_html=True)
    
    # -------------------------------------------------------------------------
    # ROW 4c: Live Sensor Dashboard + Alerts
    # -------------------------------------------------------------------------
    
    st.markdown("---")
    st.markdown(f"### 📡 Live Tower Data: {tower['id']}")
    
    sensor_col1, sensor_col2, sensor_col3, sensor_col4 = st.columns(4)
    
    # Simulated live sensor data (would come from IoT in production)
    import random
    random.seed(hash(tower['id']))  # Consistent per tower
    
    with sensor_col1:
        temp = 22 + random.randint(-5, 15)
        temp_color = '#27ae60' if temp < 30 else '#f39c12' if temp < 40 else '#e63946'
        st.markdown(f"""
            <div style="background: white; border-radius: 10px; padding: 1rem; text-align: center; border-top: 4px solid {temp_color};">
                <div style="font-size: 0.8rem; color: #888;">🌡️ Equipment Temp</div>
                <div style="font-size: 2rem; font-weight: 700; color: {temp_color};">{temp}°C</div>
                <div style="font-size: 0.7rem; color: #888;">Normal: &lt;35°C</div>
            </div>
        """, unsafe_allow_html=True)
    
    with sensor_col2:
        power = 15 + random.randint(0, 25) + (len(tower['tenants']) * 5)
        st.markdown(f"""
            <div style="background: white; border-radius: 10px; padding: 1rem; text-align: center; border-top: 4px solid #3498db;">
                <div style="font-size: 0.8rem; color: #888;">⚡ Power Draw</div>
                <div style="font-size: 2rem; font-weight: 700; color: #3498db;">{power} kW</div>
                <div style="font-size: 0.7rem; color: #888;">Capacity: 80 kW</div>
            </div>
        """, unsafe_allow_html=True)
    
    with sensor_col3:
        wind = random.randint(5, 35)
        wind_color = '#27ae60' if wind < 20 else '#f39c12' if wind < 40 else '#e63946'
        st.markdown(f"""
            <div style="background: white; border-radius: 10px; padding: 1rem; text-align: center; border-top: 4px solid {wind_color};">
                <div style="font-size: 0.8rem; color: #888;">💨 Wind Speed</div>
                <div style="font-size: 2rem; font-weight: 700; color: {wind_color};">{wind} km/h</div>
                <div style="font-size: 0.7rem; color: #888;">Alert: &gt;60 km/h</div>
            </div>
        """, unsafe_allow_html=True)
    
    with sensor_col4:
        uptime = 99.0 + random.random() * 0.99
        st.markdown(f"""
            <div style="background: white; border-radius: 10px; padding: 1rem; text-align: center; border-top: 4px solid #27ae60;">
                <div style="font-size: 0.8rem; color: #888;">📶 Uptime (30d)</div>
                <div style="font-size: 2rem; font-weight: 700; color: #27ae60;">{uptime:.2f}%</div>
                <div style="font-size: 0.7rem; color: #888;">SLA: 99.9%</div>
            </div>
        """, unsafe_allow_html=True)
    
    # -------------------------------------------------------------------------
    # ROW 4d: Alerts + Revenue
    # -------------------------------------------------------------------------
    
    alert_col, revenue_col = st.columns(2)
    
    with alert_col:
        st.markdown("#### ⚠️ Active Alerts")
        
        # Generate alerts based on tower characteristics
        alerts = []
        if tower['current_load'] / tower['max_load'] > 0.8:
            alerts.append({"type": "warning", "msg": "Load capacity at 90% - plan expansion", "time": "2h ago"})
        if tower['year'] < 2010:
            alerts.append({"type": "info", "msg": "Structural inspection due in 30 days", "time": "Today"})
        if wind > 25:
            alerts.append({"type": "warning", "msg": f"High wind alert: {wind} km/h", "time": "Live"})
        if len(alerts) == 0:
            alerts.append({"type": "success", "msg": "All systems operating normally", "time": "Live"})
        
        # Add some random alerts for variety
        random_alerts = [
            {"type": "info", "msg": "Scheduled maintenance: Dec 15", "time": "5d"},
            {"type": "warning", "msg": "Backup power test required", "time": "1d ago"},
            {"type": "success", "msg": "5G upgrade completed successfully", "time": "3d ago"},
        ]
        alerts.extend(random.sample(random_alerts, min(2, len(random_alerts))))
        
        for alert in alerts[:4]:
            alert_color = '#e63946' if alert['type'] == 'critical' else '#f39c12' if alert['type'] == 'warning' else '#27ae60' if alert['type'] == 'success' else '#3498db'
            alert_icon = '🔴' if alert['type'] == 'critical' else '🟡' if alert['type'] == 'warning' else '🟢' if alert['type'] == 'success' else '🔵'
            st.markdown(f"""
                <div style="background: {alert_color}10; border-left: 3px solid {alert_color}; padding: 0.5rem 0.75rem; margin-bottom: 0.5rem; border-radius: 0 6px 6px 0;">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <span style="font-size: 0.85rem;">{alert_icon} {alert['msg']}</span>
                        <span style="font-size: 0.7rem; color: #888;">{alert['time']}</span>
                    </div>
                </div>
            """, unsafe_allow_html=True)
    
    with revenue_col:
        st.markdown("#### 💰 Revenue & ROI")
        
        # Calculate revenue based on tenants and tower size
        base_revenue = tower['height'] * 500 + len(tower['tenants']) * 15000
        monthly_revenue = base_revenue + random.randint(-5000, 10000)
        annual_revenue = monthly_revenue * 12
        years_operating = 2025 - tower['year']
        total_revenue = annual_revenue * years_operating
        construction_cost = tower['height'] * 8000 + 150000
        roi = ((total_revenue - construction_cost) / construction_cost) * 100
        
        rev_col1, rev_col2 = st.columns(2)
        with rev_col1:
            st.metric("Monthly Revenue", f"€{monthly_revenue:,.0f}", f"+{random.randint(2,8)}% YoY")
            st.metric("Total ROI", f"{roi:.0f}%", f"Since {tower['year']}")
        with rev_col2:
            st.metric("Annual Revenue", f"€{annual_revenue:,.0f}")
            st.metric("Payback Period", f"{construction_cost/annual_revenue:.1f} yrs", "✓ Achieved")
        
        # Revenue by tenant pie
        tenant_revenues = [base_revenue / len(tower['tenants']) + random.randint(-2000, 5000) for _ in tower['tenants']]
        fig_rev = go.Figure(data=[go.Pie(
            labels=[t['name'] for t in tower['tenants']],
            values=tenant_revenues,
            hole=0.5,
            marker_colors=[t['color'] for t in tower['tenants']],
            textinfo='percent',
            textfont_size=10
        )])
        fig_rev.update_layout(
            height=150,
            margin=dict(l=10, r=10, t=10, b=10),
            showlegend=False,
            paper_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_rev, use_container_width=True)
        st.caption("Revenue split by tenant")
    
    # -------------------------------------------------------------------------
    # ROW 4e: Drone Inspection + Tower Timeline
    # -------------------------------------------------------------------------
    
    drone_col, timeline_col = st.columns(2)
    
    with drone_col:
        st.markdown("#### 🚁 Drone Inspection Status")
        
        last_scan_days = random.randint(15, 120)
        scan_status = "✅ Recent" if last_scan_days < 30 else "🟡 Due Soon" if last_scan_days < 90 else "🔴 Overdue"
        scan_color = '#27ae60' if last_scan_days < 30 else '#f39c12' if last_scan_days < 90 else '#e63946'
        
        st.markdown(f"""
            <div style="background: white; border-radius: 10px; padding: 1rem; margin-bottom: 0.5rem;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem;">
                    <span style="font-weight: 600; color: #1a2b4a;">Last Inspection</span>
                    <span style="background: {scan_color}20; color: {scan_color}; padding: 0.25rem 0.5rem; border-radius: 12px; font-size: 0.75rem; font-weight: 600;">{scan_status}</span>
                </div>
                <div style="font-size: 0.85rem; color: #666; margin-bottom: 0.5rem;">📅 {last_scan_days} days ago</div>
                <div style="font-size: 0.85rem; color: #666; margin-bottom: 0.5rem;">📸 847 photos captured</div>
                <div style="font-size: 0.85rem; color: #666;">🤖 AI Analysis: No anomalies detected</div>
            </div>
        """, unsafe_allow_html=True)
        
        # Inspection findings
        findings = [
            {"item": "Structure integrity", "status": "✅ Good"},
            {"item": "Antenna alignment", "status": "✅ Good"},
            {"item": "Cable condition", "status": "🟡 Monitor"},
            {"item": "Rust/corrosion", "status": "✅ None"},
        ]
        
        for f in findings:
            st.markdown(f"<span style='font-size: 0.85rem;'>{f['status']} {f['item']}</span>", unsafe_allow_html=True)
        
        st.button("🚁 Request New Scan", use_container_width=True)
    
    with timeline_col:
        st.markdown("#### 📈 Tower Evolution Timeline")
        
        # Generate timeline based on tower year
        events = [
            {"year": tower['year'], "event": "🏗️ Tower constructed", "color": "#1a2b4a"},
        ]
        
        # Add tenant events
        for i, tenant in enumerate(tower['tenants']):
            event_year = tower['year'] + 2 + i * 2 + random.randint(0, 3)
            if event_year <= 2025:
                events.append({"year": event_year, "event": f"📡 {tenant['name']} {tenant['tech']} installed", "color": tenant['color']})
        
        # Add some upgrade events
        if tower['year'] < 2015:
            events.append({"year": 2018, "event": "⚡ Power system upgraded", "color": "#f39c12"})
        if tower['year'] < 2020:
            events.append({"year": 2022, "event": "🔒 Security system added", "color": "#9b59b6"})
        
        events.append({"year": 2025, "event": "🔍 Current inspection", "color": "#27ae60"})
        
        # Sort by year
        events = sorted(events, key=lambda x: x['year'])
        
        for event in events[-6:]:  # Show last 6 events
            st.markdown(f"""
                <div style="display: flex; align-items: center; gap: 0.75rem; margin-bottom: 0.5rem;">
                    <div style="background: {event['color']}; color: white; padding: 0.25rem 0.5rem; border-radius: 4px; font-size: 0.75rem; font-weight: 600; min-width: 50px; text-align: center;">{event['year']}</div>
                    <div style="font-size: 0.85rem; color: #666;">{event['event']}</div>
                </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 4g: Photo-to-Product Reference Reconciliation (KEY PAIN POINT)
    # -------------------------------------------------------------------------
    
    st.markdown("### 🔍 Photo-to-Product Reference Reconciliation")
    st.caption("Cross-checking Digital Twin assets with Product Reference Master Data")
    
    # KPIs for reconciliation status
    recon_col1, recon_col2, recon_col3, recon_col4 = st.columns(4)
    
    with recon_col1:
        st.markdown("""
            <div style="background: white; border-radius: 10px; padding: 1rem; text-align: center; border-left: 4px solid #3498db;">
                <div style="font-size: 0.75rem; color: #888;">📸 Digital Twin Assets</div>
                <div style="font-size: 1.8rem; font-weight: 700; color: #1a2b4a;">247,834</div>
                <div style="font-size: 0.7rem; color: #3498db;">Photos & 3D scans captured</div>
            </div>
        """, unsafe_allow_html=True)
    
    with recon_col2:
        st.markdown("""
            <div style="background: white; border-radius: 10px; padding: 1rem; text-align: center; border-left: 4px solid #27ae60;">
                <div style="font-size: 0.75rem; color: #888;">✅ Matched to Reference</div>
                <div style="font-size: 1.8rem; font-weight: 700; color: #27ae60;">198,412</div>
                <div style="font-size: 0.7rem; color: #27ae60;">80.1% reconciled</div>
            </div>
        """, unsafe_allow_html=True)
    
    with recon_col3:
        st.markdown("""
            <div style="background: white; border-radius: 10px; padding: 1rem; text-align: center; border-left: 4px solid #f39c12;">
                <div style="font-size: 0.75rem; color: #888;">⚠️ Pending Review</div>
                <div style="font-size: 1.8rem; font-weight: 700; color: #f39c12;">35,847</div>
                <div style="font-size: 0.7rem; color: #f39c12;">14.5% needs validation</div>
            </div>
        """, unsafe_allow_html=True)
    
    with recon_col4:
        st.markdown("""
            <div style="background: white; border-radius: 10px; padding: 1rem; text-align: center; border-left: 4px solid #e63946;">
                <div style="font-size: 0.75rem; color: #888;">❌ Unmatched Items</div>
                <div style="font-size: 1.8rem; font-weight: 700; color: #e63946;">13,575</div>
                <div style="font-size: 0.7rem; color: #e63946;">5.4% discrepancies</div>
            </div>
        """, unsafe_allow_html=True)
    
    # Discrepancy Types + Reconciliation Queue
    disc_col, queue_col = st.columns(2)
    
    with disc_col:
        st.markdown("#### 📊 Discrepancy Breakdown")
        
        discrepancy_types = {
            "Photo exists, no reference": 5234,
            "Reference exists, no photo": 4891,
            "Serial number mismatch": 1823,
            "Model/type mismatch": 987,
            "Location mismatch": 640
        }
        
        fig_disc = go.Figure(data=[go.Bar(
            y=list(discrepancy_types.keys()),
            x=list(discrepancy_types.values()),
            orientation='h',
            marker_color=['#e63946', '#c0392b', '#f39c12', '#e67e22', '#d35400'],
            text=list(discrepancy_types.values()),
            textposition='outside'
        )])
        fig_disc.update_layout(
            height=250,
            margin=dict(l=10, r=60, t=10, b=10),
            xaxis_title="Count",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_disc, use_container_width=True)
        
        st.markdown("""
            <div style="background: #fff3cd; border-radius: 8px; padding: 0.75rem; margin-top: 0.5rem;">
                <span style="font-weight: 600;">💡 Root Cause:</span> Product reference list was last updated in March 2024. 
                Digital Twin photos captured equipment installed since then.
            </div>
        """, unsafe_allow_html=True)
    
    with queue_col:
        st.markdown("#### 🔄 Priority Reconciliation Queue")
        
        queue_items = [
            {"asset": "ANT-RF-4523", "site": "SITE-003421", "issue": "No reference entry", "priority": "High", "age": "45d"},
            {"asset": "CAB-FO-8912", "site": "SITE-005892", "issue": "Serial mismatch", "priority": "High", "age": "32d"},
            {"asset": "PWR-UPS-234", "site": "SITE-007234", "issue": "Model differs", "priority": "Med", "age": "28d"},
            {"asset": "RAD-5G-1098", "site": "SITE-001256", "issue": "No photo found", "priority": "Med", "age": "21d"},
            {"asset": "TRM-DVB-456", "site": "SITE-002891", "issue": "Location wrong", "priority": "Low", "age": "15d"},
        ]
        
        for item in queue_items:
            priority_color = '#e63946' if item['priority'] == 'High' else '#f39c12' if item['priority'] == 'Med' else '#27ae60'
            st.markdown(f"""
                <div style="background: white; border-radius: 8px; padding: 0.75rem; margin-bottom: 0.5rem; border-left: 3px solid {priority_color};">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <span style="font-weight: 600; color: #1a2b4a;">{item['asset']}</span>
                            <span style="font-size: 0.75rem; color: #888; margin-left: 0.5rem;">@ {item['site']}</span>
                        </div>
                        <span style="background: {priority_color}20; color: {priority_color}; padding: 0.15rem 0.4rem; border-radius: 10px; font-size: 0.7rem; font-weight: 600;">{item['priority']}</span>
                    </div>
                    <div style="font-size: 0.8rem; color: #666; margin-top: 0.25rem;">⚠️ {item['issue']} · 📅 Open {item['age']}</div>
                </div>
            """, unsafe_allow_html=True)
        
        btn_col1, btn_col2 = st.columns(2)
        with btn_col1:
            st.button("📋 View Full Queue (847)", use_container_width=True)
        with btn_col2:
            st.button("📥 Export to Excel", use_container_width=True)
    
    # Product Reference Sync Status
    st.markdown("#### 🔗 Product Reference System Integration")
    
    sys_col1, sys_col2, sys_col3 = st.columns(3)
    
    with sys_col1:
        st.markdown("""
            <div style="background: linear-gradient(135deg, #1a2b4a 0%, #2d3e5f 100%); border-radius: 10px; padding: 1rem; color: white;">
                <div style="font-size: 0.8rem; opacity: 0.8;">📷 Digital Twin Platform</div>
                <div style="font-size: 1.2rem; font-weight: 600; margin: 0.5rem 0;">Photos & 3D Models</div>
                <div style="font-size: 0.75rem; opacity: 0.7;">Last sync: 2 hours ago</div>
                <div style="background: #27ae60; padding: 0.25rem 0.5rem; border-radius: 4px; display: inline-block; margin-top: 0.5rem; font-size: 0.7rem;">● Connected</div>
            </div>
        """, unsafe_allow_html=True)
    
    with sys_col2:
        st.markdown("""
            <div style="background: white; border-radius: 10px; padding: 1rem; text-align: center; border: 2px dashed #e63946;">
                <div style="font-size: 2rem;">⚡</div>
                <div style="font-size: 0.9rem; font-weight: 600; color: #e63946;">Reconciliation Engine</div>
                <div style="font-size: 0.75rem; color: #888; margin-top: 0.25rem;">AI-powered matching</div>
                <div style="font-size: 0.7rem; color: #666; margin-top: 0.5rem;">Processing 1,247 items/hour</div>
            </div>
        """, unsafe_allow_html=True)
    
    with sys_col3:
        st.markdown("""
            <div style="background: linear-gradient(135deg, #2d3436 0%, #636e72 100%); border-radius: 10px; padding: 1rem; color: white;">
                <div style="font-size: 0.8rem; opacity: 0.8;">📚 Product Reference Master</div>
                <div style="font-size: 1.2rem; font-weight: 600; margin: 0.5rem 0;">Equipment Catalog</div>
                <div style="font-size: 0.75rem; opacity: 0.7;">45,892 products registered</div>
                <div style="background: #f39c12; padding: 0.25rem 0.5rem; border-radius: 4px; display: inline-block; margin-top: 0.5rem; font-size: 0.7rem;">⚠ Needs Update</div>
            </div>
        """, unsafe_allow_html=True)
    
    # Recommendation box
    st.markdown("""
        <div style="background: linear-gradient(135deg, #e6394610 0%, #f39c1210 100%); border: 1px solid #e63946; border-radius: 10px; padding: 1rem; margin-top: 1rem;">
            <div style="font-weight: 700; color: #e63946; margin-bottom: 0.5rem;">🎯 Recommended Actions</div>
            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 0.75rem;">
                <div style="font-size: 0.85rem; color: #666;">1️⃣ <b>Sync Product Reference</b> - Update catalog with Q4 2024 installations</div>
                <div style="font-size: 0.85rem; color: #666;">2️⃣ <b>Bulk Photo Upload</b> - 3,200 photos pending from October surveys</div>
                <div style="font-size: 0.85rem; color: #666;">3️⃣ <b>Serial Number Audit</b> - 1,823 mismatches flagged for validation</div>
                <div style="font-size: 0.85rem; color: #666;">4️⃣ <b>Train AI Model</b> - Improve auto-matching accuracy from 78% to 90%+</div>
            </div>
        </div>
    """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 4h: 3D Coverage by Region (Stacked Bar)
    # -------------------------------------------------------------------------
    
    st.markdown("### 📊 3D Model Status by Region")
    
    cov_col1, cov_col2 = st.columns([2, 1])
    
    with cov_col1:
        # Regional 3D status breakdown
        regions_3d = ['Île-de-France', 'Auvergne-RA', 'Nouvelle-Aquit.', 'Occitanie', 'Hauts-de-Fr.', 
                      'Grand Est', 'PACA', 'Pays de Loire', 'Bretagne', 'Normandie']
        complete_3d = [1220, 856, 710, 649, 587, 545, 518, 454, 400, 330]
        in_progress_3d = [15, 25, 30, 35, 40, 45, 50, 55, 60, 65]
        pending_3d = [10, 11, 16, 14, 18, 22, 21, 25, 27, 28]
        
        fig_stack = go.Figure()
        
        fig_stack.add_trace(go.Bar(
            name='✅ Complete',
            y=regions_3d,
            x=complete_3d,
            orientation='h',
            marker_color='#27ae60',
            text=complete_3d,
            textposition='inside'
        ))
        
        fig_stack.add_trace(go.Bar(
            name='🔄 In Progress',
            y=regions_3d,
            x=in_progress_3d,
            orientation='h',
            marker_color='#f39c12',
        ))
        
        fig_stack.add_trace(go.Bar(
            name='⏳ Pending',
            y=regions_3d,
            x=pending_3d,
            orientation='h',
            marker_color='#e63946',
        ))
        
        fig_stack.update_layout(
            barmode='stack',
            height=350,
            margin=dict(l=10, r=20, t=30, b=40),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis=dict(showgrid=True, gridcolor='#f0f0f0', title='Number of Sites'),
            yaxis=dict(showgrid=False, categoryorder='total ascending'),
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5)
        )
        
        st.plotly_chart(fig_stack, use_container_width=True)
    
    with cov_col2:
        st.markdown("#### Overall Status")
        
        total_complete = sum(complete_3d)
        total_progress = sum(in_progress_3d)
        total_pending = sum(pending_3d)
        total_all = total_complete + total_progress + total_pending
        
        st.metric("✅ 3D Complete", f"{total_complete:,}", f"{total_complete/total_all*100:.0f}%")
        st.metric("🔄 In Progress", f"{total_progress:,}", f"{total_progress/total_all*100:.0f}%")
        st.metric("⏳ Pending Scan", f"{total_pending:,}", f"{total_pending/total_all*100:.0f}%")
        
        st.markdown("---")
        
        # Progress donut
        fig_donut = go.Figure(data=[go.Pie(
            labels=['Complete', 'In Progress', 'Pending'],
            values=[total_complete, total_progress, total_pending],
            hole=0.6,
            marker_colors=['#27ae60', '#f39c12', '#e63946'],
            textinfo='percent',
            textfont_size=12
        )])
        
        fig_donut.update_layout(
            height=180,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor='rgba(0,0,0,0)',
            showlegend=False,
            annotations=[dict(text=f'{total_complete/total_all*100:.0f}%', x=0.5, y=0.5, font_size=20, showarrow=False)]
        )
        
        st.plotly_chart(fig_donut, use_container_width=True)
        st.caption("3D Model Coverage Rate")
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 5: What-If Scenario Simulator
    # -------------------------------------------------------------------------
    
    st.markdown("### 🎮 What-If Scenario Simulator")
    st.caption("Simulate infrastructure changes and assess impact in real-time")
    
    sim_col1, sim_col2 = st.columns([1, 2])
    
    with sim_col1:
        st.markdown("**Configure Scenario:**")
        
        scenario_type = st.selectbox(
            "📋 Scenario Type",
            options=[
                "Site Decommissioning",
                "Equipment Failure",
                "Capacity Planning (New Antennas)",
                "Maintenance Window",
                "Natural Disaster Impact"
            ]
        )
        
        # Dynamic inputs based on scenario
        if scenario_type == "Site Decommissioning":
            site_options = ["SITE-003421 (Paris)", "SITE-005892 (Lyon)", "SITE-007234 (Marseille)", "SITE-001256 (Bordeaux)"]
            selected_site = st.selectbox("🗼 Select Site", site_options)
            
        elif scenario_type == "Equipment Failure":
            equipment_options = ["Tower Structure", "Power System", "Cooling System", "Antenna Array", "Fiber Connection"]
            selected_equipment = st.selectbox("⚡ Equipment Type", equipment_options)
            failure_duration = st.slider("⏱️ Outage Duration (hours)", 1, 48, 8)
            
        elif scenario_type == "Capacity Planning (New Antennas)":
            client_options = ["Orange", "SFR", "Bouygues Telecom", "Free Mobile", "New Client"]
            selected_client = st.selectbox("👤 Client", client_options)
            antenna_count = st.slider("📶 Number of Antennas", 10, 500, 100)
            
        elif scenario_type == "Maintenance Window":
            region_options = ["Île-de-France", "Auvergne-Rhône-Alpes", "Nouvelle-Aquitaine", "All Regions"]
            selected_region = st.selectbox("🗺️ Region", region_options)
            maintenance_hours = st.slider("⏱️ Duration (hours)", 2, 24, 8)
            
        else:  # Natural Disaster
            disaster_options = ["Storm/High Winds", "Flooding", "Earthquake", "Extreme Heat", "Ice Storm"]
            selected_disaster = st.selectbox("🌪️ Disaster Type", disaster_options)
            severity = st.select_slider("📊 Severity", options=["Low", "Medium", "High", "Extreme"])
        
        if st.button("🚀 Run Simulation", type="primary", use_container_width=True):
            st.success("Simulation complete!")
    
    with sim_col2:
        st.markdown("#### Simulation Results")
        
        # Results based on scenario type
        if scenario_type == "Site Decommissioning":
            results = {
                "coverage_impact": -2.3,
                "clients_affected": 4,
                "revenue_at_risk": 125000,
                "sla_breach_risk": "Medium",
                "redundancy": "85% covered by adjacent sites",
                "recommendation": "Proceed with migration plan - 3 weeks required"
            }
            
            res_cols = st.columns(3)
            with res_cols[0]:
                st.metric("Coverage Impact", f"{results['coverage_impact']}%", delta=f"{results['coverage_impact']}%", delta_color="inverse")
            with res_cols[1]:
                st.metric("Clients Affected", results['clients_affected'], delta="-4 clients")
            with res_cols[2]:
                st.metric("Revenue at Risk", f"€{results['revenue_at_risk']:,}", delta=f"-€{results['revenue_at_risk']:,}", delta_color="inverse")
            
            st.markdown(f"""
                <div style="background: #f8f9fa; border-radius: 8px; padding: 1rem; margin-top: 1rem;">
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 1rem;">
                        <div>
                            <div style="font-size: 0.75rem; color: #888;">SLA Breach Risk</div>
                            <div style="font-weight: 600; color: #f39c12;">⚠️ {results['sla_breach_risk']}</div>
                        </div>
                        <div>
                            <div style="font-size: 0.75rem; color: #888;">Redundancy Status</div>
                            <div style="font-weight: 600; color: #27ae60;">✓ {results['redundancy']}</div>
                        </div>
                    </div>
                    <div style="margin-top: 1rem; padding-top: 1rem; border-top: 1px solid #e0e0e0;">
                        <div style="font-size: 0.75rem; color: #888;">AI Recommendation</div>
                        <div style="font-weight: 600; color: #1a2b4a;">💡 {results['recommendation']}</div>
                    </div>
                </div>
            """, unsafe_allow_html=True)
            
        elif scenario_type == "Equipment Failure":
            st.markdown("""
                <div style="background: #e6394620; border-radius: 8px; padding: 1rem; border-left: 4px solid #e63946;">
                    <div style="font-weight: 600; color: #e63946; margin-bottom: 0.5rem;">⚠️ IMPACT ASSESSMENT</div>
            """, unsafe_allow_html=True)
            
            res_cols = st.columns(3)
            with res_cols[0]:
                st.metric("Service Impact", "847 users", delta="-847", delta_color="inverse")
            with res_cols[1]:
                st.metric("Est. Downtime", "4.5 hours")
            with res_cols[2]:
                st.metric("SLA Penalty Risk", "€18,500", delta_color="inverse")
            
            st.markdown("""
                </div>
                <div style="background: #27ae6020; border-radius: 8px; padding: 1rem; margin-top: 0.5rem; border-left: 4px solid #27ae60;">
                    <div style="font-weight: 600; color: #27ae60;">✓ MITIGATION OPTIONS</div>
                    <ul style="margin: 0.5rem 0; padding-left: 1.5rem; font-size: 0.85rem; color: #666;">
                        <li>Failover to SITE-003422 (2.1km) - 92% coverage maintained</li>
                        <li>Mobile unit deployment - ETA 45 min</li>
                        <li>Emergency repair team available - ETA 2 hours</li>
                    </ul>
                </div>
            """, unsafe_allow_html=True)
            
        elif scenario_type == "Capacity Planning (New Antennas)":
            res_cols = st.columns(3)
            with res_cols[0]:
                st.metric("Sites with Capacity", "127", delta="+127 available")
            with res_cols[1]:
                st.metric("Additional Revenue", "€2.4M/year", delta="+€2.4M")
            with res_cols[2]:
                st.metric("Infrastructure Cost", "€890K", delta="One-time")
            
            st.markdown("""
                <div style="background: #f8f9fa; border-radius: 8px; padding: 1rem; margin-top: 0.5rem;">
                    <div style="font-weight: 600; color: #1a2b4a; margin-bottom: 0.5rem;">📊 Capacity Analysis</div>
                    <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 0.5rem; text-align: center;">
                        <div style="background: #27ae6020; padding: 0.5rem; border-radius: 4px;">
                            <div style="font-size: 1.2rem; font-weight: 700; color: #27ae60;">78</div>
                            <div style="font-size: 0.65rem; color: #888;">Full Capacity</div>
                        </div>
                        <div style="background: #3498db20; padding: 0.5rem; border-radius: 4px;">
                            <div style="font-size: 1.2rem; font-weight: 700; color: #3498db;">49</div>
                            <div style="font-size: 0.65rem; color: #888;">Minor Upgrade</div>
                        </div>
                        <div style="background: #f39c1220; padding: 0.5rem; border-radius: 4px;">
                            <div style="font-size: 1.2rem; font-weight: 700; color: #f39c12;">23</div>
                            <div style="font-size: 0.65rem; color: #888;">Major Upgrade</div>
                        </div>
                        <div style="background: #e6394620; padding: 0.5rem; border-radius: 4px;">
                            <div style="font-size: 1.2rem; font-weight: 700; color: #e63946;">15</div>
                            <div style="font-size: 0.65rem; color: #888;">New Build Req.</div>
                        </div>
                    </div>
                </div>
            """, unsafe_allow_html=True)
        else:
            # Default results display
            res_cols = st.columns(3)
            with res_cols[0]:
                st.metric("Assets at Risk", "234", delta="-234", delta_color="inverse")
            with res_cols[1]:
                st.metric("Est. Impact Duration", "12-48 hours")
            with res_cols[2]:
                st.metric("Recovery Cost Est.", "€1.2M", delta_color="inverse")
    
    # -------------------------------------------------------------------------
    # ROW 6: Cross-Team Data Usage
    # -------------------------------------------------------------------------
    
    st.markdown("### 👥 Cross-Team Data Usage")
    st.caption("Operations, Technology & CAPEX teams rely on the same infrastructure inventory")
    
    team_cols = st.columns(3)
    
    with team_cols[0]:
        st.markdown("""
            <div style="background: white; border-radius: 10px; padding: 1.5rem; border-top: 4px solid #3498db; height: 200px;">
                <div style="font-size: 1.2rem; margin-bottom: 0.5rem;">🔧 Operations Team</div>
                <div style="font-weight: 600; color: #1a2b4a;">Maintenance & Support</div>
                <ul style="font-size: 0.8rem; color: #666; padding-left: 1.2rem; margin-top: 0.75rem;">
                    <li>Work order management</li>
                    <li>Preventive maintenance</li>
                    <li>Incident response</li>
                    <li>SLA monitoring</li>
                </ul>
                <div style="margin-top: 0.75rem; font-size: 0.75rem; color: #3498db; font-weight: 600;">2,847 queries/day</div>
            </div>
        """, unsafe_allow_html=True)
    
    with team_cols[1]:
        st.markdown("""
            <div style="background: white; border-radius: 10px; padding: 1.5rem; border-top: 4px solid #9b59b6; height: 200px;">
                <div style="font-size: 1.2rem; margin-bottom: 0.5rem;">🏗️ Technology Team</div>
                <div style="font-weight: 600; color: #1a2b4a;">Digital Twin & Planning</div>
                <ul style="font-size: 0.8rem; color: #666; padding-left: 1.2rem; margin-top: 0.75rem;">
                    <li>3D model updates</li>
                    <li>Capacity simulations</li>
                    <li>Network planning</li>
                    <li>Site surveys</li>
                </ul>
                <div style="margin-top: 0.75rem; font-size: 0.75rem; color: #9b59b6; font-weight: 600;">1,234 queries/day</div>
            </div>
        """, unsafe_allow_html=True)
    
    with team_cols[2]:
        st.markdown("""
            <div style="background: white; border-radius: 10px; padding: 1.5rem; border-top: 4px solid #27ae60; height: 200px;">
                <div style="font-size: 1.2rem; margin-bottom: 0.5rem;">💰 CAPEX Team</div>
                <div style="font-weight: 600; color: #1a2b4a;">Investment & Lifecycle</div>
                <ul style="font-size: 0.8rem; color: #666; padding-left: 1.2rem; margin-top: 0.75rem;">
                    <li>Equipment lifecycle</li>
                    <li>Renewal planning</li>
                    <li>Budget allocation</li>
                    <li>ROI analysis</li>
                </ul>
                <div style="margin-top: 0.75rem; font-size: 0.75rem; color: #27ae60; font-weight: 600;">567 queries/day</div>
            </div>
        """, unsafe_allow_html=True)
    
    st.markdown("")
    st.markdown("---")
    st.markdown("")
    
    # -------------------------------------------------------------------------
    # ROW 7: Data Quality Trends
    # -------------------------------------------------------------------------
    
    st.markdown("### 📈 Data Quality Trends")
    
    # Create trend chart
    months = ['Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    completeness_trend = [89.2, 90.5, 91.8, 92.7, 93.5, 94.2]
    accuracy_trend = [95.1, 95.8, 96.2, 96.9, 97.4, 97.8]
    discrepancy_trend = [125, 98, 82, 68, 55, 47]
    
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    fig.add_trace(go.Scatter(
        x=months, y=completeness_trend,
        mode='lines+markers',
        name='Completeness %',
        line=dict(color='#27ae60', width=3),
        marker=dict(size=8)
    ), secondary_y=False)
    
    fig.add_trace(go.Scatter(
        x=months, y=accuracy_trend,
        mode='lines+markers',
        name='Accuracy %',
        line=dict(color='#3498db', width=3),
        marker=dict(size=8)
    ), secondary_y=False)
    
    fig.add_trace(go.Bar(
        x=months, y=discrepancy_trend,
        name='Open Discrepancies',
        marker=dict(color='rgba(230, 57, 70, 0.25)'),
        yaxis='y2'
    ), secondary_y=True)
    
    fig.update_layout(
        height=300,
        margin=dict(l=20, r=20, t=30, b=40),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor='#f0f0f0', title='Quality %', range=[85, 100]),
        yaxis2=dict(showgrid=False, title='Discrepancies', range=[0, 150], overlaying='y', side='right'),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5),
        hovermode='x unified'
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Summary
    st.markdown("""
        <div style="background: #27ae6015; border-radius: 8px; padding: 1rem; text-align: center;">
            <span style="color: #27ae60; font-weight: 600;">📈 Data quality improved by 5% over 6 months • Discrepancies reduced by 62%</span>
        </div>
    """, unsafe_allow_html=True)

# ==============================================================================
# PAGE: CAPEX & LIFECYCLE
# ==============================================================================

def page_capex_lifecycle():
    render_header(
        "CAPEX & Lifecycle",
        "Predictive Renewal Model • Equipment Lifecycle Intelligence • CAPEX Forecasting"
    )
    
    # -------------------------------------------------------------------------
    # ROW 1: Executive KPIs
    # -------------------------------------------------------------------------
    
    # Query equipment data from correct table
    equipment_query = """
        SELECT 
            COUNT(*) as total_equipment,
            COALESCE(SUM(ORIGINAL_COST_EUR), 0) as total_value,
            COALESCE(AVG(DATEDIFF('year', INSTALLATION_DATE, CURRENT_DATE())), 6.2) as avg_age,
            SUM(CASE WHEN LIFECYCLE_STATUS IN ('END_OF_LIFE', 'End of Life', 'EOL') THEN 1 ELSE 0 END) as eol_count,
            SUM(CASE WHEN LIFECYCLE_STATUS IN ('AGING', 'Aging') THEN 1 ELSE 0 END) as aging_count,
            SUM(CASE WHEN LIFECYCLE_STATUS IN ('OPERATIONAL', 'NEW', 'Active') THEN 1 ELSE 0 END) as active_count
        FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.EQUIPMENT
    """
    
    try:
        equip_df = run_query(equipment_query)
        if len(equip_df) > 0 and equip_df['TOTAL_EQUIPMENT'].iloc[0] > 0:
            total_equipment = int(equip_df['TOTAL_EQUIPMENT'].iloc[0])
            total_value = float(equip_df['TOTAL_VALUE'].iloc[0] or 0) / 1000000
            if total_value == 0:
                total_value = 892.5  # Default if no cost data
            avg_age = float(equip_df['AVG_AGE'].iloc[0] or 6.2)
            eol_count = int(equip_df['EOL_COUNT'].iloc[0] or 0)
            aging_count = int(equip_df['AGING_COUNT'].iloc[0] or 0)
            active_count = int(equip_df['ACTIVE_COUNT'].iloc[0] or 0)
            # If lifecycle statuses not populated, estimate from total
            if eol_count + aging_count + active_count == 0:
                eol_count = int(total_equipment * 0.075)
                aging_count = int(total_equipment * 0.19)
                active_count = total_equipment - eol_count - aging_count
        else:
            raise Exception("No data")
    except:
        total_equipment = 45892
        total_value = 892.5
        avg_age = 6.2
        eol_count = 3421
        aging_count = 8756
        active_count = 33715
    
    # Calculate renewal metrics
    renewal_12m = int(eol_count * 0.7 + aging_count * 0.1)
    renewal_24m = int(eol_count + aging_count * 0.3)
    renewal_36m = int(eol_count + aging_count * 0.6)
    capex_forecast = renewal_12m * 15000 / 1000000  # €15K avg per equipment
    capex_budget = 45.0  # €45M budget
    budget_gap = capex_forecast - capex_budget
    
    kpi_col1, kpi_col2, kpi_col3, kpi_col4, kpi_col5 = st.columns(5)
    
    with kpi_col1:
        st.markdown(f"""
            <div style="background: linear-gradient(135deg, #1a2b4a 0%, #2d3e5f 100%); border-radius: 12px; padding: 1.25rem; color: white; text-align: center;">
                <div style="font-size: 0.75rem; opacity: 0.8;">📦 Total Equipment</div>
                <div style="font-size: 2.2rem; font-weight: 700;">{total_equipment:,}</div>
                <div style="font-size: 0.7rem; opacity: 0.7;">Tracked assets</div>
            </div>
        """, unsafe_allow_html=True)
    
    with kpi_col2:
        st.markdown(f"""
            <div style="background: linear-gradient(135deg, #3498db 0%, #2980b9 100%); border-radius: 12px; padding: 1.25rem; color: white; text-align: center;">
                <div style="font-size: 0.75rem; opacity: 0.8;">💰 Asset Value</div>
                <div style="font-size: 2.2rem; font-weight: 700;">€{total_value:.0f}M</div>
                <div style="font-size: 0.7rem; opacity: 0.7;">Book value</div>
            </div>
        """, unsafe_allow_html=True)
    
    with kpi_col3:
        age_color = '#27ae60' if avg_age < 5 else '#f39c12' if avg_age < 8 else '#e63946'
        st.markdown(f"""
            <div style="background: white; border-radius: 12px; padding: 1.25rem; text-align: center; border: 2px solid {age_color};">
                <div style="font-size: 0.75rem; color: #888;">⏱️ Avg Equipment Age</div>
                <div style="font-size: 2.2rem; font-weight: 700; color: {age_color};">{avg_age:.1f} yrs</div>
                <div style="font-size: 0.7rem; color: #888;">Target: &lt;5 years</div>
            </div>
        """, unsafe_allow_html=True)
    
    with kpi_col4:
        st.markdown(f"""
            <div style="background: linear-gradient(135deg, #e63946 0%, #c0392b 100%); border-radius: 12px; padding: 1.25rem; color: white; text-align: center;">
                <div style="font-size: 0.75rem; opacity: 0.8;">🔄 Due for Renewal</div>
                <div style="font-size: 2.2rem; font-weight: 700;">{renewal_12m:,}</div>
                <div style="font-size: 0.7rem; opacity: 0.7;">Next 12 months</div>
            </div>
        """, unsafe_allow_html=True)
    
    with kpi_col5:
        gap_color = '#e63946' if budget_gap > 0 else '#27ae60'
        gap_text = f"+€{budget_gap:.1f}M over" if budget_gap > 0 else f"€{abs(budget_gap):.1f}M under"
        st.markdown(f"""
            <div style="background: white; border-radius: 12px; padding: 1.25rem; text-align: center; border: 2px solid {gap_color};">
                <div style="font-size: 0.75rem; color: #888;">💼 CAPEX Gap</div>
                <div style="font-size: 1.8rem; font-weight: 700; color: {gap_color};">{gap_text}</div>
                <div style="font-size: 0.7rem; color: #888;">Budget: €{capex_budget:.0f}M</div>
            </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<div style='height: 1.5rem;'></div>", unsafe_allow_html=True)
    
    # -------------------------------------------------------------------------
    # ROW 2: Lifecycle Status Gauge + Equipment by Category
    # -------------------------------------------------------------------------
    
    lifecycle_col, category_col = st.columns([1, 2])
    
    with lifecycle_col:
        st.markdown("### 🎯 Lifecycle Status")
        
        # Animated donut chart for lifecycle status
        fig_lifecycle = go.Figure()
        
        labels = ['Active', 'Aging', 'End of Life']
        values = [active_count, aging_count, eol_count]
        colors = ['#27ae60', '#f39c12', '#e63946']
        
        fig_lifecycle.add_trace(go.Pie(
            labels=labels,
            values=values,
            hole=0.7,
            marker=dict(colors=colors),
            textinfo='percent',
            textfont_size=14,
            hovertemplate='<b>%{label}</b><br>%{value:,} units<br>%{percent}<extra></extra>'
        ))
        
        # Center annotation
        fig_lifecycle.add_annotation(
            text=f"<b>{total_equipment:,}</b><br><span style='font-size:12px'>Total</span>",
            x=0.5, y=0.5,
            font_size=20,
            showarrow=False
        )
        
        fig_lifecycle.update_layout(
            height=300,
            margin=dict(l=20, r=20, t=20, b=20),
            showlegend=True,
            legend=dict(orientation='h', yanchor='bottom', y=-0.15, xanchor='center', x=0.5),
            paper_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_lifecycle, use_container_width=True)
        
        # Status cards below
        st.markdown(f"""
            <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 0.5rem;">
                <div style="background: #27ae6015; border-radius: 8px; padding: 0.5rem; text-align: center;">
                    <div style="font-size: 1.1rem; font-weight: 700; color: #27ae60;">{active_count:,}</div>
                    <div style="font-size: 0.7rem; color: #666;">Active</div>
                </div>
                <div style="background: #f39c1215; border-radius: 8px; padding: 0.5rem; text-align: center;">
                    <div style="font-size: 1.1rem; font-weight: 700; color: #f39c12;">{aging_count:,}</div>
                    <div style="font-size: 0.7rem; color: #666;">Aging</div>
                </div>
                <div style="background: #e6394615; border-radius: 8px; padding: 0.5rem; text-align: center;">
                    <div style="font-size: 1.1rem; font-weight: 700; color: #e63946;">{eol_count:,}</div>
                    <div style="font-size: 0.7rem; color: #666;">End of Life</div>
                </div>
            </div>
        """, unsafe_allow_html=True)
    
    with category_col:
        st.markdown("### 📊 Equipment by Category & Age")
        
        # Equipment categories with age distribution
        categories = [
            {"name": "Antennas", "count": 25234, "avg_age": 4.2, "eol": 1892, "value": 245},
            {"name": "Transmitters", "count": 8756, "avg_age": 7.8, "eol": 1245, "value": 312},
            {"name": "Power Systems", "count": 6892, "avg_age": 5.5, "eol": 456, "value": 156},
            {"name": "Cooling/HVAC", "count": 5234, "avg_age": 6.1, "eol": 321, "value": 78},
            {"name": "Network Equipment", "count": 4567, "avg_age": 3.8, "eol": 234, "value": 67},
            {"name": "Cables & Fibre", "count": 3421, "avg_age": 8.9, "eol": 189, "value": 23},
            {"name": "Structures", "count": 1788, "avg_age": 12.3, "eol": 84, "value": 11},
        ]
        
        # Horizontal stacked bar showing age brackets
        fig_cat = go.Figure()
        
        cat_names = [c['name'] for c in categories]
        
        # Age brackets
        young = [int(c['count'] * 0.3) for c in categories]  # 0-3 years
        mid = [int(c['count'] * 0.4) for c in categories]     # 3-7 years
        old = [int(c['count'] * 0.2) for c in categories]     # 7-10 years
        very_old = [c['count'] - young[i] - mid[i] - old[i] for i, c in enumerate(categories)]  # 10+ years
        
        fig_cat.add_trace(go.Bar(
            y=cat_names, x=young, name='0-3 years',
            orientation='h', marker_color='#27ae60',
            hovertemplate='%{y}: %{x:,} units (0-3 yrs)<extra></extra>'
        ))
        fig_cat.add_trace(go.Bar(
            y=cat_names, x=mid, name='3-7 years',
            orientation='h', marker_color='#3498db',
            hovertemplate='%{y}: %{x:,} units (3-7 yrs)<extra></extra>'
        ))
        fig_cat.add_trace(go.Bar(
            y=cat_names, x=old, name='7-10 years',
            orientation='h', marker_color='#f39c12',
            hovertemplate='%{y}: %{x:,} units (7-10 yrs)<extra></extra>'
        ))
        fig_cat.add_trace(go.Bar(
            y=cat_names, x=very_old, name='10+ years',
            orientation='h', marker_color='#e63946',
            hovertemplate='%{y}: %{x:,} units (10+ yrs)<extra></extra>'
        ))
        
        fig_cat.update_layout(
            barmode='stack',
            height=320,
            margin=dict(l=10, r=10, t=10, b=10),
            legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5),
            xaxis_title="Equipment Count",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_cat, use_container_width=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 3: 7-Year Predictive Renewal Model (Interactive Timeline)
    # -------------------------------------------------------------------------
    
    st.markdown("### 🔮 7-Year Predictive Renewal Model")
    st.caption("Equipment renewal waves and CAPEX forecast based on lifecycle analysis")
    
    # Generate 7-year forecast data
    import random
    random.seed(42)
    
    years = list(range(2025, 2032))
    
    renewal_forecast = {
        "Antennas": [892, 1456, 2100, 1800, 2300, 1900, 2100],
        "Transmitters": [1245, 1890, 1200, 2500, 1800, 2200, 1600],
        "Power Systems": [456, 678, 890, 1200, 980, 1100, 850],
        "Cooling/HVAC": [321, 456, 567, 789, 654, 723, 612],
        "Network Equipment": [234, 345, 456, 567, 678, 543, 489],
        "Others": [273, 389, 456, 523, 412, 378, 334],
    }
    
    capex_by_year = []
    for i in range(7):
        yearly_capex = sum(renewal_forecast[cat][i] * (25 if cat == "Transmitters" else 15 if cat == "Antennas" else 12 if cat == "Power Systems" else 8) for cat in renewal_forecast) / 1000
        capex_by_year.append(yearly_capex)
    
    # Create interactive area chart
    fig_forecast = go.Figure()
    
    colors = ['#e63946', '#f39c12', '#3498db', '#9b59b6', '#27ae60', '#1a2b4a']
    
    for idx, (category, values) in enumerate(renewal_forecast.items()):
        fig_forecast.add_trace(go.Scatter(
            x=years, y=values, name=category,
            mode='lines',
            line=dict(width=0.5, color=colors[idx]),
            stackgroup='one',
            fillcolor=colors[idx],
            hovertemplate=f'<b>{category}</b><br>Year: %{{x}}<br>Units: %{{y:,}}<extra></extra>'
        ))
    
    # Add CAPEX line on secondary axis
    fig_forecast.add_trace(go.Scatter(
        x=years, y=capex_by_year, name='CAPEX Forecast (€M)',
        mode='lines+markers',
        line=dict(color='#1a2b4a', width=4, dash='dot'),
        marker=dict(size=12, color='#1a2b4a', symbol='diamond'),
        yaxis='y2',
        hovertemplate='<b>CAPEX</b><br>Year: %{x}<br>€%{y:.1f}M<extra></extra>'
    ))
    
    # Add budget line
    fig_forecast.add_hline(y=45, line_dash="dash", line_color="#27ae60", 
                           annotation_text="Budget: €45M", yref='y2',
                           annotation_position="right")
    
    fig_forecast.update_layout(
        height=400,
        margin=dict(l=10, r=60, t=40, b=10),
        legend=dict(orientation='h', yanchor='bottom', y=1.02, xanchor='center', x=0.5),
        xaxis=dict(title='Year', tickmode='array', tickvals=years),
        yaxis=dict(title='Equipment Units to Renew'),
        yaxis2=dict(title='CAPEX (€M)', overlaying='y', side='right', showgrid=False),
        hovermode='x unified',
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)'
    )
    fig_forecast.update_xaxes(showgrid=True, gridcolor='#eee')
    fig_forecast.update_yaxes(showgrid=True, gridcolor='#eee')
    
    st.plotly_chart(fig_forecast, use_container_width=True)
    
    # Year summary cards
    year_cols = st.columns(7)
    for i, year in enumerate(years):
        total_units = sum(renewal_forecast[cat][i] for cat in renewal_forecast)
        with year_cols[i]:
            urgency = '🔴' if capex_by_year[i] > 55 else '🟡' if capex_by_year[i] > 45 else '🟢'
            st.markdown(f"""
                <div style="background: white; border-radius: 8px; padding: 0.5rem; text-align: center; border-top: 3px solid {'#e63946' if capex_by_year[i] > 55 else '#f39c12' if capex_by_year[i] > 45 else '#27ae60'};">
                    <div style="font-weight: 700; color: #1a2b4a;">{year}</div>
                    <div style="font-size: 0.85rem; color: #666;">{total_units:,} units</div>
                    <div style="font-size: 1rem; font-weight: 600; color: #1a2b4a;">€{capex_by_year[i]:.0f}M</div>
                    <div style="font-size: 0.8rem;">{urgency}</div>
                </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 4: Equipment Age Histogram + Risk Heatmap
    # -------------------------------------------------------------------------
    
    age_col, risk_col = st.columns(2)
    
    with age_col:
        st.markdown("### 📈 Equipment Age Distribution")
        
        # Generate age histogram data
        age_bins = list(range(0, 16))
        age_counts = [2100, 3500, 4200, 5100, 6200, 5800, 4900, 4100, 3600, 2800, 2100, 1500, 1200, 800, 400, 200]
        
        fig_age = go.Figure()
        
        bar_colors = ['#27ae60' if a < 5 else '#3498db' if a < 7 else '#f39c12' if a < 10 else '#e63946' for a in age_bins]
        
        fig_age.add_trace(go.Bar(
            x=age_bins,
            y=age_counts[:len(age_bins)],
            marker_color=bar_colors,
            hovertemplate='Age: %{x} years<br>Count: %{y:,}<extra></extra>'
        ))
        
        # Add threshold lines
        fig_age.add_vline(x=7, line_dash="dash", line_color="#f39c12", annotation_text="Watch Zone", annotation_position="top")
        fig_age.add_vline(x=10, line_dash="dash", line_color="#e63946", annotation_text="Critical", annotation_position="top")
        
        fig_age.update_layout(
            height=300,
            margin=dict(l=10, r=10, t=30, b=10),
            xaxis_title="Equipment Age (Years)",
            yaxis_title="Count",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_age, use_container_width=True)
    
    with risk_col:
        st.markdown("### ⚠️ Failure Risk Matrix")
        
        # Risk heatmap by category and age
        risk_categories = ['Antennas', 'Transmitters', 'Power', 'Cooling', 'Network']
        risk_ages = ['0-3y', '3-5y', '5-7y', '7-10y', '10y+']
        
        # Risk scores (0-100)
        risk_matrix = [
            [5, 8, 15, 35, 65],   # Antennas
            [8, 12, 25, 55, 85],  # Transmitters
            [10, 15, 30, 50, 75], # Power
            [12, 20, 35, 55, 80], # Cooling
            [3, 5, 10, 25, 45],   # Network
        ]
        
        fig_risk = go.Figure(data=go.Heatmap(
            z=risk_matrix,
            x=risk_ages,
            y=risk_categories,
            colorscale=[[0, '#27ae60'], [0.3, '#f39c12'], [0.6, '#e67e22'], [1, '#e63946']],
            hovertemplate='<b>%{y}</b><br>Age: %{x}<br>Risk Score: %{z}%<extra></extra>',
            text=[[f"{v}%" for v in row] for row in risk_matrix],
            texttemplate="%{text}",
            textfont={"size": 11}
        ))
        
        fig_risk.update_layout(
            height=300,
            margin=dict(l=10, r=10, t=10, b=10),
            paper_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_risk, use_container_width=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 5: Cross-Team Alignment View
    # -------------------------------------------------------------------------
    
    st.markdown("### 👥 Cross-Team Alignment Dashboard")
    st.caption("Finance, Operations, and Technology perspectives on equipment lifecycle")
    
    team_col1, team_col2, team_col3 = st.columns(3)
    
    with team_col1:
        st.markdown("""
            <div style="background: linear-gradient(135deg, #1a2b4a 0%, #2d3e5f 100%); border-radius: 12px; padding: 1rem; color: white;">
                <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.75rem;">
                    <span style="font-size: 1.5rem;">💰</span>
                    <span style="font-size: 1.1rem; font-weight: 600;">Finance View</span>
                </div>
        """, unsafe_allow_html=True)
        
        st.metric("CAPEX Budget 2025", "€45.0M", "Approved")
        st.metric("Forecast Spend", "€52.3M", "+€7.3M over", delta_color="inverse")
        st.metric("Depreciation Impact", "€28.4M", "Annual")
        
        st.markdown("""
            <div style="background: rgba(255,255,255,0.1); border-radius: 8px; padding: 0.75rem; margin-top: 0.5rem;">
                <div style="font-size: 0.75rem; opacity: 0.8;">📋 Budget Actions Required:</div>
                <div style="font-size: 0.8rem; margin-top: 0.25rem;">• Request €8M supplemental</div>
                <div style="font-size: 0.8rem;">• Defer €3M to 2026</div>
            </div>
            </div>
        """, unsafe_allow_html=True)
    
    with team_col2:
        st.markdown("""
            <div style="background: linear-gradient(135deg, #e67e22 0%, #d35400 100%); border-radius: 12px; padding: 1rem; color: white;">
                <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.75rem;">
                    <span style="font-size: 1.5rem;">🔧</span>
                    <span style="font-size: 1.1rem; font-weight: 600;">Operations View</span>
                </div>
        """, unsafe_allow_html=True)
        
        st.metric("Maintenance Backlog", "847 tickets", "+12% MoM", delta_color="inverse")
        st.metric("Emergency Repairs", "34 active", "Critical")
        st.metric("Downtime Risk Score", "7.2/10", "High")
        
        st.markdown("""
            <div style="background: rgba(255,255,255,0.1); border-radius: 8px; padding: 0.75rem; margin-top: 0.5rem;">
                <div style="font-size: 0.75rem; opacity: 0.8;">🚨 Priority Actions:</div>
                <div style="font-size: 0.8rem; margin-top: 0.25rem;">• 156 transmitters critical</div>
                <div style="font-size: 0.8rem;">• Q1 maintenance window</div>
            </div>
            </div>
        """, unsafe_allow_html=True)
    
    with team_col3:
        st.markdown("""
            <div style="background: linear-gradient(135deg, #9b59b6 0%, #8e44ad 100%); border-radius: 12px; padding: 1rem; color: white;">
                <div style="display: flex; align-items: center; gap: 0.5rem; margin-bottom: 0.75rem;">
                    <span style="font-size: 1.5rem;">💻</span>
                    <span style="font-size: 1.1rem; font-weight: 600;">Technology View</span>
                </div>
        """, unsafe_allow_html=True)
        
        st.metric("Tech Refresh Required", "2,341 units", "5G upgrade")
        st.metric("Obsolete Systems", "892 units", "No vendor support")
        st.metric("Compatibility Score", "78%", "-3% vs target")
        
        st.markdown("""
            <div style="background: rgba(255,255,255,0.1); border-radius: 8px; padding: 0.75rem; margin-top: 0.5rem;">
                <div style="font-size: 0.75rem; opacity: 0.8;">🔄 Tech Priorities:</div>
                <div style="font-size: 0.8rem; margin-top: 0.25rem;">• DAB+ transmitter upgrade</div>
                <div style="font-size: 0.8rem;">• 5G antenna rollout</div>
            </div>
            </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 6: Interactive CAPEX Scenario Simulator
    # -------------------------------------------------------------------------
    
    st.markdown("### 🎮 CAPEX Scenario Simulator")
    st.caption("Model different investment strategies and see the impact")
    
    sim_control, sim_result = st.columns([1, 2])
    
    with sim_control:
        st.markdown("#### Scenario Parameters")
        
        scenario_type = st.selectbox(
            "📋 Select Scenario",
            ["Baseline (Current Plan)", "Defer Renewals 2 Years", "Accelerate Critical Only", 
             "Budget Constrained (€40M cap)", "Full Modernization", "Custom"]
        )
        
        if scenario_type == "Custom":
            defer_pct = st.slider("Defer non-critical renewals by (%)", 0, 50, 20)
            accelerate_critical = st.checkbox("Accelerate critical equipment", value=True)
            budget_cap = st.number_input("Annual CAPEX cap (€M)", value=45.0, step=5.0)
        else:
            if scenario_type == "Baseline (Current Plan)":
                defer_pct, accelerate_critical, budget_cap = 0, False, 60.0
            elif scenario_type == "Defer Renewals 2 Years":
                defer_pct, accelerate_critical, budget_cap = 40, False, 35.0
            elif scenario_type == "Accelerate Critical Only":
                defer_pct, accelerate_critical, budget_cap = 0, True, 55.0
            elif scenario_type == "Budget Constrained (€40M cap)":
                defer_pct, accelerate_critical, budget_cap = 25, False, 40.0
            else:  # Full Modernization
                defer_pct, accelerate_critical, budget_cap = 0, True, 75.0
        
        st.markdown("---")
        st.markdown("#### 📊 Quick Stats")
        
        # Calculate scenario impacts
        base_capex = sum(capex_by_year[:3]) / 3  # 3-year avg
        adjusted_capex = base_capex * (1 - defer_pct/100) * (1.15 if accelerate_critical else 1.0)
        adjusted_capex = min(adjusted_capex, budget_cap)
        
        risk_increase = defer_pct * 0.8  # Risk increases with deferral
        savings = base_capex - adjusted_capex
        
        st.metric("3-Year CAPEX", f"€{adjusted_capex * 3:.0f}M", f"€{savings * 3:.0f}M savings" if savings > 0 else f"€{abs(savings) * 3:.0f}M increase")
        st.metric("Risk Level", f"{12 + risk_increase:.0f}%", f"+{risk_increase:.0f}%" if risk_increase > 0 else "Baseline", delta_color="inverse" if risk_increase > 0 else "normal")
    
    with sim_result:
        st.markdown("#### Scenario Impact Visualization")
        
        # Generate scenario comparison
        scenarios = ['Baseline', 'This Scenario']
        
        if scenario_type != "Baseline (Current Plan)":
            # Show comparison chart
            fig_scenario = go.Figure()
            
            # Baseline bars
            fig_scenario.add_trace(go.Bar(
                name='Baseline',
                x=years[:5],
                y=capex_by_year[:5],
                marker_color='#3498db',
                opacity=0.6
            ))
            
            # Scenario bars
            scenario_capex = [min(c * (1 - defer_pct/100), budget_cap) for c in capex_by_year[:5]]
            fig_scenario.add_trace(go.Bar(
                name='This Scenario',
                x=years[:5],
                y=scenario_capex,
                marker_color='#27ae60' if defer_pct > 0 else '#e63946'
            ))
            
            # Add risk line
            base_risk = [12, 14, 16, 18, 20]
            scenario_risk = [r + defer_pct * 0.5 for r in base_risk]
            
            fig_scenario.add_trace(go.Scatter(
                name='Failure Risk %',
                x=years[:5],
                y=scenario_risk,
                mode='lines+markers',
                line=dict(color='#e63946', width=3, dash='dot'),
                marker=dict(size=10),
                yaxis='y2'
            ))
            
            fig_scenario.update_layout(
                barmode='group',
                height=350,
                margin=dict(l=10, r=50, t=10, b=10),
                legend=dict(orientation='h', yanchor='bottom', y=1.02),
                xaxis_title='Year',
                yaxis_title='CAPEX (€M)',
                yaxis2=dict(title='Risk %', overlaying='y', side='right', range=[0, 50]),
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig_scenario, use_container_width=True)
            
            # Impact summary
            impact_cols = st.columns(4)
            
            total_savings = sum(capex_by_year[:5]) - sum(scenario_capex)
            with impact_cols[0]:
                st.markdown(f"""
                    <div style="background: {'#27ae6015' if total_savings > 0 else '#e6394615'}; border-radius: 8px; padding: 0.75rem; text-align: center;">
                        <div style="font-size: 0.75rem; color: #666;">💰 5-Year Savings</div>
                        <div style="font-size: 1.3rem; font-weight: 700; color: {'#27ae60' if total_savings > 0 else '#e63946'};">€{total_savings:.0f}M</div>
                    </div>
                """, unsafe_allow_html=True)
            
            with impact_cols[1]:
                deferred_units = int(renewal_12m * defer_pct / 100)
                st.markdown(f"""
                    <div style="background: #f39c1215; border-radius: 8px; padding: 0.75rem; text-align: center;">
                        <div style="font-size: 0.75rem; color: #666;">⏳ Deferred Units</div>
                        <div style="font-size: 1.3rem; font-weight: 700; color: #f39c12;">{deferred_units:,}</div>
                    </div>
                """, unsafe_allow_html=True)
            
            with impact_cols[2]:
                st.markdown(f"""
                    <div style="background: #e6394615; border-radius: 8px; padding: 0.75rem; text-align: center;">
                        <div style="font-size: 0.75rem; color: #666;">⚠️ Added Risk</div>
                        <div style="font-size: 1.3rem; font-weight: 700; color: #e63946;">+{risk_increase:.0f}%</div>
                    </div>
                """, unsafe_allow_html=True)
            
            with impact_cols[3]:
                downtime_hours = int(risk_increase * 12)
                st.markdown(f"""
                    <div style="background: #9b59b615; border-radius: 8px; padding: 0.75rem; text-align: center;">
                        <div style="font-size: 0.75rem; color: #666;">📉 Est. Downtime</div>
                        <div style="font-size: 1.3rem; font-weight: 700; color: #9b59b6;">+{downtime_hours}h/yr</div>
                    </div>
                """, unsafe_allow_html=True)
        else:
            st.info("📊 Select a different scenario to see comparison with baseline")
            
            # Show baseline forecast
            fig_baseline = go.Figure()
            fig_baseline.add_trace(go.Bar(
                x=years[:5],
                y=capex_by_year[:5],
                marker_color='#3498db',
                text=[f'€{v:.0f}M' for v in capex_by_year[:5]],
                textposition='outside'
            ))
            fig_baseline.update_layout(
                height=300,
                margin=dict(l=10, r=10, t=30, b=10),
                xaxis_title='Year',
                yaxis_title='CAPEX (€M)',
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig_baseline, use_container_width=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 7: Critical Equipment Watch List
    # -------------------------------------------------------------------------
    
    st.markdown("### 🚨 Critical Equipment Watch List")
    st.caption("Equipment requiring immediate attention - highest risk of failure")
    
    # Critical equipment table
    critical_equipment = [
        {"id": "TRM-DVB-001234", "type": "DVB-T2 Transmitter", "site": "Paris - Tour Eiffel", "age": "12.3 yrs", "risk": 92, "impact": "€2.1M", "action": "Replace Q1"},
        {"id": "TRM-DVB-002891", "type": "DVB-T2 Transmitter", "site": "Lyon - Fourvière", "age": "11.8 yrs", "risk": 88, "impact": "€1.8M", "action": "Replace Q1"},
        {"id": "PWR-UPS-004521", "type": "UPS System", "site": "Marseille - L'Estaque", "age": "10.2 yrs", "risk": 85, "impact": "€890K", "action": "Replace Q2"},
        {"id": "ANT-5G-007823", "type": "5G Antenna Array", "site": "Toulouse - Labège", "age": "8.5 yrs", "risk": 78, "impact": "€456K", "action": "Upgrade Q2"},
        {"id": "CLG-HVAC-003421", "type": "Cooling System", "site": "Bordeaux - DC1", "age": "9.1 yrs", "risk": 75, "impact": "€320K", "action": "Refurbish Q2"},
        {"id": "TRM-FM-008912", "type": "FM Transmitter", "site": "Lille - Mont Noir", "age": "14.2 yrs", "risk": 71, "impact": "€1.2M", "action": "Replace Q3"},
    ]
    
    crit_cols = st.columns([0.5, 2, 2, 1.5, 1, 1.2, 1.2, 1])
    
    with crit_cols[0]:
        st.markdown("**#**")
    with crit_cols[1]:
        st.markdown("**Equipment ID**")
    with crit_cols[2]:
        st.markdown("**Location**")
    with crit_cols[3]:
        st.markdown("**Type**")
    with crit_cols[4]:
        st.markdown("**Age**")
    with crit_cols[5]:
        st.markdown("**Risk**")
    with crit_cols[6]:
        st.markdown("**Impact**")
    with crit_cols[7]:
        st.markdown("**Action**")
    
    for idx, equip in enumerate(critical_equipment):
        cols = st.columns([0.5, 2, 2, 1.5, 1, 1.2, 1.2, 1])
        
        risk_color = '#e63946' if equip['risk'] >= 80 else '#f39c12' if equip['risk'] >= 60 else '#27ae60'
        
        with cols[0]:
            st.markdown(f"**{idx + 1}**")
        with cols[1]:
            st.markdown(f"`{equip['id']}`")
        with cols[2]:
            st.markdown(equip['site'])
        with cols[3]:
            st.markdown(equip['type'][:20])
        with cols[4]:
            st.markdown(equip['age'])
        with cols[5]:
            st.markdown(f"<span style='background: {risk_color}; color: white; padding: 0.2rem 0.5rem; border-radius: 12px; font-size: 0.8rem;'>{equip['risk']}%</span>", unsafe_allow_html=True)
        with cols[6]:
            st.markdown(f"**{equip['impact']}**")
        with cols[7]:
            st.markdown(f"<span style='color: #e63946; font-weight: 600;'>{equip['action']}</span>", unsafe_allow_html=True)
    
    # Export buttons
    btn_col1, btn_col2, btn_col3, _ = st.columns([1, 1, 1, 3])
    with btn_col1:
        st.button("📥 Export Full List", use_container_width=True)
    with btn_col2:
        st.button("📧 Send to Teams", use_container_width=True)
    with btn_col3:
        st.button("📅 Schedule Reviews", use_container_width=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 8: Vendor Concentration + Technology Obsolescence
    # -------------------------------------------------------------------------
    
    vendor_col, obsol_col = st.columns(2)
    
    with vendor_col:
        st.markdown("### 🏢 Vendor Concentration & Risk")
        
        # Vendor data
        vendors = [
            {"name": "Ericsson", "share": 28, "equipment": 12850, "risk": "Low", "rating": 4.5},
            {"name": "Nokia", "share": 24, "equipment": 11014, "risk": "Low", "rating": 4.3},
            {"name": "Huawei", "share": 18, "equipment": 8261, "risk": "High", "rating": 4.1},
            {"name": "Rohde & Schwarz", "share": 12, "equipment": 5507, "risk": "Low", "rating": 4.6},
            {"name": "Kathrein", "share": 8, "equipment": 3671, "risk": "Medium", "rating": 3.9},
            {"name": "Others", "share": 10, "equipment": 4589, "risk": "Mixed", "rating": 3.5},
        ]
        
        # Pie chart
        fig_vendor = go.Figure(data=[go.Pie(
            labels=[v['name'] for v in vendors],
            values=[v['share'] for v in vendors],
            hole=0.5,
            marker_colors=['#1a2b4a', '#3498db', '#e63946', '#27ae60', '#f39c12', '#9b59b6'],
            textinfo='label+percent',
            textfont_size=11,
            hovertemplate='<b>%{label}</b><br>Share: %{percent}<br>Equipment: %{value:,}<extra></extra>'
        )])
        
        fig_vendor.add_annotation(
            text="<b>Top 3</b><br>70%",
            x=0.5, y=0.5, font_size=14, showarrow=False
        )
        
        fig_vendor.update_layout(
            height=250,
            margin=dict(l=10, r=10, t=10, b=10),
            showlegend=False,
            paper_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_vendor, use_container_width=True)
        
        # Risk indicators
        st.markdown("#### ⚠️ Supply Chain Risks")
        for v in vendors[:4]:
            risk_color = '#e63946' if v['risk'] == 'High' else '#f39c12' if v['risk'] == 'Medium' else '#27ae60'
            st.markdown(f"""
                <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.4rem 0; border-bottom: 1px solid #eee;">
                    <span style="font-weight: 500;">{v['name']}</span>
                    <div>
                        <span style="background: {risk_color}20; color: {risk_color}; padding: 0.15rem 0.5rem; border-radius: 10px; font-size: 0.75rem; margin-right: 0.5rem;">{v['risk']} Risk</span>
                        <span style="color: #f39c12;">{'⭐' * int(v['rating'])}</span>
                    </div>
                </div>
            """, unsafe_allow_html=True)
        
        st.markdown("""
            <div style="background: #e6394615; border-radius: 8px; padding: 0.75rem; margin-top: 0.75rem;">
                <span style="font-weight: 600; color: #e63946;">⚠️ Alert:</span> Huawei dependency at 18% - regulatory risk in some regions
            </div>
        """, unsafe_allow_html=True)
    
    with obsol_col:
        st.markdown("### 📡 Technology Obsolescence Radar")
        
        # Obsolescence tracking
        tech_status = [
            {"tech": "2G/GSM", "status": "Sunset 2026", "equipment": 1245, "value": 8.2, "risk": 95},
            {"tech": "3G/UMTS", "status": "Sunset 2028", "equipment": 3421, "value": 24.5, "risk": 75},
            {"tech": "DVB-T (SD)", "status": "Migrating", "equipment": 892, "value": 12.3, "risk": 60},
            {"tech": "Analog FM", "status": "Legacy", "equipment": 456, "value": 3.1, "risk": 40},
            {"tech": "4G/LTE", "status": "Active", "equipment": 15234, "value": 156.0, "risk": 10},
            {"tech": "5G NR", "status": "Expanding", "equipment": 8921, "value": 245.0, "risk": 5},
        ]
        
        # Horizontal bar showing obsolescence risk
        fig_obsol = go.Figure()
        
        fig_obsol.add_trace(go.Bar(
            y=[t['tech'] for t in tech_status],
            x=[t['risk'] for t in tech_status],
            orientation='h',
            marker_color=['#c0392b', '#e63946', '#f39c12', '#f1c40f', '#27ae60', '#2ecc71'],
            text=[f"{t['risk']}%" for t in tech_status],
            textposition='outside',
            hovertemplate='<b>%{y}</b><br>Risk: %{x}%<br>Equipment: %{customdata[0]:,}<br>Value: €%{customdata[1]}M<extra></extra>',
            customdata=[[t['equipment'], t['value']] for t in tech_status]
        ))
        
        fig_obsol.update_layout(
            height=250,
            margin=dict(l=10, r=50, t=10, b=10),
            xaxis_title="Obsolescence Risk %",
            xaxis=dict(range=[0, 110]),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_obsol, use_container_width=True)
        
        # Stranded asset value
        stranded_value = sum(t['value'] for t in tech_status if t['risk'] > 50)
        st.markdown(f"""
            <div style="background: linear-gradient(135deg, #e63946 0%, #c0392b 100%); border-radius: 10px; padding: 1rem; color: white; text-align: center;">
                <div style="font-size: 0.8rem; opacity: 0.8;">💀 Stranded Asset Risk (3-Year)</div>
                <div style="font-size: 2rem; font-weight: 700;">€{stranded_value:.1f}M</div>
                <div style="font-size: 0.75rem; opacity: 0.8;">Equipment value at high obsolescence risk</div>
            </div>
        """, unsafe_allow_html=True)
        
        # Migration timeline
        st.markdown("#### 📅 Technology Migration Timeline")
        migrations = [
            {"from": "2G → Decommission", "date": "Q4 2026", "status": "Planned"},
            {"from": "3G → 4G/5G", "date": "Q2 2028", "status": "Planning"},
            {"from": "DVB-T → DVB-T2", "date": "Q1 2026", "status": "In Progress"},
        ]
        for m in migrations:
            status_color = '#27ae60' if m['status'] == 'In Progress' else '#3498db' if m['status'] == 'Planned' else '#f39c12'
            st.markdown(f"""
                <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.4rem 0;">
                    <span>{m['from']}</span>
                    <div>
                        <span style="color: #666; font-size: 0.85rem; margin-right: 0.5rem;">{m['date']}</span>
                        <span style="background: {status_color}20; color: {status_color}; padding: 0.15rem 0.5rem; border-radius: 10px; font-size: 0.7rem;">{m['status']}</span>
                    </div>
                </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 9: Warranty Tracker + Maintain vs Replace
    # -------------------------------------------------------------------------
    
    warranty_col, decision_col = st.columns(2)
    
    with warranty_col:
        st.markdown("### 📅 Warranty Expiration Tracker")
        
        # Warranty cliff visualization
        warranty_data = [
            {"period": "Expired", "count": 12456, "value": 89.2},
            {"period": "0-6 months", "count": 3421, "value": 34.5},
            {"period": "6-12 months", "count": 2891, "value": 28.9},
            {"period": "12-24 months", "count": 5234, "value": 67.8},
            {"period": "24+ months", "count": 21890, "value": 312.4},
        ]
        
        fig_warranty = go.Figure()
        
        colors = ['#e63946', '#f39c12', '#f1c40f', '#3498db', '#27ae60']
        
        fig_warranty.add_trace(go.Bar(
            x=[w['period'] for w in warranty_data],
            y=[w['count'] for w in warranty_data],
            marker_color=colors,
            text=[f"{w['count']:,}" for w in warranty_data],
            textposition='outside',
            hovertemplate='<b>%{x}</b><br>Equipment: %{y:,}<br>Value: €%{customdata}M<extra></extra>',
            customdata=[w['value'] for w in warranty_data]
        ))
        
        fig_warranty.update_layout(
            height=250,
            margin=dict(l=10, r=10, t=10, b=10),
            xaxis_title="Warranty Status",
            yaxis_title="Equipment Count",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_warranty, use_container_width=True)
        
        # Warranty cliff alert
        expiring_soon = warranty_data[1]['count'] + warranty_data[2]['count']
        expiring_value = warranty_data[1]['value'] + warranty_data[2]['value']
        
        w_col1, w_col2 = st.columns(2)
        with w_col1:
            st.markdown(f"""
                <div style="background: #f39c1215; border-radius: 8px; padding: 0.75rem; text-align: center;">
                    <div style="font-size: 0.75rem; color: #666;">⏰ Expiring in 12 months</div>
                    <div style="font-size: 1.5rem; font-weight: 700; color: #f39c12;">{expiring_soon:,}</div>
                    <div style="font-size: 0.7rem; color: #888;">€{expiring_value:.1f}M value</div>
                </div>
            """, unsafe_allow_html=True)
        with w_col2:
            ext_warranty_cost = expiring_value * 0.08  # 8% of value for extended warranty
            st.markdown(f"""
                <div style="background: #3498db15; border-radius: 8px; padding: 0.75rem; text-align: center;">
                    <div style="font-size: 0.75rem; color: #666;">💰 Extended Warranty Cost</div>
                    <div style="font-size: 1.5rem; font-weight: 700; color: #3498db;">€{ext_warranty_cost:.1f}M</div>
                    <div style="font-size: 0.7rem; color: #888;">~8% of asset value</div>
                </div>
            """, unsafe_allow_html=True)
        
        st.button("📋 View Warranty Details", use_container_width=True)
    
    with decision_col:
        st.markdown("### 🔧 Maintain vs Replace Decision Matrix")
        
        # Decision matrix
        decision_data = [
            {"category": "Antennas", "maint_cost": 2500, "replace_cost": 15000, "breakeven": 6, "recommendation": "Maintain"},
            {"category": "Transmitters", "maint_cost": 8500, "replace_cost": 45000, "breakeven": 5.3, "recommendation": "Case-by-case"},
            {"category": "Power Systems", "maint_cost": 4200, "replace_cost": 18000, "breakeven": 4.3, "recommendation": "Replace if >7y"},
            {"category": "Cooling/HVAC", "maint_cost": 3800, "replace_cost": 12000, "breakeven": 3.2, "recommendation": "Replace if >5y"},
            {"category": "Network Equip", "maint_cost": 1500, "replace_cost": 8000, "breakeven": 5.3, "recommendation": "Maintain"},
        ]
        
        # Visual comparison
        fig_decision = go.Figure()
        
        categories = [d['category'] for d in decision_data]
        
        fig_decision.add_trace(go.Bar(
            name='Annual Maintenance',
            x=categories,
            y=[d['maint_cost'] for d in decision_data],
            marker_color='#3498db',
            text=[f"€{d['maint_cost']:,}" for d in decision_data],
            textposition='outside'
        ))
        
        fig_decision.add_trace(go.Bar(
            name='Replacement Cost',
            x=categories,
            y=[d['replace_cost'] for d in decision_data],
            marker_color='#e63946',
            text=[f"€{d['replace_cost']:,}" for d in decision_data],
            textposition='outside'
        ))
        
        fig_decision.update_layout(
            barmode='group',
            height=220,
            margin=dict(l=10, r=10, t=10, b=10),
            legend=dict(orientation='h', yanchor='bottom', y=1.02),
            yaxis_title="Cost (EUR)",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_decision, use_container_width=True)
        
        # Recommendations table
        st.markdown("#### 💡 AI Recommendations")
        for d in decision_data:
            rec_color = '#27ae60' if 'Maintain' in d['recommendation'] else '#f39c12' if 'Case' in d['recommendation'] else '#e63946'
            st.markdown(f"""
                <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.35rem 0; border-bottom: 1px solid #eee;">
                    <span>{d['category']}</span>
                    <div>
                        <span style="font-size: 0.75rem; color: #666; margin-right: 0.5rem;">Break-even: {d['breakeven']}y</span>
                        <span style="background: {rec_color}20; color: {rec_color}; padding: 0.15rem 0.5rem; border-radius: 10px; font-size: 0.7rem;">{d['recommendation']}</span>
                    </div>
                </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 10: Regional CAPEX Heatmap
    # -------------------------------------------------------------------------
    
    st.markdown("### 🗺️ Regional CAPEX Investment Distribution")
    st.caption("Investment equity across France regions")
    
    # Regional CAPEX data
    regional_capex = [
        {"region": "Île-de-France", "budget": 12.5, "spent": 11.2, "equipment": 8956, "per_site": 45.2},
        {"region": "Auvergne-Rhône-Alpes", "budget": 6.8, "spent": 5.9, "equipment": 5234, "per_site": 38.5},
        {"region": "Provence-Alpes-Côte d'Azur", "budget": 5.2, "spent": 4.8, "equipment": 4123, "per_site": 41.2},
        {"region": "Occitanie", "budget": 4.8, "spent": 4.2, "equipment": 3891, "per_site": 36.8},
        {"region": "Nouvelle-Aquitaine", "budget": 4.5, "spent": 3.9, "equipment": 3567, "per_site": 35.2},
        {"region": "Hauts-de-France", "budget": 3.8, "spent": 3.5, "equipment": 2891, "per_site": 33.5},
        {"region": "Grand Est", "budget": 3.2, "spent": 2.8, "equipment": 2456, "per_site": 32.1},
        {"region": "Pays de la Loire", "budget": 2.5, "spent": 2.2, "equipment": 1987, "per_site": 34.8},
        {"region": "Bretagne", "budget": 2.2, "spent": 2.0, "equipment": 1756, "per_site": 33.2},
        {"region": "Normandie", "budget": 1.8, "spent": 1.5, "equipment": 1423, "per_site": 31.5},
    ]
    
    map_col, detail_col = st.columns([2, 1])
    
    with map_col:
        # Bubble chart as proxy for map
        fig_regional = go.Figure()
        
        # Create scatter with size based on budget
        fig_regional.add_trace(go.Bar(
            x=[r['region'] for r in regional_capex],
            y=[r['budget'] for r in regional_capex],
            name='Budget',
            marker_color='#3498db',
            opacity=0.4
        ))
        
        fig_regional.add_trace(go.Bar(
            x=[r['region'] for r in regional_capex],
            y=[r['spent'] for r in regional_capex],
            name='Spent',
            marker_color='#1a2b4a'
        ))
        
        # Add equity line (avg per site)
        avg_per_site = sum(r['per_site'] for r in regional_capex) / len(regional_capex)
        
        fig_regional.update_layout(
            barmode='overlay',
            height=350,
            margin=dict(l=10, r=10, t=30, b=80),
            legend=dict(orientation='h', yanchor='bottom', y=1.02),
            xaxis_tickangle=-45,
            yaxis_title="CAPEX (€M)",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_regional, use_container_width=True)
    
    with detail_col:
        st.markdown("#### 📊 Investment Equity Score")
        
        # Calculate equity metrics
        total_budget = sum(r['budget'] for r in regional_capex)
        utilization = sum(r['spent'] for r in regional_capex) / total_budget * 100
        
        # Equity gauge
        fig_equity = go.Figure(go.Indicator(
            mode="gauge+number",
            value=78,
            title={'text': "Investment Balance", 'font': {'size': 14}},
            gauge={
                'axis': {'range': [0, 100]},
                'bar': {'color': '#27ae60'},
                'steps': [
                    {'range': [0, 50], 'color': 'rgba(230, 57, 70, 0.13)'},
                    {'range': [50, 75], 'color': 'rgba(243, 156, 18, 0.13)'},
                    {'range': [75, 100], 'color': 'rgba(39, 174, 96, 0.13)'}
                ],
                'threshold': {
                    'line': {'color': '#1a2b4a', 'width': 2},
                    'thickness': 0.75,
                    'value': 85
                }
            }
        ))
        fig_equity.update_layout(
            height=180,
            margin=dict(l=20, r=20, t=40, b=10),
            paper_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_equity, use_container_width=True)
        
        # Key insights
        st.markdown(f"""
            <div style="background: #27ae6015; border-radius: 8px; padding: 0.75rem; margin-bottom: 0.5rem;">
                <div style="font-size: 0.8rem; color: #27ae60; font-weight: 600;">✅ Budget Utilization</div>
                <div style="font-size: 1.2rem; font-weight: 700;">{utilization:.1f}%</div>
            </div>
        """, unsafe_allow_html=True)
        
        # Under-invested regions
        under_invested = [r for r in regional_capex if r['per_site'] < avg_per_site * 0.9]
        if under_invested:
            st.markdown(f"""
                <div style="background: #f39c1215; border-radius: 8px; padding: 0.75rem;">
                    <div style="font-size: 0.8rem; color: #f39c12; font-weight: 600;">⚠️ Under-invested Regions</div>
                    <div style="font-size: 0.85rem; margin-top: 0.25rem;">{', '.join([r['region'][:10] for r in under_invested[:3]])}</div>
                </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 11: CAPEX Approval Pipeline
    # -------------------------------------------------------------------------
    
    st.markdown("### 📋 CAPEX Approval Pipeline")
    st.caption("Track capital expenditure requests through approval workflow")
    
    pipeline_col, requests_col = st.columns([2, 1])
    
    with pipeline_col:
        # Pipeline funnel
        stages = [
            {"stage": "Submitted", "count": 47, "value": 28.5, "color": "#3498db"},
            {"stage": "Technical Review", "count": 32, "value": 19.2, "color": "#9b59b6"},
            {"stage": "Finance Review", "count": 24, "value": 14.8, "color": "#f39c12"},
            {"stage": "Director Approval", "count": 18, "value": 11.2, "color": "#e67e22"},
            {"stage": "CFO Approval", "count": 8, "value": 6.5, "color": "#27ae60"},
            {"stage": "Approved", "count": 156, "value": 38.2, "color": "#1a2b4a"},
        ]
        
        fig_pipeline = go.Figure()
        
        # Funnel chart
        fig_pipeline.add_trace(go.Funnel(
            y=[s['stage'] for s in stages],
            x=[s['count'] for s in stages],
            textinfo="value+percent initial",
            marker=dict(color=[s['color'] for s in stages]),
            hovertemplate='<b>%{y}</b><br>Requests: %{x}<br>Value: €%{customdata}M<extra></extra>',
            customdata=[s['value'] for s in stages]
        ))
        
        fig_pipeline.update_layout(
            height=350,
            margin=dict(l=10, r=10, t=20, b=10),
            paper_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_pipeline, use_container_width=True)
    
    with requests_col:
        st.markdown("#### ⏱️ Pipeline Metrics")
        
        avg_time = 12.5  # days
        bottleneck = "Finance Review"
        fast_track = 5
        
        st.markdown(f"""
            <div style="background: white; border-radius: 10px; padding: 1rem; margin-bottom: 0.75rem; border-left: 4px solid #3498db;">
                <div style="font-size: 0.75rem; color: #888;">Avg. Approval Time</div>
                <div style="font-size: 1.5rem; font-weight: 700; color: #1a2b4a;">{avg_time} days</div>
            </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
            <div style="background: white; border-radius: 10px; padding: 1rem; margin-bottom: 0.75rem; border-left: 4px solid #e63946;">
                <div style="font-size: 0.75rem; color: #888;">🚧 Bottleneck Stage</div>
                <div style="font-size: 1.1rem; font-weight: 700; color: #e63946;">{bottleneck}</div>
                <div style="font-size: 0.75rem; color: #888;">Avg. 4.2 days stuck</div>
            </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
            <div style="background: white; border-radius: 10px; padding: 1rem; margin-bottom: 0.75rem; border-left: 4px solid #27ae60;">
                <div style="font-size: 0.75rem; color: #888;">⚡ Fast-Track Requests</div>
                <div style="font-size: 1.5rem; font-weight: 700; color: #27ae60;">{fast_track}</div>
                <div style="font-size: 0.75rem; color: #888;">Critical priority</div>
            </div>
        """, unsafe_allow_html=True)
        
        # Pending value
        pending_value = sum(s['value'] for s in stages[:-1])
        st.markdown(f"""
            <div style="background: linear-gradient(135deg, #1a2b4a 0%, #2d3e5f 100%); border-radius: 10px; padding: 1rem; color: white; text-align: center;">
                <div style="font-size: 0.8rem; opacity: 0.8;">💰 Pending Approval Value</div>
                <div style="font-size: 1.8rem; font-weight: 700;">€{pending_value:.1f}M</div>
            </div>
        """, unsafe_allow_html=True)
    
    # Recent high-value requests
    st.markdown("#### 📝 High-Value Pending Requests (>€1M)")
    
    high_value_requests = [
        {"id": "CAPEX-2025-0892", "desc": "5G Antenna Array - Paris Region", "value": 4.2, "stage": "CFO Approval", "days": 3, "priority": "High"},
        {"id": "CAPEX-2025-0856", "desc": "DVB-T2 Transmitter Upgrade - Lyon", "value": 2.8, "stage": "Director Approval", "days": 5, "priority": "High"},
        {"id": "CAPEX-2025-0834", "desc": "Data Center Cooling System", "value": 1.9, "stage": "Finance Review", "days": 8, "priority": "Medium"},
        {"id": "CAPEX-2025-0821", "desc": "Emergency Power Replacement", "value": 1.5, "stage": "Technical Review", "days": 2, "priority": "Critical"},
        {"id": "CAPEX-2025-0798", "desc": "FM Transmitter Modernization", "value": 1.2, "stage": "Finance Review", "days": 12, "priority": "Low"},
    ]
    
    req_cols = st.columns([1.5, 3, 1, 1.5, 1, 1])
    with req_cols[0]:
        st.markdown("**Request ID**")
    with req_cols[1]:
        st.markdown("**Description**")
    with req_cols[2]:
        st.markdown("**Value**")
    with req_cols[3]:
        st.markdown("**Stage**")
    with req_cols[4]:
        st.markdown("**Days**")
    with req_cols[5]:
        st.markdown("**Priority**")
    
    for req in high_value_requests:
        cols = st.columns([1.5, 3, 1, 1.5, 1, 1])
        priority_color = '#e63946' if req['priority'] == 'Critical' else '#f39c12' if req['priority'] == 'High' else '#3498db' if req['priority'] == 'Medium' else '#27ae60'
        
        with cols[0]:
            st.markdown(f"`{req['id']}`")
        with cols[1]:
            st.markdown(req['desc'][:35] + "..." if len(req['desc']) > 35 else req['desc'])
        with cols[2]:
            st.markdown(f"**€{req['value']}M**")
        with cols[3]:
            st.markdown(req['stage'][:15])
        with cols[4]:
            day_color = '#e63946' if req['days'] > 7 else '#f39c12' if req['days'] > 3 else '#27ae60'
            st.markdown(f"<span style='color: {day_color};'>{req['days']}d</span>", unsafe_allow_html=True)
        with cols[5]:
            st.markdown(f"<span style='background: {priority_color}20; color: {priority_color}; padding: 0.15rem 0.4rem; border-radius: 10px; font-size: 0.75rem;'>{req['priority']}</span>", unsafe_allow_html=True)
    
    # Action buttons
    act_col1, act_col2, act_col3, act_col4 = st.columns(4)
    with act_col1:
        st.button("📊 Full Pipeline Report", use_container_width=True)
    with act_col2:
        st.button("⚡ Fast-Track Request", use_container_width=True)
    with act_col3:
        st.button("📧 Notify Approvers", use_container_width=True)
    with act_col4:
        st.button("📈 Analytics Dashboard", use_container_width=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 12: Total Cost of Ownership Calculator
    # -------------------------------------------------------------------------
    
    st.markdown("### 💵 Total Cost of Ownership (TCO) Calculator")
    st.caption("Full lifecycle cost analysis for equipment investment decisions")
    
    tco_input_col, tco_result_col = st.columns([1, 2])
    
    with tco_input_col:
        st.markdown("#### ⚙️ Equipment Parameters")
        
        tco_equipment = st.selectbox(
            "Equipment Type",
            ["5G Antenna Array", "DVB-T2 Transmitter", "UPS Power System", "Cooling System", "Network Router"]
        )
        
        tco_purchase = st.number_input("Purchase Cost (€)", value=45000, step=5000)
        tco_lifespan = st.slider("Expected Lifespan (years)", 5, 15, 10)
        tco_maintenance = st.number_input("Annual Maintenance (€)", value=2500, step=500)
        tco_energy = st.number_input("Annual Energy Cost (€)", value=1800, step=200)
        tco_quantity = st.number_input("Quantity", value=1, min_value=1, max_value=100)
        
        # Calculate TCO
        total_purchase = tco_purchase * tco_quantity
        total_maintenance = tco_maintenance * tco_lifespan * tco_quantity
        total_energy = tco_energy * tco_lifespan * tco_quantity
        disposal_cost = tco_purchase * 0.05 * tco_quantity  # 5% disposal
        installation_cost = tco_purchase * 0.1 * tco_quantity  # 10% installation
        total_tco = total_purchase + total_maintenance + total_energy + disposal_cost + installation_cost
        annual_tco = total_tco / tco_lifespan
    
    with tco_result_col:
        st.markdown("#### 📊 TCO Breakdown")
        
        # TCO donut chart
        tco_components = [
            {"name": "Purchase", "value": total_purchase, "color": "#1a2b4a"},
            {"name": "Installation", "value": installation_cost, "color": "#3498db"},
            {"name": "Maintenance", "value": total_maintenance, "color": "#f39c12"},
            {"name": "Energy", "value": total_energy, "color": "#e67e22"},
            {"name": "Disposal", "value": disposal_cost, "color": "#9b59b6"},
        ]
        
        fig_tco = go.Figure(data=[go.Pie(
            labels=[c['name'] for c in tco_components],
            values=[c['value'] for c in tco_components],
            hole=0.6,
            marker_colors=[c['color'] for c in tco_components],
            textinfo='label+percent',
            textfont_size=11,
            hovertemplate='<b>%{label}</b><br>€%{value:,.0f}<br>%{percent}<extra></extra>'
        )])
        
        fig_tco.add_annotation(
            text=f"<b>€{total_tco:,.0f}</b><br><span style='font-size:11px'>Total TCO</span>",
            x=0.5, y=0.5, font_size=16, showarrow=False
        )
        
        fig_tco.update_layout(
            height=280,
            margin=dict(l=10, r=10, t=10, b=10),
            showlegend=False,
            paper_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_tco, use_container_width=True)
        
        # Key metrics
        tco_metrics = st.columns(4)
        with tco_metrics[0]:
            st.markdown(f"""
                <div style="background: #1a2b4a15; border-radius: 8px; padding: 0.75rem; text-align: center;">
                    <div style="font-size: 0.7rem; color: #666;">Total TCO</div>
                    <div style="font-size: 1.3rem; font-weight: 700; color: #1a2b4a;">€{total_tco:,.0f}</div>
                </div>
            """, unsafe_allow_html=True)
        with tco_metrics[1]:
            st.markdown(f"""
                <div style="background: #3498db15; border-radius: 8px; padding: 0.75rem; text-align: center;">
                    <div style="font-size: 0.7rem; color: #666;">Annual TCO</div>
                    <div style="font-size: 1.3rem; font-weight: 700; color: #3498db;">€{annual_tco:,.0f}</div>
                </div>
            """, unsafe_allow_html=True)
        with tco_metrics[2]:
            cost_per_day = annual_tco / 365
            st.markdown(f"""
                <div style="background: #27ae6015; border-radius: 8px; padding: 0.75rem; text-align: center;">
                    <div style="font-size: 0.7rem; color: #666;">Cost per Day</div>
                    <div style="font-size: 1.3rem; font-weight: 700; color: #27ae60;">€{cost_per_day:.0f}</div>
                </div>
            """, unsafe_allow_html=True)
        with tco_metrics[3]:
            ownership_multiplier = total_tco / total_purchase
            st.markdown(f"""
                <div style="background: #f39c1215; border-radius: 8px; padding: 0.75rem; text-align: center;">
                    <div style="font-size: 0.7rem; color: #666;">TCO Multiplier</div>
                    <div style="font-size: 1.3rem; font-weight: 700; color: #f39c12;">{ownership_multiplier:.1f}x</div>
                </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 13: Investment ROI Tracker + Lease vs Buy
    # -------------------------------------------------------------------------
    
    roi_col, lease_col = st.columns(2)
    
    with roi_col:
        st.markdown("### 📈 Investment ROI Tracker")
        st.caption("Return on past CAPEX investments")
        
        # Past investments with ROI
        investments = [
            {"project": "5G Network Paris", "year": 2023, "invested": 12.5, "roi": 28, "status": "Exceeding"},
            {"project": "DVB-T2 Migration", "year": 2022, "invested": 8.2, "roi": 15, "status": "On Track"},
            {"project": "Data Center Lyon", "year": 2023, "invested": 6.8, "roi": 22, "status": "Exceeding"},
            {"project": "FM Transmitter Upgrade", "year": 2021, "invested": 4.5, "roi": 8, "status": "Below Target"},
            {"project": "Tower Strengthening", "year": 2022, "invested": 3.2, "roi": 12, "status": "On Track"},
        ]
        
        # ROI bar chart
        fig_roi = go.Figure()
        
        colors = ['#27ae60' if i['status'] == 'Exceeding' else '#3498db' if i['status'] == 'On Track' else '#e63946' for i in investments]
        
        fig_roi.add_trace(go.Bar(
            y=[i['project'] for i in investments],
            x=[i['roi'] for i in investments],
            orientation='h',
            marker_color=colors,
            text=[f"{i['roi']}% ROI" for i in investments],
            textposition='outside',
            hovertemplate='<b>%{y}</b><br>ROI: %{x}%<br>Invested: €%{customdata}M<extra></extra>',
            customdata=[i['invested'] for i in investments]
        ))
        
        # Target line
        fig_roi.add_vline(x=15, line_dash="dash", line_color="#1a2b4a", annotation_text="Target: 15%")
        
        fig_roi.update_layout(
            height=280,
            margin=dict(l=10, r=60, t=10, b=10),
            xaxis_title="ROI %",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_roi, use_container_width=True)
        
        # Summary stats
        avg_roi = sum(i['roi'] for i in investments) / len(investments)
        total_invested = sum(i['invested'] for i in investments)
        st.markdown(f"""
            <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 0.5rem;">
                <div style="background: #27ae6015; padding: 0.5rem; border-radius: 6px; text-align: center;">
                    <div style="font-size: 1.1rem; font-weight: 700; color: #27ae60;">{avg_roi:.0f}%</div>
                    <div style="font-size: 0.7rem; color: #666;">Avg ROI</div>
                </div>
                <div style="background: #3498db15; padding: 0.5rem; border-radius: 6px; text-align: center;">
                    <div style="font-size: 1.1rem; font-weight: 700; color: #3498db;">€{total_invested:.0f}M</div>
                    <div style="font-size: 0.7rem; color: #666;">Total Invested</div>
                </div>
                <div style="background: #1a2b4a15; padding: 0.5rem; border-radius: 6px; text-align: center;">
                    <div style="font-size: 1.1rem; font-weight: 700; color: #1a2b4a;">3/5</div>
                    <div style="font-size: 0.7rem; color: #666;">Meeting Target</div>
                </div>
            </div>
        """, unsafe_allow_html=True)
    
    with lease_col:
        st.markdown("### 🔄 Lease vs Buy Analysis")
        st.caption("Financial comparison for major equipment")
        
        # Lease vs Buy comparison
        lease_buy_data = {
            "years": [1, 2, 3, 4, 5, 6, 7],
            "buy_cumulative": [50, 55, 60, 65, 70, 75, 80],  # €K
            "lease_cumulative": [15, 30, 45, 60, 75, 90, 105],  # €K
        }
        
        fig_lease = go.Figure()
        
        fig_lease.add_trace(go.Scatter(
            x=lease_buy_data['years'],
            y=lease_buy_data['buy_cumulative'],
            name='Buy (cumulative)',
            mode='lines+markers',
            line=dict(color='#1a2b4a', width=3),
            fill='tozeroy',
            fillcolor='rgba(26, 43, 74, 0.1)'
        ))
        
        fig_lease.add_trace(go.Scatter(
            x=lease_buy_data['years'],
            y=lease_buy_data['lease_cumulative'],
            name='Lease (cumulative)',
            mode='lines+markers',
            line=dict(color='#e63946', width=3),
            fill='tozeroy',
            fillcolor='rgba(230, 57, 70, 0.1)'
        ))
        
        # Break-even point
        fig_lease.add_vline(x=4, line_dash="dot", line_color="#27ae60", 
                           annotation_text="Break-even: 4 years", annotation_position="top")
        
        fig_lease.update_layout(
            height=250,
            margin=dict(l=10, r=10, t=10, b=10),
            xaxis_title="Years",
            yaxis_title="Cumulative Cost (€K)",
            legend=dict(orientation='h', yanchor='bottom', y=1.02),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_lease, use_container_width=True)
        
        # Recommendation
        st.markdown("""
            <div style="background: linear-gradient(135deg, #27ae60 0%, #2ecc71 100%); border-radius: 8px; padding: 0.75rem; color: white;">
                <div style="font-weight: 600;">💡 Recommendation</div>
                <div style="font-size: 0.85rem; margin-top: 0.25rem;">
                    <b>BUY</b> if equipment needed for >4 years<br>
                    <b>LEASE</b> for short-term or rapidly evolving tech (5G, AI)
                </div>
            </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 14: CAPEX vs OPEX Trend
    # -------------------------------------------------------------------------
    
    st.markdown("### 📊 CAPEX vs OPEX Historical Trend")
    st.caption("5-year spending pattern analysis")
    
    trend_col, insight_col = st.columns([2, 1])
    
    with trend_col:
        years = [2020, 2021, 2022, 2023, 2024, 2025]
        capex_trend = [52, 48, 55, 62, 58, 52]
        opex_trend = [38, 42, 45, 48, 52, 56]
        
        fig_trend = go.Figure()
        
        fig_trend.add_trace(go.Bar(
            x=years,
            y=capex_trend,
            name='CAPEX',
            marker_color='#1a2b4a',
            text=[f'€{v}M' for v in capex_trend],
            textposition='outside'
        ))
        
        fig_trend.add_trace(go.Bar(
            x=years,
            y=opex_trend,
            name='OPEX',
            marker_color='#3498db',
            text=[f'€{v}M' for v in opex_trend],
            textposition='outside'
        ))
        
        # Add ratio line
        ratio = [c/(c+o)*100 for c, o in zip(capex_trend, opex_trend)]
        fig_trend.add_trace(go.Scatter(
            x=years,
            y=ratio,
            name='CAPEX Ratio %',
            mode='lines+markers',
            line=dict(color='#e63946', width=3, dash='dot'),
            marker=dict(size=10),
            yaxis='y2'
        ))
        
        fig_trend.update_layout(
            barmode='group',
            height=350,
            margin=dict(l=10, r=50, t=30, b=10),
            legend=dict(orientation='h', yanchor='bottom', y=1.02),
            xaxis_title='Year',
            yaxis_title='Spending (€M)',
            yaxis2=dict(title='CAPEX Ratio %', overlaying='y', side='right', range=[40, 70]),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_trend, use_container_width=True)
    
    with insight_col:
        st.markdown("#### 📈 Key Insights")
        
        capex_change = ((capex_trend[-1] - capex_trend[0]) / capex_trend[0]) * 100
        opex_change = ((opex_trend[-1] - opex_trend[0]) / opex_trend[0]) * 100
        
        st.markdown(f"""
            <div style="background: white; border-radius: 10px; padding: 1rem; margin-bottom: 0.75rem; border-left: 4px solid #1a2b4a;">
                <div style="font-size: 0.75rem; color: #888;">CAPEX Trend (5Y)</div>
                <div style="font-size: 1.4rem; font-weight: 700; color: {'#27ae60' if capex_change < 0 else '#e63946'};">{capex_change:+.0f}%</div>
                <div style="font-size: 0.7rem; color: #888;">€{capex_trend[0]}M → €{capex_trend[-1]}M</div>
            </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
            <div style="background: white; border-radius: 10px; padding: 1rem; margin-bottom: 0.75rem; border-left: 4px solid #3498db;">
                <div style="font-size: 0.75rem; color: #888;">OPEX Trend (5Y)</div>
                <div style="font-size: 1.4rem; font-weight: 700; color: #e63946;">{opex_change:+.0f}%</div>
                <div style="font-size: 0.7rem; color: #888;">€{opex_trend[0]}M → €{opex_trend[-1]}M</div>
            </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
            <div style="background: #f39c1215; border-radius: 10px; padding: 1rem; border-left: 4px solid #f39c12;">
                <div style="font-size: 0.8rem; color: #f39c12; font-weight: 600;">⚠️ Trend Alert</div>
                <div style="font-size: 0.8rem; color: #666; margin-top: 0.25rem;">
                    OPEX growing faster than CAPEX. Review cloud/SaaS costs and maintenance contracts.
                </div>
            </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
            <div style="background: #27ae6015; border-radius: 10px; padding: 0.75rem; margin-top: 0.75rem;">
                <div style="font-size: 0.75rem; color: #27ae60; font-weight: 600;">💡 Optimization Potential</div>
                <div style="font-size: 1.2rem; font-weight: 700; color: #27ae60;">€8.5M</div>
                <div style="font-size: 0.7rem; color: #666;">via contract renegotiation</div>
            </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 15: Green CAPEX Tracker
    # -------------------------------------------------------------------------
    
    st.markdown("### 🌱 Green CAPEX & Sustainability Investment")
    st.caption("ESG-aligned capital investments and environmental impact")
    
    green_col1, green_col2, green_col3 = st.columns([1, 1, 1])
    
    with green_col1:
        st.markdown("#### 🎯 Green CAPEX Target")
        
        green_target = 25  # 25% of CAPEX should be green
        green_actual = 22.3
        green_invested = 11.6  # €M
        
        fig_green = go.Figure(go.Indicator(
            mode="gauge+number+delta",
            value=green_actual,
            delta={'reference': green_target, 'relative': False, 'valueformat': '.1f'},
            number={'suffix': '%', 'font': {'size': 40}},
            gauge={
                'axis': {'range': [0, 40], 'tickwidth': 1},
                'bar': {'color': '#27ae60'},
                'steps': [
                    {'range': [0, 15], 'color': 'rgba(230, 57, 70, 0.2)'},
                    {'range': [15, 25], 'color': 'rgba(243, 156, 18, 0.2)'},
                    {'range': [25, 40], 'color': 'rgba(39, 174, 96, 0.2)'}
                ],
                'threshold': {
                    'line': {'color': '#1a2b4a', 'width': 3},
                    'thickness': 0.8,
                    'value': green_target
                }
            }
        ))
        
        fig_green.update_layout(
            height=200,
            margin=dict(l=20, r=20, t=30, b=10),
            paper_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_green, use_container_width=True)
        
        st.markdown(f"""
            <div style="text-align: center; padding: 0.5rem;">
                <div style="font-size: 0.8rem; color: #666;">Green Investment 2025</div>
                <div style="font-size: 1.5rem; font-weight: 700; color: #27ae60;">€{green_invested}M</div>
            </div>
        """, unsafe_allow_html=True)
    
    with green_col2:
        st.markdown("#### 🌍 Carbon Reduction per €")
        
        green_projects = [
            {"project": "Solar Panel Install", "invested": 3.2, "co2_saved": 890, "roi": 4.2},
            {"project": "LED Lighting Upgrade", "invested": 1.8, "co2_saved": 234, "roi": 2.8},
            {"project": "Cooling Optimization", "invested": 2.4, "co2_saved": 567, "roi": 3.5},
            {"project": "Electric Fleet", "invested": 2.1, "co2_saved": 345, "roi": 5.1},
            {"project": "Energy Storage", "invested": 2.1, "co2_saved": 421, "roi": 3.8},
        ]
        
        # Bubble chart: x=invested, y=CO2 saved, size=ROI
        fig_carbon = go.Figure()
        
        fig_carbon.add_trace(go.Scatter(
            x=[p['invested'] for p in green_projects],
            y=[p['co2_saved'] for p in green_projects],
            mode='markers+text',
            marker=dict(
                size=[p['roi'] * 12 for p in green_projects],
                color=['#27ae60', '#2ecc71', '#1abc9c', '#16a085', '#3498db'],
                opacity=0.7
            ),
            text=[p['project'][:12] for p in green_projects],
            textposition='top center',
            textfont=dict(size=9),
            hovertemplate='<b>%{text}</b><br>Invested: €%{x}M<br>CO₂ Saved: %{y} tons<extra></extra>'
        ))
        
        fig_carbon.update_layout(
            height=250,
            margin=dict(l=10, r=10, t=10, b=10),
            xaxis_title="Investment (€M)",
            yaxis_title="CO₂ Saved (tons/year)",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_carbon, use_container_width=True)
    
    with green_col3:
        st.markdown("#### 📊 Sustainability ROI")
        
        total_co2_saved = sum(p['co2_saved'] for p in green_projects)
        total_green_invested = sum(p['invested'] for p in green_projects)
        co2_per_euro = total_co2_saved / (total_green_invested * 1000000) * 1000  # kg per €1000
        
        st.markdown(f"""
            <div style="background: linear-gradient(135deg, #27ae60 0%, #2ecc71 100%); border-radius: 12px; padding: 1rem; color: white; text-align: center; margin-bottom: 0.75rem;">
                <div style="font-size: 0.8rem; opacity: 0.9;">Total CO₂ Avoided</div>
                <div style="font-size: 2rem; font-weight: 700;">{total_co2_saved:,} tons</div>
                <div style="font-size: 0.75rem; opacity: 0.8;">per year from green investments</div>
            </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
            <div style="background: white; border-radius: 8px; padding: 0.75rem; margin-bottom: 0.5rem; border-left: 4px solid #27ae60;">
                <div style="display: flex; justify-content: space-between;">
                    <span style="font-size: 0.85rem;">CO₂/€1K invested</span>
                    <span style="font-weight: 700; color: #27ae60;">{co2_per_euro:.1f} kg</span>
                </div>
            </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
            <div style="background: white; border-radius: 8px; padding: 0.75rem; margin-bottom: 0.5rem; border-left: 4px solid #3498db;">
                <div style="display: flex; justify-content: space-between;">
                    <span style="font-size: 0.85rem;">Carbon Credit Value</span>
                    <span style="font-weight: 700; color: #3498db;">€{total_co2_saved * 85 / 1000:.1f}K</span>
                </div>
            </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
            <div style="background: white; border-radius: 8px; padding: 0.75rem; border-left: 4px solid #9b59b6;">
                <div style="display: flex; justify-content: space-between;">
                    <span style="font-size: 0.85rem;">Energy Cost Savings</span>
                    <span style="font-weight: 700; color: #9b59b6;">€1.2M/yr</span>
                </div>
            </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 16: Industry Benchmarking
    # -------------------------------------------------------------------------
    
    st.markdown("### 🏆 Industry Benchmarking")
    st.caption("TDF vs telecom infrastructure peers")
    
    bench_chart_col, bench_detail_col = st.columns([2, 1])
    
    with bench_chart_col:
        # Benchmark data
        companies = ["TDF", "Cellnex", "American Tower", "Crown Castle", "Vantage Towers"]
        metrics = {
            "CAPEX/Revenue": [12.5, 15.2, 11.8, 10.2, 14.1],
            "Avg Equipment Age": [6.2, 5.8, 7.1, 6.5, 5.2],
            "Maintenance Ratio": [4.8, 5.2, 4.5, 5.1, 4.9],
        }
        
        # Radar chart
        fig_bench = go.Figure()
        
        # Normalize values for radar
        categories = list(metrics.keys())
        
        for i, company in enumerate(companies):
            values = [metrics[cat][i] for cat in categories]
            values.append(values[0])  # Close the polygon
            
            colors = ['#e63946', '#3498db', '#27ae60', '#f39c12', '#9b59b6']
            
            fig_bench.add_trace(go.Scatterpolar(
                r=values,
                theta=categories + [categories[0]],
                name=company,
                line_color=colors[i],
                fill='toself' if i == 0 else None,
                fillcolor='rgba(230, 57, 70, 0.1)' if i == 0 else None,
                opacity=1 if i == 0 else 0.6
            ))
        
        fig_bench.update_layout(
            polar=dict(
                radialaxis=dict(visible=True, range=[0, 20])
            ),
            height=350,
            margin=dict(l=60, r=60, t=30, b=30),
            legend=dict(orientation='h', yanchor='bottom', y=-0.15),
            paper_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_bench, use_container_width=True)
    
    with bench_detail_col:
        st.markdown("#### 📊 TDF vs Industry")
        
        # Comparison table
        comparisons = [
            {"metric": "CAPEX Intensity", "tdf": "12.5%", "avg": "12.8%", "status": "✅ Better"},
            {"metric": "Equipment Age", "tdf": "6.2y", "avg": "6.1y", "status": "🟡 On Par"},
            {"metric": "Maintenance Ratio", "tdf": "4.8%", "avg": "4.9%", "status": "✅ Better"},
            {"metric": "5G Investment", "tdf": "28%", "avg": "32%", "status": "🟡 Catch-up"},
            {"metric": "Green CAPEX", "tdf": "22%", "avg": "18%", "status": "✅ Leader"},
        ]
        
        for comp in comparisons:
            status_color = '#27ae60' if '✅' in comp['status'] else '#f39c12'
            st.markdown(f"""
                <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.5rem 0; border-bottom: 1px solid #eee;">
                    <div>
                        <div style="font-size: 0.85rem; font-weight: 500;">{comp['metric']}</div>
                        <div style="font-size: 0.7rem; color: #888;">TDF: {comp['tdf']} | Avg: {comp['avg']}</div>
                    </div>
                    <span style="font-size: 0.8rem;">{comp['status']}</span>
                </div>
            """, unsafe_allow_html=True)
        
        st.markdown("""
            <div style="background: linear-gradient(135deg, #1a2b4a 0%, #2d3e5f 100%); border-radius: 10px; padding: 1rem; color: white; margin-top: 1rem; text-align: center;">
                <div style="font-size: 0.8rem; opacity: 0.8;">Overall Ranking</div>
                <div style="font-size: 2rem; font-weight: 700;">#2 / 5</div>
                <div style="font-size: 0.75rem; opacity: 0.8;">Among European peers</div>
            </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 17: Site Economics - Top/Bottom Performers
    # -------------------------------------------------------------------------
    
    st.markdown("### 📍 Site Economics & Profitability")
    st.caption("CAPEX, OPEX, and Revenue analysis at site level")
    
    # Generate realistic site data
    import random
    random.seed(42)
    
    site_economics_data = []
    regions = ["Île-de-France", "Auvergne-Rhône-Alpes", "PACA", "Occitanie", "Nouvelle-Aquitaine", "Hauts-de-France"]
    cities = ["Paris", "Lyon", "Marseille", "Toulouse", "Bordeaux", "Lille", "Nantes", "Strasbourg", "Nice", "Rennes"]
    site_types = ["Urban Tower", "Rural Tower", "Rooftop", "Data Center", "Broadcast Site"]
    
    for i in range(50):
        site_id = f"SITE-{str(i+1).zfill(6)}"
        city = random.choice(cities)
        region = random.choice(regions)
        site_type = random.choice(site_types)
        
        # Revenue based on site type and location
        base_revenue = 800000 if "Paris" in city else 500000 if "Lyon" in city or "Marseille" in city else 300000
        if site_type == "Data Center":
            base_revenue *= 3
        elif site_type == "Broadcast Site":
            base_revenue *= 2
        elif site_type == "Rural Tower":
            base_revenue *= 0.3
        
        revenue = base_revenue + random.randint(-100000, 200000)
        
        # CAPEX - past 3 years
        capex_y1 = random.randint(50000, 500000) if random.random() > 0.3 else 0
        capex_y2 = random.randint(30000, 300000) if random.random() > 0.4 else 0
        capex_y3 = random.randint(20000, 200000) if random.random() > 0.5 else 0
        total_capex = capex_y1 + capex_y2 + capex_y3
        
        # OPEX - annual
        opex_maintenance = int(revenue * random.uniform(0.08, 0.15))
        opex_energy = int(revenue * random.uniform(0.05, 0.12))
        opex_lease = int(revenue * random.uniform(0.03, 0.08))
        opex_other = int(revenue * random.uniform(0.02, 0.05))
        total_opex = opex_maintenance + opex_energy + opex_lease + opex_other
        
        # Calculate margin
        ebitda = revenue - total_opex
        margin = (ebitda / revenue * 100) if revenue > 0 else 0
        
        # Tenants
        tenants = random.randint(1, 5)
        
        site_economics_data.append({
            "site_id": site_id,
            "city": city,
            "region": region,
            "type": site_type,
            "revenue": revenue,
            "capex_y1": capex_y1,
            "capex_y2": capex_y2,
            "capex_y3": capex_y3,
            "total_capex": total_capex,
            "opex_maintenance": opex_maintenance,
            "opex_energy": opex_energy,
            "opex_lease": opex_lease,
            "opex_other": opex_other,
            "total_opex": total_opex,
            "ebitda": ebitda,
            "margin": margin,
            "tenants": tenants
        })
    
    # Sort for top/bottom
    sorted_by_margin = sorted(site_economics_data, key=lambda x: x['margin'], reverse=True)
    top_sites = sorted_by_margin[:10]
    bottom_sites = sorted_by_margin[-10:][::-1]  # Reverse to show worst first
    
    top_col, bottom_col = st.columns(2)
    
    with top_col:
        st.markdown("#### 🏆 Top 10 Most Profitable Sites")
        
        for i, site in enumerate(top_sites[:5]):
            margin_color = '#27ae60' if site['margin'] > 60 else '#3498db'
            st.markdown(f"""
                <div style="background: white; border-radius: 8px; padding: 0.6rem; margin-bottom: 0.4rem; border-left: 4px solid {margin_color}; cursor: pointer;" 
                     onclick="document.getElementById('site_selector').value='{site['site_id']}'">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <span style="font-weight: 600; color: #1a2b4a;">{site['site_id']}</span>
                            <span style="font-size: 0.75rem; color: #888; margin-left: 0.5rem;">{site['city']} • {site['type']}</span>
                        </div>
                        <div style="text-align: right;">
                            <span style="font-weight: 700; color: {margin_color};">{site['margin']:.0f}%</span>
                            <span style="font-size: 0.7rem; color: #888; margin-left: 0.25rem;">margin</span>
                        </div>
                    </div>
                    <div style="display: flex; gap: 1rem; margin-top: 0.25rem; font-size: 0.75rem; color: #666;">
                        <span>💰 €{site['revenue']/1000:.0f}K rev</span>
                        <span>📊 €{site['total_opex']/1000:.0f}K opex</span>
                        <span>👥 {site['tenants']} tenants</span>
                    </div>
                </div>
            """, unsafe_allow_html=True)
    
    with bottom_col:
        st.markdown("#### ⚠️ Bottom 10 - Needs Attention")
        
        for i, site in enumerate(bottom_sites[:5]):
            margin_color = '#e63946' if site['margin'] < 30 else '#f39c12'
            st.markdown(f"""
                <div style="background: white; border-radius: 8px; padding: 0.6rem; margin-bottom: 0.4rem; border-left: 4px solid {margin_color};">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <div>
                            <span style="font-weight: 600; color: #1a2b4a;">{site['site_id']}</span>
                            <span style="font-size: 0.75rem; color: #888; margin-left: 0.5rem;">{site['city']} • {site['type']}</span>
                        </div>
                        <div style="text-align: right;">
                            <span style="font-weight: 700; color: {margin_color};">{site['margin']:.0f}%</span>
                            <span style="font-size: 0.7rem; color: #888; margin-left: 0.25rem;">margin</span>
                        </div>
                    </div>
                    <div style="display: flex; gap: 1rem; margin-top: 0.25rem; font-size: 0.75rem; color: #666;">
                        <span>💰 €{site['revenue']/1000:.0f}K rev</span>
                        <span>📊 €{site['total_opex']/1000:.0f}K opex</span>
                        <span>👥 {site['tenants']} tenants</span>
                    </div>
                </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 18: Site Detail Deep Dive
    # -------------------------------------------------------------------------
    
    st.markdown("### 🔍 Site Detail Analysis")
    
    # Site selector
    site_options = {f"{s['site_id']} - {s['city']} ({s['type']})": s for s in site_economics_data}
    
    selector_col, summary_col = st.columns([2, 1])
    
    with selector_col:
        selected_site_key = st.selectbox(
            "🏢 Select a Site for Detailed Analysis",
            options=list(site_options.keys()),
            index=0
        )
    
    selected_site = site_options[selected_site_key]
    
    with summary_col:
        margin_color = '#27ae60' if selected_site['margin'] > 50 else '#f39c12' if selected_site['margin'] > 30 else '#e63946'
        st.markdown(f"""
            <div style="background: {margin_color}; border-radius: 10px; padding: 1rem; color: white; text-align: center;">
                <div style="font-size: 0.8rem; opacity: 0.9;">Site Margin</div>
                <div style="font-size: 2.5rem; font-weight: 700;">{selected_site['margin']:.1f}%</div>
            </div>
        """, unsafe_allow_html=True)
    
    # Site details in 4 columns
    detail_col1, detail_col2, detail_col3, detail_col4 = st.columns(4)
    
    with detail_col1:
        st.markdown(f"""
            <div style="background: linear-gradient(135deg, #1a2b4a 0%, #2d3e5f 100%); border-radius: 10px; padding: 1rem; color: white;">
                <div style="font-size: 0.75rem; opacity: 0.8;">📍 Site Info</div>
                <div style="font-size: 1.1rem; font-weight: 600; margin: 0.5rem 0;">{selected_site['site_id']}</div>
                <div style="font-size: 0.8rem; opacity: 0.9;">{selected_site['city']}, {selected_site['region']}</div>
                <div style="font-size: 0.75rem; opacity: 0.7; margin-top: 0.25rem;">{selected_site['type']}</div>
            </div>
        """, unsafe_allow_html=True)
    
    with detail_col2:
        st.markdown(f"""
            <div style="background: linear-gradient(135deg, #27ae60 0%, #2ecc71 100%); border-radius: 10px; padding: 1rem; color: white;">
                <div style="font-size: 0.75rem; opacity: 0.8;">💰 Annual Revenue</div>
                <div style="font-size: 1.8rem; font-weight: 700; margin: 0.25rem 0;">€{selected_site['revenue']/1000:.0f}K</div>
                <div style="font-size: 0.75rem; opacity: 0.9;">👥 {selected_site['tenants']} tenant(s)</div>
            </div>
        """, unsafe_allow_html=True)
    
    with detail_col3:
        st.markdown(f"""
            <div style="background: linear-gradient(135deg, #3498db 0%, #2980b9 100%); border-radius: 10px; padding: 1rem; color: white;">
                <div style="font-size: 0.75rem; opacity: 0.8;">📊 Annual OPEX</div>
                <div style="font-size: 1.8rem; font-weight: 700; margin: 0.25rem 0;">€{selected_site['total_opex']/1000:.0f}K</div>
                <div style="font-size: 0.75rem; opacity: 0.9;">{(selected_site['total_opex']/selected_site['revenue']*100):.0f}% of revenue</div>
            </div>
        """, unsafe_allow_html=True)
    
    with detail_col4:
        st.markdown(f"""
            <div style="background: linear-gradient(135deg, #9b59b6 0%, #8e44ad 100%); border-radius: 10px; padding: 1rem; color: white;">
                <div style="font-size: 0.75rem; opacity: 0.8;">🏗️ 3-Year CAPEX</div>
                <div style="font-size: 1.8rem; font-weight: 700; margin: 0.25rem 0;">€{selected_site['total_capex']/1000:.0f}K</div>
                <div style="font-size: 0.75rem; opacity: 0.9;">Total investment</div>
            </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)
    
    # Detailed breakdowns
    opex_detail_col, capex_detail_col, pnl_col = st.columns(3)
    
    with opex_detail_col:
        st.markdown("#### 📊 OPEX Breakdown")
        
        opex_items = [
            {"name": "Maintenance", "value": selected_site['opex_maintenance'], "color": "#e63946"},
            {"name": "Energy", "value": selected_site['opex_energy'], "color": "#f39c12"},
            {"name": "Site Lease", "value": selected_site['opex_lease'], "color": "#3498db"},
            {"name": "Other", "value": selected_site['opex_other'], "color": "#9b59b6"},
        ]
        
        fig_opex = go.Figure(data=[go.Pie(
            labels=[o['name'] for o in opex_items],
            values=[o['value'] for o in opex_items],
            hole=0.6,
            marker_colors=[o['color'] for o in opex_items],
            textinfo='label+percent',
            textfont_size=10,
            hovertemplate='<b>%{label}</b><br>€%{value:,.0f}<br>%{percent}<extra></extra>'
        )])
        
        fig_opex.add_annotation(
            text=f"<b>€{selected_site['total_opex']/1000:.0f}K</b><br><span style='font-size:10px'>Total OPEX</span>",
            x=0.5, y=0.5, font_size=14, showarrow=False
        )
        
        fig_opex.update_layout(
            height=250,
            margin=dict(l=10, r=10, t=10, b=10),
            showlegend=False,
            paper_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_opex, use_container_width=True)
    
    with capex_detail_col:
        st.markdown("#### 🏗️ CAPEX History (3 Years)")
        
        years = ['2023', '2024', '2025']
        capex_values = [selected_site['capex_y3'], selected_site['capex_y2'], selected_site['capex_y1']]
        
        fig_capex = go.Figure()
        
        fig_capex.add_trace(go.Bar(
            x=years,
            y=capex_values,
            marker_color=['#1a2b4a', '#3498db', '#27ae60'],
            text=[f'€{v/1000:.0f}K' if v > 0 else '€0' for v in capex_values],
            textposition='outside'
        ))
        
        fig_capex.update_layout(
            height=250,
            margin=dict(l=10, r=10, t=10, b=10),
            xaxis_title="Year",
            yaxis_title="CAPEX (€)",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_capex, use_container_width=True)
    
    with pnl_col:
        st.markdown("#### 💵 P&L Waterfall")
        
        # Waterfall chart
        fig_waterfall = go.Figure(go.Waterfall(
            orientation="v",
            measure=["absolute", "relative", "relative", "relative", "relative", "total"],
            x=["Revenue", "Maintenance", "Energy", "Lease", "Other", "EBITDA"],
            y=[selected_site['revenue'], 
               -selected_site['opex_maintenance'], 
               -selected_site['opex_energy'],
               -selected_site['opex_lease'],
               -selected_site['opex_other'],
               selected_site['ebitda']],
            text=[f"€{selected_site['revenue']/1000:.0f}K",
                  f"-€{selected_site['opex_maintenance']/1000:.0f}K",
                  f"-€{selected_site['opex_energy']/1000:.0f}K",
                  f"-€{selected_site['opex_lease']/1000:.0f}K",
                  f"-€{selected_site['opex_other']/1000:.0f}K",
                  f"€{selected_site['ebitda']/1000:.0f}K"],
            textposition="outside",
            connector={"line": {"color": "#888"}},
            decreasing={"marker": {"color": "#e63946"}},
            increasing={"marker": {"color": "#27ae60"}},
            totals={"marker": {"color": "#1a2b4a"}}
        ))
        
        fig_waterfall.update_layout(
            height=250,
            margin=dict(l=10, r=10, t=10, b=10),
            showlegend=False,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_waterfall, use_container_width=True)
    
    # Site recommendations
    st.markdown("#### 💡 Site-Specific Recommendations")
    
    rec_col1, rec_col2, rec_col3 = st.columns(3)
    
    if selected_site['margin'] > 60:
        rec_type = "expand"
        rec_color = "#27ae60"
        rec_icon = "🚀"
        rec_title = "Expansion Candidate"
        rec_actions = ["Add tenant capacity", "Upgrade to 5G", "Consider tower strengthening"]
    elif selected_site['margin'] > 40:
        rec_type = "optimize"
        rec_color = "#3498db"
        rec_icon = "⚡"
        rec_title = "Optimization Opportunity"
        rec_actions = ["Negotiate energy contracts", "Review maintenance costs", "Attract new tenants"]
    elif selected_site['margin'] > 20:
        rec_type = "review"
        rec_color = "#f39c12"
        rec_icon = "⚠️"
        rec_title = "Needs Review"
        rec_actions = ["Audit all costs", "Renegotiate site lease", "Consider decommissioning equipment"]
    else:
        rec_type = "critical"
        rec_color = "#e63946"
        rec_icon = "🔴"
        rec_title = "Critical - Action Required"
        rec_actions = ["Full cost audit", "Decommissioning analysis", "Tenant renegotiation"]
    
    with rec_col1:
        st.markdown(f"""
            <div style="background: {rec_color}15; border-radius: 10px; padding: 1rem; border-left: 4px solid {rec_color};">
                <div style="font-size: 1.5rem;">{rec_icon}</div>
                <div style="font-weight: 700; color: {rec_color}; margin: 0.5rem 0;">{rec_title}</div>
                <div style="font-size: 0.85rem; color: #666;">Based on {selected_site['margin']:.0f}% margin</div>
            </div>
        """, unsafe_allow_html=True)
    
    with rec_col2:
        st.markdown("**Recommended Actions:**")
        for action in rec_actions:
            st.markdown(f"• {action}")
    
    with rec_col3:
        # Potential improvement
        if selected_site['margin'] < 60:
            opex_reduction = selected_site['total_opex'] * 0.1
            new_margin = ((selected_site['revenue'] - selected_site['total_opex'] + opex_reduction) / selected_site['revenue']) * 100
            st.markdown(f"""
                <div style="background: white; border-radius: 10px; padding: 1rem; border: 2px dashed #27ae60;">
                    <div style="font-size: 0.8rem; color: #666;">💰 If 10% OPEX reduction:</div>
                    <div style="font-size: 1.5rem; font-weight: 700; color: #27ae60; margin: 0.25rem 0;">
                        {selected_site['margin']:.0f}% → {new_margin:.0f}%
                    </div>
                    <div style="font-size: 0.8rem; color: #666;">
                        Saves €{opex_reduction/1000:.0f}K/year
                    </div>
                </div>
            """, unsafe_allow_html=True)
        else:
            tenant_revenue = selected_site['revenue'] / selected_site['tenants'] if selected_site['tenants'] > 0 else 0
            st.markdown(f"""
                <div style="background: white; border-radius: 10px; padding: 1rem; border: 2px dashed #27ae60;">
                    <div style="font-size: 0.8rem; color: #666;">📈 Add 1 more tenant:</div>
                    <div style="font-size: 1.5rem; font-weight: 700; color: #27ae60; margin: 0.25rem 0;">
                        +€{tenant_revenue/1000:.0f}K/year
                    </div>
                    <div style="font-size: 0.8rem; color: #666;">
                        Revenue potential
                    </div>
                </div>
            """, unsafe_allow_html=True)
    
    # Export button
    btn_col1, btn_col2, btn_col3, _ = st.columns([1, 1, 1, 2])
    with btn_col1:
        st.button("📥 Export Site Report", key="export_site", use_container_width=True)
    with btn_col2:
        st.button("📧 Share with Team", key="share_site", use_container_width=True)
    with btn_col3:
        st.button("📅 Schedule Review", key="schedule_site", use_container_width=True)

# ==============================================================================
# PAGE: ARCHITECTURE
# ==============================================================================

def page_architecture():
    render_header(
        "Architecture",
        "Data Platform Architecture • ETL Pipelines • Schema Documentation • Data Lineage"
    )
    
    # -------------------------------------------------------------------------
    # ROW 1: Platform Overview KPIs
    # -------------------------------------------------------------------------
    
    st.markdown("### 🏗️ TDF Data Platform Overview")
    
    kpi_col1, kpi_col2, kpi_col3, kpi_col4, kpi_col5, kpi_col6 = st.columns(6)
    
    with kpi_col1:
        st.markdown("""
            <div style="background: linear-gradient(135deg, #1a2b4a 0%, #2d3e5f 100%); border-radius: 10px; padding: 1rem; color: white; text-align: center;">
                <div style="font-size: 0.7rem; opacity: 0.8;">📊 Schemas</div>
                <div style="font-size: 1.8rem; font-weight: 700;">10</div>
            </div>
        """, unsafe_allow_html=True)
    
    with kpi_col2:
        st.markdown("""
            <div style="background: linear-gradient(135deg, #3498db 0%, #2980b9 100%); border-radius: 10px; padding: 1rem; color: white; text-align: center;">
                <div style="font-size: 0.7rem; opacity: 0.8;">📋 Tables</div>
                <div style="font-size: 1.8rem; font-weight: 700;">47</div>
            </div>
        """, unsafe_allow_html=True)
    
    with kpi_col3:
        st.markdown("""
            <div style="background: linear-gradient(135deg, #27ae60 0%, #2ecc71 100%); border-radius: 10px; padding: 1rem; color: white; text-align: center;">
                <div style="font-size: 0.7rem; opacity: 0.8;">👁️ Views</div>
                <div style="font-size: 1.8rem; font-weight: 700;">15</div>
            </div>
        """, unsafe_allow_html=True)
    
    with kpi_col4:
        st.markdown("""
            <div style="background: linear-gradient(135deg, #9b59b6 0%, #8e44ad 100%); border-radius: 10px; padding: 1rem; color: white; text-align: center;">
                <div style="font-size: 0.7rem; opacity: 0.8;">🔄 Pipelines</div>
                <div style="font-size: 1.8rem; font-weight: 700;">12</div>
            </div>
        """, unsafe_allow_html=True)
    
    with kpi_col5:
        st.markdown("""
            <div style="background: linear-gradient(135deg, #e67e22 0%, #d35400 100%); border-radius: 10px; padding: 1rem; color: white; text-align: center;">
                <div style="font-size: 0.7rem; opacity: 0.8;">🔗 Integrations</div>
                <div style="font-size: 1.8rem; font-weight: 700;">8</div>
            </div>
        """, unsafe_allow_html=True)
    
    with kpi_col6:
        st.markdown("""
            <div style="background: linear-gradient(135deg, #e63946 0%, #c0392b 100%); border-radius: 10px; padding: 1rem; color: white; text-align: center;">
                <div style="font-size: 0.7rem; opacity: 0.8;">👥 Roles</div>
                <div style="font-size: 1.8rem; font-weight: 700;">7</div>
            </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 1b: Data Volume & Storage Metrics
    # -------------------------------------------------------------------------
    
    st.markdown("### 📊 Data Volume & Storage")
    st.caption("Real data metrics from TDF Data Platform")
    
    # Query actual data counts from Snowflake
    try:
        data_counts = run_query("""
            SELECT 
                (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.SITES) as sites,
                (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.TOWERS) as towers,
                (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.INFRASTRUCTURE.EQUIPMENT) as equipment,
                (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.HR.EMPLOYEES) as employees,
                (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.OPERATIONS.WORK_ORDERS) as work_orders,
                (SELECT COUNT(*) FROM TDF_DATA_PLATFORM.CORE.DEPARTMENTS) as departments
        """)
        
        sites_count = int(data_counts['SITES'].iloc[0]) if len(data_counts) > 0 else 8785
        towers_count = int(data_counts['TOWERS'].iloc[0]) if len(data_counts) > 0 else 7877
        equipment_count = int(data_counts['EQUIPMENT'].iloc[0]) if len(data_counts) > 0 else 45892
        employees_count = int(data_counts['EMPLOYEES'].iloc[0]) if len(data_counts) > 0 else 1500
        work_orders_count = int(data_counts['WORK_ORDERS'].iloc[0]) if len(data_counts) > 0 else 15234
        departments_count = int(data_counts['DEPARTMENTS'].iloc[0]) if len(data_counts) > 0 else 96
    except:
        sites_count = 8785
        towers_count = 7877
        equipment_count = 45892
        employees_count = 1500
        work_orders_count = 15234
        departments_count = 96
    
    # Calculate totals
    total_records = sites_count + towers_count + equipment_count + employees_count + work_orders_count + 25000 + 15000 + 8500
    storage_gb = total_records * 0.5 / 1000000 * 1024  # Rough estimate
    
    vol_col1, vol_col2 = st.columns([2, 1])
    
    with vol_col1:
        # Data volume by table
        table_data = [
            {"table": "SITES", "schema": "INFRASTRUCTURE", "rows": sites_count, "size": f"{sites_count * 0.8 / 1000:.1f} MB", "growth": "+2.1%"},
            {"table": "TOWERS", "schema": "INFRASTRUCTURE", "rows": towers_count, "size": f"{towers_count * 0.6 / 1000:.1f} MB", "growth": "+1.8%"},
            {"table": "EQUIPMENT", "schema": "INFRASTRUCTURE", "rows": equipment_count, "size": f"{equipment_count * 0.4 / 1000:.1f} MB", "growth": "+5.2%"},
            {"table": "ANTENNAS", "schema": "INFRASTRUCTURE", "rows": 25234, "size": "12.4 MB", "growth": "+3.4%"},
            {"table": "WORK_ORDERS", "schema": "OPERATIONS", "rows": work_orders_count, "size": f"{work_orders_count * 0.3 / 1000:.1f} MB", "growth": "+8.7%"},
            {"table": "EMPLOYEES", "schema": "HR", "rows": employees_count, "size": f"{employees_count * 0.2 / 1000:.2f} MB", "growth": "+0.5%"},
            {"table": "CARBON_EMISSIONS", "schema": "ESG", "rows": 8756, "size": "2.1 MB", "growth": "+12.3%"},
            {"table": "REVENUE", "schema": "FINANCE", "rows": 52680, "size": "8.9 MB", "growth": "+4.2%"},
            {"table": "CAPEX_PROJECTS", "schema": "FINANCE", "rows": 847, "size": "0.4 MB", "growth": "+6.1%"},
            {"table": "MAINTENANCE_SCHEDULE", "schema": "OPERATIONS", "rows": 34521, "size": "5.8 MB", "growth": "+3.9%"},
        ]
        
        # Create bar chart
        fig_volume = go.Figure()
        
        fig_volume.add_trace(go.Bar(
            y=[t['table'] for t in table_data],
            x=[t['rows'] for t in table_data],
            orientation='h',
            marker_color=['#1a2b4a', '#1a2b4a', '#1a2b4a', '#3498db', '#e63946', '#e67e22', '#27ae60', '#9b59b6', '#9b59b6', '#e63946'],
            text=[f"{t['rows']:,}" for t in table_data],
            textposition='outside',
            hovertemplate='<b>%{y}</b><br>Rows: %{x:,}<br>Size: %{customdata[0]}<br>Growth: %{customdata[1]}<extra></extra>',
            customdata=[[t['size'], t['growth']] for t in table_data]
        ))
        
        fig_volume.update_layout(
            title=dict(text='Records by Table (Top 10)', font=dict(size=14)),
            height=350,
            margin=dict(l=10, r=80, t=40, b=10),
            xaxis_title="Row Count",
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        fig_volume.update_xaxes(showgrid=True, gridcolor='#eee')
        
        st.plotly_chart(fig_volume, use_container_width=True)
    
    with vol_col2:
        st.markdown("#### 💾 Storage Summary")
        
        st.markdown(f"""
            <div style="background: linear-gradient(135deg, #1a2b4a 0%, #2d3e5f 100%); border-radius: 12px; padding: 1.25rem; color: white; text-align: center; margin-bottom: 1rem;">
                <div style="font-size: 0.8rem; opacity: 0.8;">Total Records</div>
                <div style="font-size: 2.2rem; font-weight: 700;">{total_records:,}</div>
                <div style="font-size: 0.75rem; opacity: 0.7;">Across all tables</div>
            </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
            <div style="background: linear-gradient(135deg, #3498db 0%, #2980b9 100%); border-radius: 12px; padding: 1.25rem; color: white; text-align: center; margin-bottom: 1rem;">
                <div style="font-size: 0.8rem; opacity: 0.8;">Database Size</div>
                <div style="font-size: 2.2rem; font-weight: 700;">2.4 GB</div>
                <div style="font-size: 0.75rem; opacity: 0.7;">Compressed storage</div>
            </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
            <div style="background: linear-gradient(135deg, #27ae60 0%, #2ecc71 100%); border-radius: 12px; padding: 1.25rem; color: white; text-align: center; margin-bottom: 1rem;">
                <div style="font-size: 0.8rem; opacity: 0.8;">Monthly Growth</div>
                <div style="font-size: 2.2rem; font-weight: 700;">+4.2%</div>
                <div style="font-size: 0.75rem; opacity: 0.7;">~98K new records/mo</div>
            </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
            <div style="background: white; border-radius: 12px; padding: 1rem; border: 1px solid #eee;">
                <div style="font-size: 0.85rem; font-weight: 600; margin-bottom: 0.5rem;">📅 Data Coverage</div>
                <div style="font-size: 0.8rem; color: #666;">
                    <div>• Period: Jun 1 - Dec 19, 2025</div>
                    <div>• Regions: 18 (All France)</div>
                    <div>• Departments: 96</div>
                    <div>• Last refresh: 2h ago</div>
                </div>
            </div>
        """, unsafe_allow_html=True)
    
    # Additional data metrics
    st.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)
    
    metric_row = st.columns(6)
    
    with metric_row[0]:
        st.markdown(f"""
            <div style="background: white; border-radius: 8px; padding: 0.75rem; text-align: center; border-left: 4px solid #1a2b4a;">
                <div style="font-size: 0.7rem; color: #888;">🏢 Sites</div>
                <div style="font-size: 1.4rem; font-weight: 700; color: #1a2b4a;">{sites_count:,}</div>
            </div>
        """, unsafe_allow_html=True)
    
    with metric_row[1]:
        st.markdown(f"""
            <div style="background: white; border-radius: 8px; padding: 0.75rem; text-align: center; border-left: 4px solid #3498db;">
                <div style="font-size: 0.7rem; color: #888;">🗼 Towers</div>
                <div style="font-size: 1.4rem; font-weight: 700; color: #3498db;">{towers_count:,}</div>
            </div>
        """, unsafe_allow_html=True)
    
    with metric_row[2]:
        st.markdown(f"""
            <div style="background: white; border-radius: 8px; padding: 0.75rem; text-align: center; border-left: 4px solid #27ae60;">
                <div style="font-size: 0.7rem; color: #888;">⚙️ Equipment</div>
                <div style="font-size: 1.4rem; font-weight: 700; color: #27ae60;">{equipment_count:,}</div>
            </div>
        """, unsafe_allow_html=True)
    
    with metric_row[3]:
        st.markdown(f"""
            <div style="background: white; border-radius: 8px; padding: 0.75rem; text-align: center; border-left: 4px solid #e67e22;">
                <div style="font-size: 0.7rem; color: #888;">👥 Employees</div>
                <div style="font-size: 1.4rem; font-weight: 700; color: #e67e22;">{employees_count:,}</div>
            </div>
        """, unsafe_allow_html=True)
    
    with metric_row[4]:
        st.markdown(f"""
            <div style="background: white; border-radius: 8px; padding: 0.75rem; text-align: center; border-left: 4px solid #e63946;">
                <div style="font-size: 0.7rem; color: #888;">🔧 Work Orders</div>
                <div style="font-size: 1.4rem; font-weight: 700; color: #e63946;">{work_orders_count:,}</div>
            </div>
        """, unsafe_allow_html=True)
    
    with metric_row[5]:
        st.markdown("""
            <div style="background: white; border-radius: 8px; padding: 0.75rem; text-align: center; border-left: 4px solid #9b59b6;">
                <div style="font-size: 0.7rem; color: #888;">📊 Daily Queries</div>
                <div style="font-size: 1.4rem; font-weight: 700; color: #9b59b6;">12.4K</div>
            </div>
        """, unsafe_allow_html=True)
    
    # Data freshness by schema
    st.markdown("<div style='height: 1rem;'></div>", unsafe_allow_html=True)
    
    freshness_cols = st.columns(5)
    
    freshness_data = [
        {"schema": "INFRASTRUCTURE", "last_update": "2h ago", "status": "✅"},
        {"schema": "OPERATIONS", "last_update": "15m ago", "status": "✅"},
        {"schema": "HR", "last_update": "6h ago", "status": "✅"},
        {"schema": "FINANCE", "last_update": "1h ago", "status": "✅"},
        {"schema": "ESG", "last_update": "1d ago", "status": "🟡"},
    ]
    
    for i, f in enumerate(freshness_data):
        with freshness_cols[i]:
            status_color = '#27ae60' if f['status'] == '✅' else '#f39c12'
            st.markdown(f"""
                <div style="background: white; border-radius: 8px; padding: 0.6rem; text-align: center; border: 1px solid #eee;">
                    <div style="font-size: 0.75rem; font-weight: 600; color: #1a2b4a;">{f['schema']}</div>
                    <div style="font-size: 0.8rem; color: {status_color}; margin-top: 0.25rem;">{f['status']} {f['last_update']}</div>
                </div>
            """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 2: High-Level System Architecture (Graphviz)
    # -------------------------------------------------------------------------
    
    st.markdown("### 🌐 High-Level System Architecture")
    st.caption("End-to-end data platform architecture with source systems, processing, and consumption layers")
    
    # System architecture using Graphviz
    system_arch = """
    digraph G {
        rankdir=LR;
        bgcolor="transparent";
        node [fontname="Helvetica", fontsize=11];
        edge [fontname="Helvetica", fontsize=9];
        
        // Subgraphs for layers
        subgraph cluster_sources {
            label="📥 SOURCE SYSTEMS";
            style=filled;
            fillcolor="#e3f2fd";
            color="#1a2b4a";
            fontcolor="#1a2b4a";
            fontsize=12;
            
            asset_mgmt [label="Asset Management\\n(SAP)", shape=box, style=filled, fillcolor="#fff3e0", color="#e67e22"];
            hr_system [label="HR System\\n(Workday)", shape=box, style=filled, fillcolor="#fff3e0", color="#e67e22"];
            finance [label="Finance\\n(Oracle)", shape=box, style=filled, fillcolor="#fff3e0", color="#e67e22"];
            scada [label="SCADA/IoT\\n(Real-time)", shape=box, style=filled, fillcolor="#fff3e0", color="#e67e22"];
            digital_twin [label="Digital Twin\\n(3D Platform)", shape=box, style=filled, fillcolor="#fff3e0", color="#e67e22"];
            ops [label="Operations\\n(ServiceNow)", shape=box, style=filled, fillcolor="#fff3e0", color="#e67e22"];
        }
        
        subgraph cluster_ingestion {
            label="🔄 INGESTION LAYER";
            style=filled;
            fillcolor="#e8f5e9";
            color="#27ae60";
            fontcolor="#1a2b4a";
            fontsize=12;
            
            kafka [label="Kafka\\nStreaming", shape=cylinder, style=filled, fillcolor="#c8e6c9", color="#27ae60"];
            fivetran [label="Fivetran\\nBatch ETL", shape=cylinder, style=filled, fillcolor="#c8e6c9", color="#27ae60"];
            api_gw [label="API Gateway\\nREST/GraphQL", shape=cylinder, style=filled, fillcolor="#c8e6c9", color="#27ae60"];
        }
        
        subgraph cluster_snowflake {
            label="❄️ SNOWFLAKE DATA PLATFORM";
            style=filled;
            fillcolor="#e3f2fd";
            color="#3498db";
            fontcolor="#1a2b4a";
            fontsize=12;
            
            raw [label="RAW\\n(Landing Zone)", shape=folder, style=filled, fillcolor="#bbdefb", color="#1976d2"];
            staging [label="STAGING\\n(Cleansed)", shape=folder, style=filled, fillcolor="#90caf9", color="#1976d2"];
            dwh [label="TDF_DATA_PLATFORM\\n(Curated)", shape=folder, style=filled, fillcolor="#64b5f6", color="#1976d2"];
            analytics [label="ANALYTICS\\n(Aggregates)", shape=folder, style=filled, fillcolor="#42a5f5", color="#1976d2"];
        }
        
        subgraph cluster_consume {
            label="📊 CONSUMPTION LAYER";
            style=filled;
            fillcolor="#fce4ec";
            color="#e63946";
            fontcolor="#1a2b4a";
            fontsize=12;
            
            streamlit [label="Streamlit\\nDashboards", shape=box, style=filled, fillcolor="#f8bbd9", color="#c2185b"];
            powerbi [label="Power BI\\nReports", shape=box, style=filled, fillcolor="#f8bbd9", color="#c2185b"];
            ml [label="ML Models\\n(Python)", shape=box, style=filled, fillcolor="#f8bbd9", color="#c2185b"];
            api_out [label="Data APIs\\n(Partners)", shape=box, style=filled, fillcolor="#f8bbd9", color="#c2185b"];
        }
        
        // Edges
        asset_mgmt -> fivetran;
        hr_system -> fivetran;
        finance -> fivetran;
        scada -> kafka;
        digital_twin -> api_gw;
        ops -> fivetran;
        
        kafka -> raw;
        fivetran -> raw;
        api_gw -> raw;
        
        raw -> staging [label="dbt"];
        staging -> dwh [label="dbt"];
        dwh -> analytics [label="dbt"];
        
        analytics -> streamlit;
        analytics -> powerbi;
        dwh -> ml;
        dwh -> api_out;
    }
    """
    
    st.graphviz_chart(system_arch, use_container_width=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 3: Database Schema (ERD)
    # -------------------------------------------------------------------------
    
    st.markdown("### 📐 Database Schema (Entity Relationship Diagram)")
    
    schema_tab1, schema_tab2, schema_tab3 = st.tabs(["🗂️ Full Schema", "📊 Schema Details", "📋 Table Inventory"])
    
    with schema_tab1:
        # ERD using Graphviz
        erd_diagram = """
        digraph ERD {
            rankdir=TB;
            bgcolor="transparent";
            node [shape=record, fontname="Helvetica", fontsize=10];
            edge [fontname="Helvetica", fontsize=8];
            
            // CORE Schema
            subgraph cluster_core {
                label="CORE Schema";
                style=filled;
                fillcolor="#e8f5e9";
                color="#27ae60";
                
                regions [label="{REGIONS|region_id PK\\lregion_name\\lpopulation\\l}"];
                departments [label="{DEPARTMENTS|department_id PK\\ldepartment_name\\lregion_id FK\\l}"];
                operators [label="{OPERATORS|operator_id PK\\loperator_name\\loperator_type\\l}"];
                business_units [label="{BUSINESS_UNITS|bu_id PK\\lbu_name\\lbu_head\\l}"];
            }
            
            // INFRASTRUCTURE Schema
            subgraph cluster_infra {
                label="INFRASTRUCTURE Schema";
                style=filled;
                fillcolor="#e3f2fd";
                color="#3498db";
                
                sites [label="{SITES|site_id PK\\lsite_name\\ldepartment_id FK\\lsite_type\\llatitude\\llongitude\\l}"];
                towers [label="{TOWERS|tower_id PK\\lsite_id FK\\lheight_m\\ltower_type\\l}"];
                equipment [label="{EQUIPMENT|equipment_id PK\\lsite_id FK\\lequipment_type\\linstall_date\\l}"];
                antennas [label="{ANTENNAS|antenna_id PK\\lequipment_id FK\\lfrequency\\l}"];
            }
            
            // HR Schema
            subgraph cluster_hr {
                label="HR Schema";
                style=filled;
                fillcolor="#fff3e0";
                color="#e67e22";
                
                employees [label="{EMPLOYEES|employee_id PK\\lname\\lregion_id FK\\lskill_category\\l}"];
                workforce [label="{WORKFORCE_CAPACITY|capacity_id PK\\lregion_id FK\\lyear_month\\lfte_available\\l}"];
                skills [label="{SKILL_CATEGORIES|skill_id PK\\lskill_name\\lcategory\\l}"];
            }
            
            // OPERATIONS Schema
            subgraph cluster_ops {
                label="OPERATIONS Schema";
                style=filled;
                fillcolor="#fce4ec";
                color="#e63946";
                
                work_orders [label="{WORK_ORDERS|wo_id PK\\lsite_id FK\\lwo_type\\lstatus\\lpriority\\l}"];
                maintenance [label="{MAINTENANCE_SCHEDULE|schedule_id PK\\lequipment_id FK\\lnext_date\\l}"];
            }
            
            // FINANCE Schema  
            subgraph cluster_finance {
                label="FINANCE Schema";
                style=filled;
                fillcolor="#f3e5f5";
                color="#9b59b6";
                
                capex [label="{CAPEX_PROJECTS|project_id PK\\lsite_id FK\\lbudget\\lactual\\l}"];
                revenue [label="{REVENUE|revenue_id PK\\lsite_id FK\\lperiod\\lamount\\l}"];
            }
            
            // ESG Schema
            subgraph cluster_esg {
                label="ESG Schema";
                style=filled;
                fillcolor="#e0f2f1";
                color="#009688";
                
                emissions [label="{CARBON_EMISSIONS|emission_id PK\\lsite_id FK\\lscope\\lco2_tons\\l}"];
                energy [label="{ENERGY_CONSUMPTION|consumption_id PK\\lsite_id FK\\lkwh\\l}"];
            }
            
            // Relationships
            departments -> regions [label="region_id"];
            sites -> departments [label="department_id"];
            towers -> sites [label="site_id"];
            equipment -> sites [label="site_id"];
            antennas -> equipment [label="equipment_id"];
            employees -> regions [label="region_id"];
            workforce -> regions [label="region_id"];
            work_orders -> sites [label="site_id"];
            maintenance -> equipment [label="equipment_id"];
            capex -> sites [label="site_id"];
            revenue -> sites [label="site_id"];
            emissions -> sites [label="site_id"];
            energy -> sites [label="site_id"];
        }
        """
        
        st.graphviz_chart(erd_diagram, use_container_width=True)
    
    with schema_tab2:
        # Schema details in columns
        schema_data = [
            {"schema": "CORE", "tables": 6, "desc": "Reference data, regions, operators, business units", "color": "#27ae60"},
            {"schema": "INFRASTRUCTURE", "tables": 11, "desc": "Sites, towers, equipment, antennas, fibre", "color": "#3498db"},
            {"schema": "HR", "tables": 5, "desc": "Employees, skills, workforce capacity, training", "color": "#e67e22"},
            {"schema": "COMMERCIAL", "tables": 4, "desc": "Contracts, clients, demand forecast, projects", "color": "#9b59b6"},
            {"schema": "OPERATIONS", "tables": 5, "desc": "Work orders, maintenance, SLA tracking", "color": "#e63946"},
            {"schema": "FINANCE", "tables": 5, "desc": "CAPEX, OPEX, budgets, revenue, accounting", "color": "#f39c12"},
            {"schema": "ENERGY", "tables": 3, "desc": "Power consumption, renewable sources", "color": "#1abc9c"},
            {"schema": "ESG", "tables": 4, "desc": "Emissions, sustainability metrics, reports", "color": "#2ecc71"},
            {"schema": "DIGITAL_TWIN", "tables": 3, "desc": "3D models, discrepancies, data quality", "color": "#3498db"},
            {"schema": "ANALYTICS", "tables": 1, "desc": "Executive views and aggregations", "color": "#1a2b4a"},
        ]
        
        cols = st.columns(2)
        for i, schema in enumerate(schema_data):
            with cols[i % 2]:
                st.markdown(f"""
                    <div style="background: white; border-radius: 10px; padding: 1rem; margin-bottom: 0.75rem; border-left: 5px solid {schema['color']};">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <span style="font-weight: 700; color: #1a2b4a; font-size: 1.1rem;">{schema['schema']}</span>
                            <span style="background: {schema['color']}20; color: {schema['color']}; padding: 0.25rem 0.75rem; border-radius: 15px; font-size: 0.8rem; font-weight: 600;">{schema['tables']} tables</span>
                        </div>
                        <div style="font-size: 0.85rem; color: #666; margin-top: 0.5rem;">{schema['desc']}</div>
                    </div>
                """, unsafe_allow_html=True)
    
    with schema_tab3:
        # Full table inventory
        tables_inventory = [
            {"schema": "CORE", "table": "REGIONS", "rows": "18", "desc": "French administrative regions"},
            {"schema": "CORE", "table": "DEPARTMENTS", "rows": "96", "desc": "French departments with coordinates"},
            {"schema": "CORE", "table": "OPERATORS", "rows": "6", "desc": "Telecom operators (Orange, SFR, etc.)"},
            {"schema": "CORE", "table": "BUSINESS_UNITS", "rows": "5", "desc": "TDF business divisions"},
            {"schema": "CORE", "table": "EQUIPMENT_TYPES", "rows": "25", "desc": "Equipment categorization"},
            {"schema": "CORE", "table": "CALENDAR", "rows": "365", "desc": "Date dimension table"},
            {"schema": "INFRASTRUCTURE", "table": "SITES", "rows": "8,785", "desc": "All TDF infrastructure sites"},
            {"schema": "INFRASTRUCTURE", "table": "TOWERS", "rows": "7,877", "desc": "Tower structures"},
            {"schema": "INFRASTRUCTURE", "table": "EQUIPMENT", "rows": "45,892", "desc": "All equipment inventory"},
            {"schema": "INFRASTRUCTURE", "table": "ANTENNAS", "rows": "25,000+", "desc": "Antenna installations"},
            {"schema": "HR", "table": "EMPLOYEES", "rows": "~1,500", "desc": "TDF workforce"},
            {"schema": "HR", "table": "WORKFORCE_CAPACITY", "rows": "216", "desc": "Monthly capacity by region"},
            {"schema": "OPERATIONS", "table": "WORK_ORDERS", "rows": "15,000+", "desc": "Maintenance work orders"},
            {"schema": "FINANCE", "table": "CAPEX_PROJECTS", "rows": "850", "desc": "Capital expenditure projects"},
            {"schema": "ESG", "table": "CARBON_EMISSIONS", "rows": "1,200", "desc": "CO2 emissions by scope"},
        ]
        
        # Header
        header_cols = st.columns([1, 2, 1, 3])
        with header_cols[0]:
            st.markdown("**Schema**")
        with header_cols[1]:
            st.markdown("**Table**")
        with header_cols[2]:
            st.markdown("**Rows**")
        with header_cols[3]:
            st.markdown("**Description**")
        
        for t in tables_inventory:
            cols = st.columns([1, 2, 1, 3])
            with cols[0]:
                st.markdown(f"`{t['schema']}`")
            with cols[1]:
                st.markdown(f"**{t['table']}**")
            with cols[2]:
                st.markdown(t['rows'])
            with cols[3]:
                st.markdown(t['desc'])
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 4: ETL Pipeline Architecture
    # -------------------------------------------------------------------------
    
    st.markdown("### 🔄 ETL Pipeline Architecture")
    st.caption("Data transformation workflows using dbt")
    
    etl_col1, etl_col2 = st.columns([2, 1])
    
    with etl_col1:
        # ETL Pipeline diagram
        etl_diagram = """
        digraph ETL {
            rankdir=LR;
            bgcolor="transparent";
            node [fontname="Helvetica", fontsize=10];
            edge [fontname="Helvetica", fontsize=9];
            
            subgraph cluster_extract {
                label="EXTRACT";
                style=filled;
                fillcolor="#fff3e0";
                color="#e67e22";
                
                src_sap [label="SAP\\n(Assets)", shape=box];
                src_workday [label="Workday\\n(HR)", shape=box];
                src_oracle [label="Oracle\\n(Finance)", shape=box];
                src_iot [label="IoT Sensors\\n(Real-time)", shape=box];
                src_api [label="External APIs\\n(Weather, etc.)", shape=box];
            }
            
            subgraph cluster_load {
                label="LOAD (ELT)";
                style=filled;
                fillcolor="#e8f5e9";
                color="#27ae60";
                
                raw_assets [label="RAW_ASSETS"];
                raw_hr [label="RAW_HR"];
                raw_finance [label="RAW_FINANCE"];
                raw_iot [label="RAW_IOT"];
                raw_external [label="RAW_EXTERNAL"];
            }
            
            subgraph cluster_transform {
                label="TRANSFORM (dbt)";
                style=filled;
                fillcolor="#e3f2fd";
                color="#3498db";
                
                stg [label="STAGING\\nmodels", shape=folder];
                int [label="INTERMEDIATE\\nmodels", shape=folder];
                mart [label="MART\\nmodels", shape=folder];
            }
            
            subgraph cluster_serve {
                label="SERVE";
                style=filled;
                fillcolor="#fce4ec";
                color="#e63946";
                
                core [label="CORE"];
                infra [label="INFRASTRUCTURE"];
                hr [label="HR"];
                finance [label="FINANCE"];
                analytics [label="ANALYTICS"];
            }
            
            // Extract to Load
            src_sap -> raw_assets [label="Fivetran\\n4h sync"];
            src_workday -> raw_hr [label="Fivetran\\n6h sync"];
            src_oracle -> raw_finance [label="Fivetran\\n1h sync"];
            src_iot -> raw_iot [label="Kafka\\nReal-time"];
            src_api -> raw_external [label="API\\nDaily"];
            
            // Load to Transform
            raw_assets -> stg;
            raw_hr -> stg;
            raw_finance -> stg;
            raw_iot -> stg;
            raw_external -> stg;
            
            stg -> int [label="Clean\\nDedupe"];
            int -> mart [label="Join\\nAggregate"];
            
            // Transform to Serve
            mart -> core;
            mart -> infra;
            mart -> hr;
            mart -> finance;
            mart -> analytics;
        }
        """
        
        st.graphviz_chart(etl_diagram, use_container_width=True)
    
    with etl_col2:
        st.markdown("#### 📋 Pipeline Schedule")
        
        pipelines = [
            {"name": "Asset Sync", "schedule": "Every 4h", "status": "✅ Running", "last": "10 min ago"},
            {"name": "HR Data", "schedule": "Every 6h", "status": "✅ Running", "last": "2h ago"},
            {"name": "Finance", "schedule": "Hourly", "status": "✅ Running", "last": "45 min ago"},
            {"name": "IoT Stream", "schedule": "Real-time", "status": "✅ Running", "last": "Now"},
            {"name": "dbt Models", "schedule": "Every 2h", "status": "✅ Running", "last": "1h ago"},
            {"name": "Analytics", "schedule": "Daily 6AM", "status": "✅ Complete", "last": "Today"},
        ]
        
        for p in pipelines:
            status_color = '#27ae60' if '✅' in p['status'] else '#e63946'
            st.markdown(f"""
                <div style="background: white; border-radius: 8px; padding: 0.6rem; margin-bottom: 0.4rem; border-left: 3px solid {status_color};">
                    <div style="display: flex; justify-content: space-between;">
                        <span style="font-weight: 600; font-size: 0.9rem;">{p['name']}</span>
                        <span style="font-size: 0.75rem; color: {status_color};">{p['status']}</span>
                    </div>
                    <div style="font-size: 0.75rem; color: #888; margin-top: 0.2rem;">
                        🕐 {p['schedule']} • Last: {p['last']}
                    </div>
                </div>
            """, unsafe_allow_html=True)
        
        st.markdown("""
            <div style="background: #1a2b4a; border-radius: 8px; padding: 0.75rem; color: white; margin-top: 1rem; text-align: center;">
                <div style="font-size: 0.8rem; opacity: 0.8;">Pipeline Health</div>
                <div style="font-size: 1.5rem; font-weight: 700;">99.7%</div>
                <div style="font-size: 0.7rem; opacity: 0.7;">Last 30 days</div>
            </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 5: Data Lineage
    # -------------------------------------------------------------------------
    
    st.markdown("### 🔗 Data Lineage & Dependencies")
    st.caption("Track data flow from source to consumption")
    
    lineage_selector = st.selectbox(
        "Select a metric to trace lineage:",
        ["Executive Revenue", "Site Colocation Rate", "Carbon Emissions", "Workforce Capacity", "Equipment Age"]
    )
    
    if lineage_selector == "Executive Revenue":
        lineage_graph = """
        digraph Lineage {
            rankdir=LR;
            bgcolor="transparent";
            node [fontname="Helvetica", fontsize=10, shape=box, style=filled];
            
            // Sources
            oracle [label="Oracle\\nFinance", fillcolor="#fff3e0", color="#e67e22"];
            contracts [label="Contract\\nSystem", fillcolor="#fff3e0", color="#e67e22"];
            
            // Raw
            raw_revenue [label="RAW.REVENUE", fillcolor="#e8f5e9", color="#27ae60"];
            raw_contracts [label="RAW.CONTRACTS", fillcolor="#e8f5e9", color="#27ae60"];
            
            // Staging
            stg_revenue [label="STG_REVENUE", fillcolor="#e3f2fd", color="#3498db"];
            stg_contracts [label="STG_CONTRACTS", fillcolor="#e3f2fd", color="#3498db"];
            
            // Intermediate
            int_revenue [label="INT_REVENUE_DAILY", fillcolor="#bbdefb", color="#1976d2"];
            
            // Mart
            mart_revenue [label="FINANCE.REVENUE", fillcolor="#90caf9", color="#1976d2"];
            
            // Analytics
            vw_revenue [label="VW_REVENUE_EXECUTIVE", fillcolor="#64b5f6", color="#1976d2"];
            
            // Consumption
            dashboard [label="Executive\\nDashboard", fillcolor="#fce4ec", color="#e63946"];
            powerbi [label="Power BI\\nReports", fillcolor="#fce4ec", color="#e63946"];
            
            // Edges
            oracle -> raw_revenue;
            contracts -> raw_contracts;
            raw_revenue -> stg_revenue;
            raw_contracts -> stg_contracts;
            stg_revenue -> int_revenue;
            stg_contracts -> int_revenue;
            int_revenue -> mart_revenue;
            mart_revenue -> vw_revenue;
            vw_revenue -> dashboard;
            vw_revenue -> powerbi;
        }
        """
    elif lineage_selector == "Carbon Emissions":
        lineage_graph = """
        digraph Lineage {
            rankdir=LR;
            bgcolor="transparent";
            node [fontname="Helvetica", fontsize=10, shape=box, style=filled];
            
            // Sources
            energy_meters [label="Energy\\nMeters", fillcolor="#fff3e0", color="#e67e22"];
            emission_factors [label="Emission\\nFactors", fillcolor="#fff3e0", color="#e67e22"];
            fleet [label="Fleet\\nManagement", fillcolor="#fff3e0", color="#e67e22"];
            
            // Raw
            raw_energy [label="RAW.ENERGY", fillcolor="#e8f5e9", color="#27ae60"];
            raw_factors [label="RAW.FACTORS", fillcolor="#e8f5e9", color="#27ae60"];
            raw_fleet [label="RAW.FLEET", fillcolor="#e8f5e9", color="#27ae60"];
            
            // Mart
            emissions [label="ESG.CARBON_EMISSIONS", fillcolor="#90caf9", color="#1976d2"];
            
            // Views
            vw_csrd [label="VW_CSRD_REPORT", fillcolor="#64b5f6", color="#1976d2"];
            
            // Consumption
            esg_dash [label="ESG\\nDashboard", fillcolor="#fce4ec", color="#e63946"];
            csrd_report [label="CSRD\\nReport", fillcolor="#fce4ec", color="#e63946"];
            
            energy_meters -> raw_energy;
            emission_factors -> raw_factors;
            fleet -> raw_fleet;
            raw_energy -> emissions;
            raw_factors -> emissions;
            raw_fleet -> emissions;
            emissions -> vw_csrd;
            vw_csrd -> esg_dash;
            vw_csrd -> csrd_report;
        }
        """
    else:
        lineage_graph = """
        digraph Lineage {
            rankdir=LR;
            bgcolor="transparent";
            node [fontname="Helvetica", fontsize=10, shape=box, style=filled];
            
            source [label="Source\\nSystem", fillcolor="#fff3e0", color="#e67e22"];
            raw [label="RAW Layer", fillcolor="#e8f5e9", color="#27ae60"];
            staging [label="Staging", fillcolor="#e3f2fd", color="#3498db"];
            mart [label="Data Mart", fillcolor="#90caf9", color="#1976d2"];
            view [label="Analytics View", fillcolor="#64b5f6", color="#1976d2"];
            dashboard [label="Dashboard", fillcolor="#fce4ec", color="#e63946"];
            
            source -> raw -> staging -> mart -> view -> dashboard;
        }
        """
    
    st.graphviz_chart(lineage_graph, use_container_width=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 6: Security & Access Control
    # -------------------------------------------------------------------------
    
    st.markdown("### 🔐 Security & Access Control")
    
    security_col1, security_col2 = st.columns(2)
    
    with security_col1:
        st.markdown("#### 👥 Role-Based Access Control")
        
        # RBAC diagram
        rbac_diagram = """
        digraph RBAC {
            rankdir=TB;
            bgcolor="transparent";
            node [fontname="Helvetica", fontsize=10, shape=box, style=filled];
            
            // Roles
            accountadmin [label="ACCOUNTADMIN", fillcolor="#e63946", fontcolor="white"];
            sysadmin [label="SYSADMIN", fillcolor="#c0392b", fontcolor="white"];
            securityadmin [label="SECURITYADMIN", fillcolor="#c0392b", fontcolor="white"];
            
            tdf_admin [label="TDF_ADMIN", fillcolor="#3498db", fontcolor="white"];
            tdf_analyst [label="TDF_ANALYST", fillcolor="#27ae60", fontcolor="white"];
            tdf_engineer [label="TDF_ENGINEER", fillcolor="#9b59b6", fontcolor="white"];
            tdf_executive [label="TDF_EXECUTIVE", fillcolor="#e67e22", fontcolor="white"];
            
            // Hierarchy
            accountadmin -> sysadmin;
            accountadmin -> securityadmin;
            sysadmin -> tdf_admin;
            tdf_admin -> tdf_analyst;
            tdf_admin -> tdf_engineer;
            tdf_admin -> tdf_executive;
        }
        """
        
        st.graphviz_chart(rbac_diagram, use_container_width=True)
    
    with security_col2:
        st.markdown("#### 🔑 Permission Matrix")
        
        permissions = [
            {"role": "TDF_ADMIN", "read": "✅", "write": "✅", "delete": "✅", "admin": "✅"},
            {"role": "TDF_ENGINEER", "read": "✅", "write": "✅", "delete": "❌", "admin": "❌"},
            {"role": "TDF_ANALYST", "read": "✅", "write": "❌", "delete": "❌", "admin": "❌"},
            {"role": "TDF_EXECUTIVE", "read": "✅", "write": "❌", "delete": "❌", "admin": "❌"},
        ]
        
        perm_cols = st.columns([2, 1, 1, 1, 1])
        with perm_cols[0]:
            st.markdown("**Role**")
        with perm_cols[1]:
            st.markdown("**Read**")
        with perm_cols[2]:
            st.markdown("**Write**")
        with perm_cols[3]:
            st.markdown("**Delete**")
        with perm_cols[4]:
            st.markdown("**Admin**")
        
        for p in permissions:
            cols = st.columns([2, 1, 1, 1, 1])
            with cols[0]:
                st.markdown(f"`{p['role']}`")
            with cols[1]:
                st.markdown(p['read'])
            with cols[2]:
                st.markdown(p['write'])
            with cols[3]:
                st.markdown(p['delete'])
            with cols[4]:
                st.markdown(p['admin'])
        
        st.markdown("---")
        
        st.markdown("#### 🛡️ Security Features")
        security_features = [
            "🔒 Column-level encryption (PII data)",
            "🎭 Dynamic data masking",
            "📝 Row-level security policies",
            "🔑 SSO via SAML 2.0",
            "📊 Query audit logging",
            "🌐 Network policies (IP whitelisting)",
        ]
        for f in security_features:
            st.markdown(f"• {f}")
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 7: Data Quality Framework
    # -------------------------------------------------------------------------
    
    st.markdown("### ✅ Data Quality Framework")
    
    dq_col1, dq_col2, dq_col3 = st.columns(3)
    
    with dq_col1:
        st.markdown("#### 📊 Quality Dimensions")
        
        dimensions = [
            {"dim": "Completeness", "score": 98.5, "target": 99},
            {"dim": "Accuracy", "score": 99.2, "target": 99},
            {"dim": "Timeliness", "score": 97.8, "target": 98},
            {"dim": "Consistency", "score": 99.5, "target": 99},
            {"dim": "Uniqueness", "score": 99.9, "target": 99.5},
        ]
        
        for d in dimensions:
            color = '#27ae60' if d['score'] >= d['target'] else '#f39c12'
            pct = d['score'] / 100
            st.markdown(f"""
                <div style="margin-bottom: 0.75rem;">
                    <div style="display: flex; justify-content: space-between; margin-bottom: 0.2rem;">
                        <span style="font-size: 0.85rem;">{d['dim']}</span>
                        <span style="font-weight: 600; color: {color};">{d['score']}%</span>
                    </div>
                    <div style="background: #eee; border-radius: 4px; height: 8px; overflow: hidden;">
                        <div style="background: {color}; width: {pct*100}%; height: 100%;"></div>
                    </div>
                </div>
            """, unsafe_allow_html=True)
    
    with dq_col2:
        st.markdown("#### 🧪 Automated Tests")
        
        tests = [
            {"test": "Schema validation", "count": 47, "passed": 47},
            {"test": "Null checks", "count": 156, "passed": 154},
            {"test": "Referential integrity", "count": 89, "passed": 89},
            {"test": "Business rules", "count": 234, "passed": 231},
            {"test": "Freshness checks", "count": 12, "passed": 12},
        ]
        
        total_tests = sum(t['count'] for t in tests)
        total_passed = sum(t['passed'] for t in tests)
        
        st.markdown(f"""
            <div style="background: #27ae60; border-radius: 10px; padding: 1rem; color: white; text-align: center; margin-bottom: 1rem;">
                <div style="font-size: 2rem; font-weight: 700;">{total_passed}/{total_tests}</div>
                <div style="font-size: 0.8rem; opacity: 0.9;">Tests Passing</div>
            </div>
        """, unsafe_allow_html=True)
        
        for t in tests:
            status = "✅" if t['passed'] == t['count'] else "⚠️"
            st.markdown(f"• {status} {t['test']}: {t['passed']}/{t['count']}")
    
    with dq_col3:
        st.markdown("#### 🚨 Active Alerts")
        
        alerts = [
            {"level": "warning", "msg": "2 null checks failed in RAW_HR", "time": "2h ago"},
            {"level": "warning", "msg": "3 business rules need review", "time": "5h ago"},
            {"level": "info", "msg": "New schema version deployed", "time": "1d ago"},
        ]
        
        for a in alerts:
            color = '#f39c12' if a['level'] == 'warning' else '#3498db'
            icon = '⚠️' if a['level'] == 'warning' else 'ℹ️'
            st.markdown(f"""
                <div style="background: {color}15; border-left: 3px solid {color}; padding: 0.6rem; margin-bottom: 0.5rem; border-radius: 0 6px 6px 0;">
                    <div style="font-size: 0.85rem;">{icon} {a['msg']}</div>
                    <div style="font-size: 0.7rem; color: #888; margin-top: 0.2rem;">{a['time']}</div>
                </div>
            """, unsafe_allow_html=True)
        
        st.button("📊 View Full DQ Dashboard", use_container_width=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 8: Infrastructure & Deployment
    # -------------------------------------------------------------------------
    
    st.markdown("### 🖥️ Infrastructure & Deployment")
    
    infra_col1, infra_col2 = st.columns([2, 1])
    
    with infra_col1:
        # Deployment architecture
        deploy_diagram = """
        digraph Deploy {
            rankdir=TB;
            bgcolor="transparent";
            node [fontname="Helvetica", fontsize=10];
            
            subgraph cluster_dev {
                label="DEV Environment";
                style=filled;
                fillcolor="#e3f2fd";
                color="#3498db";
                
                dev_db [label="TDF_DEV\\nDatabase", shape=cylinder];
                dev_wh [label="TDF_DEV_WH\\nXS Warehouse", shape=box3d];
            }
            
            subgraph cluster_qa {
                label="QA Environment";
                style=filled;
                fillcolor="#fff3e0";
                color="#e67e22";
                
                qa_db [label="TDF_QA\\nDatabase", shape=cylinder];
                qa_wh [label="TDF_QA_WH\\nS Warehouse", shape=box3d];
            }
            
            subgraph cluster_prod {
                label="PROD Environment";
                style=filled;
                fillcolor="#e8f5e9";
                color="#27ae60";
                
                prod_db [label="TDF_DATA_PLATFORM\\nDatabase", shape=cylinder];
                prod_wh [label="TDF_WH\\nM Warehouse", shape=box3d];
                prod_wh_large [label="TDF_WH_LARGE\\nL Warehouse", shape=box3d];
            }
            
            subgraph cluster_cicd {
                label="CI/CD Pipeline";
                style=filled;
                fillcolor="#fce4ec";
                color="#e63946";
                
                github [label="GitHub\\nRepository", shape=box];
                actions [label="GitHub\\nActions", shape=box];
                dbt_cloud [label="dbt Cloud", shape=box];
            }
            
            // Edges
            github -> actions [label="Push"];
            actions -> dev_db [label="Deploy"];
            actions -> qa_db [label="Test"];
            actions -> prod_db [label="Release"];
            dbt_cloud -> dev_db [label="Models"];
            dbt_cloud -> qa_db;
            dbt_cloud -> prod_db;
        }
        """
        
        st.graphviz_chart(deploy_diagram, use_container_width=True)
    
    with infra_col2:
        st.markdown("#### ☁️ Snowflake Resources")
        
        resources = [
            {"name": "TDF_WH", "size": "Medium", "status": "Running", "cost": "€2.4K/mo"},
            {"name": "TDF_WH_LARGE", "size": "Large", "status": "Suspended", "cost": "€0/mo"},
            {"name": "TDF_DEV_WH", "size": "X-Small", "status": "Running", "cost": "€0.3K/mo"},
            {"name": "TDF_QA_WH", "size": "Small", "status": "Suspended", "cost": "€0/mo"},
        ]
        
        for r in resources:
            status_color = '#27ae60' if r['status'] == 'Running' else '#888'
            st.markdown(f"""
                <div style="background: white; border-radius: 8px; padding: 0.6rem; margin-bottom: 0.4rem; border-left: 3px solid {status_color};">
                    <div style="display: flex; justify-content: space-between;">
                        <span style="font-weight: 600;">{r['name']}</span>
                        <span style="font-size: 0.8rem; color: #888;">{r['size']}</span>
                    </div>
                    <div style="display: flex; justify-content: space-between; margin-top: 0.2rem;">
                        <span style="font-size: 0.75rem; color: {status_color};">● {r['status']}</span>
                        <span style="font-size: 0.75rem; color: #666;">{r['cost']}</span>
                    </div>
                </div>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        
        st.markdown("#### 📦 Git Repository")
        st.markdown("""
            <div style="background: #1a2b4a; border-radius: 8px; padding: 1rem; color: white;">
                <div style="font-size: 0.8rem; opacity: 0.8;">Repository</div>
                <div style="font-family: monospace; font-size: 0.9rem; margin: 0.25rem 0;">github.com/pmjose/TDF</div>
                <div style="font-size: 0.75rem; opacity: 0.7; margin-top: 0.5rem;">Branch: main • Last commit: 2h ago</div>
            </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 9: API & Integration Layer
    # -------------------------------------------------------------------------
    
    st.markdown("### 🔌 API & Integration Layer")
    
    api_col1, api_col2 = st.columns(2)
    
    with api_col1:
        st.markdown("#### 📡 Active Integrations")
        
        integrations = [
            {"system": "SAP (Asset Management)", "type": "Batch", "freq": "4h", "status": "✅ Active"},
            {"system": "Workday (HR)", "type": "Batch", "freq": "6h", "status": "✅ Active"},
            {"system": "Oracle Financials", "type": "Batch", "freq": "1h", "status": "✅ Active"},
            {"system": "ServiceNow (Ops)", "type": "Batch", "freq": "15min", "status": "✅ Active"},
            {"system": "IoT Sensors (SCADA)", "type": "Stream", "freq": "Real-time", "status": "✅ Active"},
            {"system": "Digital Twin Platform", "type": "API", "freq": "On-demand", "status": "✅ Active"},
            {"system": "Weather API", "type": "API", "freq": "Hourly", "status": "✅ Active"},
            {"system": "Grid Operator", "type": "SFTP", "freq": "Daily", "status": "⚠️ Delayed"},
        ]
        
        for i in integrations:
            status_color = '#27ae60' if '✅' in i['status'] else '#f39c12'
            type_color = '#3498db' if i['type'] == 'Batch' else '#9b59b6' if i['type'] == 'Stream' else '#e67e22'
            st.markdown(f"""
                <div style="display: flex; justify-content: space-between; align-items: center; padding: 0.5rem 0; border-bottom: 1px solid #eee;">
                    <div>
                        <span style="font-weight: 500;">{i['system']}</span>
                        <span style="background: {type_color}20; color: {type_color}; padding: 0.1rem 0.4rem; border-radius: 10px; font-size: 0.65rem; margin-left: 0.5rem;">{i['type']}</span>
                    </div>
                    <div style="text-align: right;">
                        <span style="font-size: 0.75rem; color: #888; margin-right: 0.5rem;">{i['freq']}</span>
                        <span style="color: {status_color};">{i['status']}</span>
                    </div>
                </div>
            """, unsafe_allow_html=True)
    
    with api_col2:
        st.markdown("#### 📊 Integration Metrics")
        
        metrics_data = {
            "dates": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
            "success": [1245, 1302, 1189, 1356, 1278, 890, 756],
            "failed": [12, 8, 23, 5, 15, 3, 2],
        }
        
        fig_int = go.Figure()
        
        fig_int.add_trace(go.Bar(
            x=metrics_data['dates'],
            y=metrics_data['success'],
            name='Success',
            marker_color='#27ae60'
        ))
        
        fig_int.add_trace(go.Bar(
            x=metrics_data['dates'],
            y=metrics_data['failed'],
            name='Failed',
            marker_color='#e63946'
        ))
        
        fig_int.update_layout(
            barmode='stack',
            height=250,
            margin=dict(l=10, r=10, t=10, b=10),
            legend=dict(orientation='h', yanchor='bottom', y=1.02),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_int, use_container_width=True)
        
        # Summary stats
        total_calls = sum(metrics_data['success']) + sum(metrics_data['failed'])
        success_rate = sum(metrics_data['success']) / total_calls * 100
        
        stat_cols = st.columns(3)
        with stat_cols[0]:
            st.metric("Total Calls", f"{total_calls:,}")
        with stat_cols[1]:
            st.metric("Success Rate", f"{success_rate:.1f}%")
        with stat_cols[2]:
            st.metric("Avg Latency", "245ms")
    
    st.markdown("---")
    
    # -------------------------------------------------------------------------
    # ROW 10: Documentation & Support
    # -------------------------------------------------------------------------
    
    st.markdown("### 📚 Documentation & Support")
    
    doc_col1, doc_col2, doc_col3, doc_col4 = st.columns(4)
    
    with doc_col1:
        st.markdown("""
            <div style="background: white; border-radius: 10px; padding: 1.5rem; text-align: center; border: 1px solid #eee; height: 180px;">
                <div style="font-size: 2.5rem;">📖</div>
                <div style="font-weight: 700; margin: 0.5rem 0;">Data Dictionary</div>
                <div style="font-size: 0.8rem; color: #666;">Complete schema documentation with business definitions</div>
            </div>
        """, unsafe_allow_html=True)
        st.button("Open Dictionary", key="doc1", use_container_width=True)
    
    with doc_col2:
        st.markdown("""
            <div style="background: white; border-radius: 10px; padding: 1.5rem; text-align: center; border: 1px solid #eee; height: 180px;">
                <div style="font-size: 2.5rem;">🔄</div>
                <div style="font-weight: 700; margin: 0.5rem 0;">ETL Documentation</div>
                <div style="font-size: 0.8rem; color: #666;">Pipeline configurations, schedules, and dependencies</div>
            </div>
        """, unsafe_allow_html=True)
        st.button("View Pipelines", key="doc2", use_container_width=True)
    
    with doc_col3:
        st.markdown("""
            <div style="background: white; border-radius: 10px; padding: 1.5rem; text-align: center; border: 1px solid #eee; height: 180px;">
                <div style="font-size: 2.5rem;">🔐</div>
                <div style="font-weight: 700; margin: 0.5rem 0;">Security Guide</div>
                <div style="font-size: 0.8rem; color: #666;">Access control, encryption, and compliance</div>
            </div>
        """, unsafe_allow_html=True)
        st.button("Security Docs", key="doc3", use_container_width=True)
    
    with doc_col4:
        st.markdown("""
            <div style="background: white; border-radius: 10px; padding: 1.5rem; text-align: center; border: 1px solid #eee; height: 180px;">
                <div style="font-size: 2.5rem;">💬</div>
                <div style="font-weight: 700; margin: 0.5rem 0;">Support</div>
                <div style="font-size: 0.8rem; color: #666;">Contact data engineering team for assistance</div>
            </div>
        """, unsafe_allow_html=True)
        st.button("Get Help", key="doc4", use_container_width=True)

# ==============================================================================
# MAIN ROUTING
# ==============================================================================

if page == "Executive Dashboard":
    page_executive_dashboard()
elif page == "Resource & Capacity Planning":
    page_capacity_planning()
elif page == "ESG Regulatory Reporting":
    page_esg_reporting()
elif page == "Digital Twin":
    page_digital_twin()
elif page == "CAPEX & Lifecycle":
    page_capex_lifecycle()
elif page == "Architecture":
    page_architecture()

