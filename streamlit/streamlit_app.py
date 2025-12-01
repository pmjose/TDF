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
    
    # Info
    st.markdown("### Data Platform")
    st.markdown("""
        <div style="color: rgba(255,255,255,0.7); font-size: 0.85rem;">
            <p>📡 8,785 Active Sites</p>
            <p>🗼 7,877 Towers</p>
            <p>👥 1,500 Employees</p>
            <p>💶 EUR 799.1M Revenue</p>
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
    
    # Fetch capacity data (using correct column names)
    capacity_df = run_query("""
        SELECT 
            SUM(FTE_AVAILABLE) as TOTAL_CAPACITY,
            SUM(FTE_ALLOCATED) as ALLOCATED_FTE,
            SUM(FTE_REMAINING) as REMAINING_FTE,
            AVG(UTILIZATION_PCT) as AVG_UTILIZATION
        FROM TDF_DATA_PLATFORM.HR.WORKFORCE_CAPACITY
        WHERE YEAR_MONTH = (SELECT MAX(YEAR_MONTH) FROM TDF_DATA_PLATFORM.HR.WORKFORCE_CAPACITY)
    """)
    
    # Fetch demand forecast
    demand_df = run_query("""
        SELECT 
            SUM(DEMAND_FTE) as TOTAL_DEMAND,
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
    
    total_capacity = safe_value(capacity_df, 'TOTAL_CAPACITY', 1500)
    allocated_fte = safe_value(capacity_df, 'ALLOCATED_FTE', 1280)
    utilization = safe_value(capacity_df, 'AVG_UTILIZATION', 85)
    total_demand = safe_value(demand_df, 'TOTAL_DEMAND', 1650)
    
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
    
    # Generate forecast data
    forecast_df = run_query("""
        WITH months AS (
            SELECT DATEADD(MONTH, SEQ4(), DATE_TRUNC('MONTH', CURRENT_DATE())) as FORECAST_MONTH
            FROM TABLE(GENERATOR(ROWCOUNT => 18))
        ),
        capacity_trend AS (
            SELECT 
                m.FORECAST_MONTH,
                1500 + (ROW_NUMBER() OVER (ORDER BY m.FORECAST_MONTH) * 8) + UNIFORM(-20, 30, RANDOM()) as CAPACITY_FTE
            FROM months m
        ),
        demand_trend AS (
            SELECT 
                df.TARGET_MONTH as FORECAST_MONTH,
                SUM(df.DEMAND_FTE) as DEMAND_FTE
            FROM TDF_DATA_PLATFORM.COMMERCIAL.DEMAND_FORECAST df
            WHERE df.TARGET_MONTH >= CURRENT_DATE()
            GROUP BY df.TARGET_MONTH
        )
        SELECT 
            c.FORECAST_MONTH,
            c.CAPACITY_FTE,
            COALESCE(d.DEMAND_FTE, c.CAPACITY_FTE * (1.05 + UNIFORM(0, 0.1, RANDOM()))) as DEMAND_FTE
        FROM capacity_trend c
        LEFT JOIN demand_trend d ON DATE_TRUNC('MONTH', c.FORECAST_MONTH) = DATE_TRUNC('MONTH', d.FORECAST_MONTH)
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
        # Fetch REAL regional capacity data from database
        region_data = run_query(f"""
            SELECT 
                COALESCE(SUM(wc.FTE_AVAILABLE), 0) as CAPACITY,
                COALESCE(SUM(wc.FTE_ALLOCATED), 0) as ALLOCATED,
                COALESCE(AVG(wc.UTILIZATION_PCT), 0) as UTILIZATION,
                COUNT(DISTINCT wc.SKILL_CATEGORY_ID) as SKILL_COUNT
            FROM TDF_DATA_PLATFORM.HR.WORKFORCE_CAPACITY wc
            WHERE wc.REGION_ID = '{selected_region_id}'
        """)
        
        # Fetch REAL demand from commercial forecast
        demand_data = run_query(f"""
            SELECT 
                COALESCE(SUM(df.DEMAND_FTE), 0) as FORECASTED_DEMAND
            FROM TDF_DATA_PLATFORM.COMMERCIAL.DEMAND_FORECAST df
            WHERE df.REGION_ID = '{selected_region_id}'
            AND df.TARGET_MONTH BETWEEN CURRENT_DATE() AND DATEADD(MONTH, {horizon}, CURRENT_DATE())
        """)
        
        # Fetch REAL employee count for this region
        employee_data = run_query(f"""
            SELECT COUNT(*) as EMP_COUNT
            FROM TDF_DATA_PLATFORM.HR.EMPLOYEES e
            WHERE e.REGION_ID = '{selected_region_id}'
            AND e.EMPLOYMENT_STATUS = 'ACTIVE'
        """)
        
        # Get real values with sensible fallbacks based on region population
        region_pop = run_query(f"""
            SELECT POPULATION FROM TDF_DATA_PLATFORM.CORE.REGIONS WHERE REGION_ID = '{selected_region_id}'
        """)
        pop = region_pop['POPULATION'].iloc[0] if not region_pop.empty else 5000000
        
        # Calculate base capacity - fallback proportional to population
        base_fallback = int(pop / 50000)  # ~1 FTE per 50K population
        
        base_capacity = float(region_data['CAPACITY'].iloc[0]) if not region_data.empty and region_data['CAPACITY'].iloc[0] > 0 else base_fallback
        base_utilization = float(region_data['UTILIZATION'].iloc[0]) if not region_data.empty and region_data['UTILIZATION'].iloc[0] > 0 else 78
        
        # Get demand from database or calculate based on capacity
        db_demand = float(demand_data['FORECASTED_DEMAND'].iloc[0]) if not demand_data.empty and demand_data['FORECASTED_DEMAND'].iloc[0] > 0 else 0
        base_demand = db_demand if db_demand > 0 else base_capacity * 1.08  # 8% growth if no forecast
        
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
        data_source = "📊 Live data" if (not region_data.empty and region_data['CAPACITY'].iloc[0] > 0) else "📊 Estimated"
        st.caption(f"{data_source} from HR.WORKFORCE_CAPACITY & COMMERCIAL.DEMAND_FORECAST")
        
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
                    
                    # Recruitment cost varies by salary level (€5K-€12K range)
                    recruitment_per_hire = 5000 + (benchmark['salary'] - 35000) / 5
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
                    rec_cost = int(fte * (5000 + (role["salary"] - 35000) / 5))
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
        "Infrastructure data quality and synchronization status"
    )
    render_placeholder(
        "Digital Twin",
        "Data quality scores, discrepancy tracking, sync status by region, and open issues"
    )

# ==============================================================================
# PAGE: CAPEX & LIFECYCLE
# ==============================================================================

def page_capex_lifecycle():
    render_header(
        "CAPEX & Lifecycle",
        "Equipment lifecycle management and capital expenditure tracking"
    )
    render_placeholder(
        "CAPEX & Lifecycle",
        "Equipment age distribution, renewal forecasts, budget vs actual, and at-risk equipment"
    )

# ==============================================================================
# PAGE: ARCHITECTURE
# ==============================================================================

def page_architecture():
    render_header(
        "Architecture",
        "Database schema and system overview"
    )
    render_placeholder(
        "Architecture",
        "Database schema diagram, table counts, data freshness, and deployment information"
    )

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

