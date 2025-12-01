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
    render_header(
        "Executive Dashboard",
        "Real-time KPIs and strategic metrics for TDF Infrastructure"
    )
    render_placeholder(
        "Executive Dashboard",
        "Key performance indicators, revenue trends, client concentration, and infrastructure metrics"
    )

# ==============================================================================
# PAGE: RESOURCE & CAPACITY PLANNING
# ==============================================================================

def page_capacity_planning():
    render_header(
        "Resource & Capacity Planning",
        "Workforce utilization and demand forecasting"
    )
    render_placeholder(
        "Resource & Capacity Planning",
        "Workforce utilization, capacity vs demand analysis, skill gaps, and work order backlog"
    )

# ==============================================================================
# PAGE: ESG REGULATORY REPORTING
# ==============================================================================

def page_esg_reporting():
    render_header(
        "ESG Regulatory Reporting",
        "Environmental, Social & Governance compliance tracking"
    )
    render_placeholder(
        "ESG Regulatory Reporting",
        "Carbon emissions, renewable energy progress, French Equality Index, and compliance status"
    )

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

