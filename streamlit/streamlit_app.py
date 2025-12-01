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
    
    # Fetch EBITDA metrics
    ebitda_df = run_query("""
        SELECT * FROM TDF_DATA_PLATFORM.FINANCE.EBITDA_METRICS 
        ORDER BY PERIOD_DATE DESC LIMIT 1
    """)
    
    # Fetch ESG status
    esg_df = run_query("""
        SELECT * FROM TDF_DATA_PLATFORM.ESG.BOARD_SCORECARD 
        ORDER BY REPORTING_DATE DESC LIMIT 1
    """)
    
    # Get values with defaults
    revenue = ebitda_df['REVENUE_EUR'].iloc[0] / 1000000 if not ebitda_df.empty else 799.1
    ebitda_margin = ebitda_df['EBITDAAL_MARGIN_PCT'].iloc[0] if not ebitda_df.empty else 47.0
    yoy_growth = ebitda_df['YOY_GROWTH_PCT'].iloc[0] if not ebitda_df.empty else 8.5
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
                    title='Revenue (EUR Millions)',
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

