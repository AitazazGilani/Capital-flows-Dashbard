"""
Macro Dashboard MVP — Entry point and global controls.
Tracks global macro indicators, capital flows, and market data.
"""

import streamlit as st
from config import COUNTRIES, DEFAULT_COUNTRIES, DATE_RANGES
from claude_chat import render_chat_sidebar

st.set_page_config(page_title="Macro Dashboard", layout="wide", page_icon="🌍")

# -- Sidebar: Global Controls --
with st.sidebar:
    st.title("🌍 Macro Dashboard")

    # Country selector
    selected = st.multiselect(
        "Countries",
        options=list(COUNTRIES.keys()),
        default=DEFAULT_COUNTRIES,
        format_func=lambda x: f"{COUNTRIES[x]['name']} ({x})"
    )
    st.session_state.selected_countries = selected

    # Date range
    date_range = st.select_slider("Date Range", options=list(DATE_RANGES.keys()), value="3Y")
    st.session_state.date_range = date_range

    st.divider()

    # Claude Chat
    render_chat_sidebar()

# -- Main Content (Home Page) --
st.header("Global Macro & Capital Flows Dashboard")
st.markdown("""
Welcome to the Macro Dashboard. Use the sidebar to select countries and date range,
then navigate to specific pages for detailed analysis.

**Pages:**
- **Markets** — Equity indices, FX, commodities, volatility
- **Liquidity** — Net liquidity, Fed balance sheet, M2
- **Rates & Credit** — Yield curve, rate expectations, credit spreads
- **Economy** — GDP, inflation, employment, leading indicators
- **Capital Flows** ⭐ — Current account, trade balance, FDI, reserves, flow signals
- **Country Risk** — Debt sustainability, fiscal position, risk scores
- **Sentiment** — VIX, copper/gold ratio, consumer sentiment

*Data is currently using mock datasets for demonstration. Connect API keys in `.env` for live data.*
""")

st.info(f"**Selected countries:** {', '.join(selected)}  |  **Date range:** {date_range}")

# Show a quick summary of what each page offers
col1, col2 = st.columns(2)

with col1:
    st.subheader("Market Overview")
    st.markdown("""
    Track equity indices, currencies, and commodities across your
    selected countries. Monitor DXY, VIX, and the copper/gold ratio
    for cross-asset signals.
    """)

    st.subheader("Liquidity Conditions")
    st.markdown("""
    Net liquidity (Fed B/S - TGA - RRP) has been a dominant driver
    of asset prices. Monitor the plumbing of the financial system
    and its correlation with equity markets.
    """)

    st.subheader("Economic Indicators")
    st.markdown("""
    GDP growth, inflation, unemployment, and leading indicators
    across selected countries. Mix of real-time (FRED) and
    annual (World Bank) data.
    """)

with col2:
    st.subheader("Rates & Credit")
    st.markdown("""
    US yield curve, Fed Funds futures implied rate path, credit
    spreads, and financial conditions. Key for understanding
    monetary policy direction.
    """)

    st.subheader("Capital Flows ⭐")
    st.markdown("""
    The primary page. Current account balances, trade flows, FDI,
    foreign reserves, and composite flow signals. Understand where
    capital is moving globally.
    """)

    st.subheader("Risk & Sentiment")
    st.markdown("""
    Country risk scores based on debt, fiscal position, and
    reserves. Sentiment indicators including VIX, MOVE, and
    consumer confidence.
    """)
