"""
Page 4: Economy — GDP, inflation, employment, leading indicators.
"""

import streamlit as st
import pandas as pd
from src.config import COUNTRIES, FRED, WB_INDICATORS
from src.data_fetcher import (
    get_fred_series, get_wb_indicator, get_commodities,
)
from src.chart_helpers import (
    line_chart, dual_axis_chart, grouped_bar_chart, metric_row,
)

st.session_state.current_page = "Economy"
st.header("Economy")
st.markdown("""
Fundamental economic health across selected countries. GDP growth drives earnings, inflation drives rates,
employment drives consumption. Compare economies side-by-side to spot which are accelerating vs decelerating
— capital flows toward growth and away from weakness.
""")

selected = st.session_state.get("selected_countries", ["US", "EU", "UK", "JP", "CN"])
wb_codes = [COUNTRIES[c]["wb_code"] for c in selected]
wb_to_short = {COUNTRIES[c]["wb_code"]: c for c in selected}

# --- GDP Growth ---
st.subheader("GDP Growth (Annual %)")
st.markdown("Year-over-year real GDP growth by country. Look for **divergence** — when one economy accelerates while others slow, its currency and equities tend to outperform. Negative growth = technical recession.")
with st.spinner("Loading GDP data..."):
    gdp_growth = get_wb_indicator(WB_INDICATORS["gdp_growth"], wb_codes, start_year=2010)
    gdp_growth.columns = [wb_to_short.get(c, c) for c in gdp_growth.columns]

# Show last 5 years as grouped bar
gdp_recent = gdp_growth.tail(5)
st.plotly_chart(
    grouped_bar_chart(gdp_recent, "GDP Growth by Country (Annual %)"),
    use_container_width=True,
)

st.divider()

# --- Inflation ---
st.subheader("Inflation (CPI YoY %)")
st.markdown("Consumer price inflation vs the 2% target. **Above target** = central banks stay hawkish (rates stay high, currencies strengthen). **Below target** = room for rate cuts (bullish for bonds and equities).")
with st.spinner("Loading inflation data..."):
    inflation = get_wb_indicator(WB_INDICATORS["inflation_cpi"], wb_codes, start_year=2010)
    inflation.columns = [wb_to_short.get(c, c) for c in inflation.columns]

import plotly.graph_objects as go
fig = line_chart(inflation, "CPI Inflation by Country (Annual %)")
# Add 2% target reference line
fig.add_hline(y=2.0, line_dash="dash", line_color="yellow",
              annotation_text="2% Target", annotation_position="bottom right")
st.plotly_chart(fig, use_container_width=True)

st.divider()

# --- Jobless Claims ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Initial Claims (Weekly)")
    st.markdown("New unemployment filings. The 4-week moving average smooths noise. Claims **rising above 300K** = labor market deteriorating. This is the fastest economic indicator — data comes weekly, not monthly.")
    with st.spinner("Loading claims data..."):
        initial_claims = get_fred_series(FRED["initial_claims"])
    claims_df = pd.DataFrame({
        "Initial Claims": initial_claims,
        "4-Week MA": initial_claims.rolling(4).mean(),
    })
    st.plotly_chart(
        line_chart(claims_df, "Initial Jobless Claims"),
        use_container_width=True,
    )

with col2:
    st.subheader("Continuing Claims")
    st.markdown("People still receiving unemployment benefits. Rising continuing claims = people aren't finding jobs quickly. This lags initial claims and confirms whether layoffs are turning into sustained unemployment.")
    with st.spinner("Loading continuing claims..."):
        cont_claims = get_fred_series(FRED["continuing_claims"])
    cont_df = pd.DataFrame({
        "Continuing Claims": cont_claims,
        "4-Week MA": cont_claims.rolling(4).mean(),
    })
    st.plotly_chart(
        line_chart(cont_df, "Continuing Claims"),
        use_container_width=True,
    )

st.divider()

# --- Consumer Sentiment & BDI ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Consumer Sentiment (Michigan)")
    st.markdown("How consumers feel about the economy. Sentiment leads spending — when it collapses, retail and consumer discretionary stocks follow.")
    with st.spinner("Loading sentiment data..."):
        sentiment = get_fred_series(FRED["consumer_sentiment"])
    sent_df = sentiment.to_frame(name="U of M Sentiment")
    st.plotly_chart(
        line_chart(sent_df, "University of Michigan Consumer Sentiment"),
        use_container_width=True,
    )

with col2:
    st.subheader("Baltic Dry Index (BDI)")
    st.markdown("Cost of shipping raw materials globally. BDI is hard to manipulate (no futures speculation) — it purely reflects real trade demand. **Rising BDI = global trade expanding.** Sharp drops often precede economic slowdowns.")
    with st.spinner("Loading BDI data..."):
        comm_data = get_commodities()
    if "BDI" in comm_data.columns:
        bdi_df = comm_data[["BDI"]]
        st.plotly_chart(
            line_chart(bdi_df, "Baltic Dry Index"),
            use_container_width=True,
        )
    else:
        st.info("BDI data not available. Run `python ingestor.py --source market` to ingest.")

st.divider()

# --- Unemployment ---
st.subheader("Unemployment Rates")
with st.spinner("Loading unemployment data..."):
    unemployment = get_wb_indicator(WB_INDICATORS["unemployment"], wb_codes, start_year=2010)
    unemployment.columns = [wb_to_short.get(c, c) for c in unemployment.columns]
st.plotly_chart(
    line_chart(unemployment, "Unemployment Rate by Country (%)"),
    use_container_width=True,
)

st.divider()

# --- LEI ---
st.subheader("Leading Economic Index (US)")
st.markdown("Composite of 10 leading indicators (claims, building permits, stock prices, credit, etc.). **Below 100 = contraction territory.** Six consecutive monthly declines have preceded every recession since 1960.")
with st.spinner("Loading LEI data..."):
    lei = get_fred_series(FRED["lei"])
lei_df = lei.to_frame(name="LEI")
fig_lei = line_chart(lei_df, "OECD Leading Economic Indicator (US)")
fig_lei.add_hline(y=100, line_dash="dash", line_color="yellow",
                  annotation_text="Expansion/Contraction", annotation_position="bottom right")
st.plotly_chart(fig_lei, use_container_width=True)

# Store summary for Claude
st.session_state.economy_summary = {
    "gdp_growth_latest": {c: f"{gdp_growth[c].dropna().iloc[-1]:.1f}%" for c in gdp_growth.columns if not gdp_growth[c].dropna().empty},
    "inflation_latest": {c: f"{inflation[c].dropna().iloc[-1]:.1f}%" for c in inflation.columns if not inflation[c].dropna().empty},
    "initial_claims": f"{initial_claims.iloc[-1]:,.0f}",
    "consumer_sentiment": f"{sentiment.iloc[-1]:.1f}",
    "lei": f"{lei.iloc[-1]:.1f}",
}
