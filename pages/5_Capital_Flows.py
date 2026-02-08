"""
Page 5: Capital Flows ⭐ — PRIMARY PAGE
Current account, trade balance, FDI, reserves, flow signals.
"""

import streamlit as st
import pandas as pd
from src.config import COUNTRIES, WB_INDICATORS
from src.data_fetcher import (
    get_wb_indicator, get_fx_rates, get_imf_gold_reserves,
)
from src.chart_helpers import (
    line_chart, bar_chart, grouped_bar_chart, sortable_table, metric_row,
)
from src.processors import compute_flow_signals

st.session_state.current_page = "Capital Flows"
st.header("⭐ Capital Flows")
st.caption("Primary analysis page — understanding where global capital is moving")

selected = st.session_state.get("selected_countries", ["US", "EU", "UK", "JP", "CN"])
wb_codes = [COUNTRIES[c]["wb_code"] for c in selected]
wb_to_short = {COUNTRIES[c]["wb_code"]: c for c in selected}

# --- Load Data ---
with st.spinner("Loading capital flows data..."):
    ca_pct_gdp = get_wb_indicator(WB_INDICATORS["current_account_pct_gdp"], wb_codes, start_year=2005)
    ca_pct_gdp.columns = [wb_to_short.get(c, c) for c in ca_pct_gdp.columns]

    trade_balance = get_wb_indicator(WB_INDICATORS["trade_balance"], wb_codes, start_year=2005)
    trade_balance.columns = [wb_to_short.get(c, c) for c in trade_balance.columns]

    fdi_inflows = get_wb_indicator(WB_INDICATORS["fdi_inflows"], wb_codes, start_year=2005)
    fdi_inflows.columns = [wb_to_short.get(c, c) for c in fdi_inflows.columns]

    fdi_outflows = get_wb_indicator(WB_INDICATORS["fdi_outflows"], wb_codes, start_year=2005)
    fdi_outflows.columns = [wb_to_short.get(c, c) for c in fdi_outflows.columns]

    reserves = get_wb_indicator(WB_INDICATORS["reserves_excl_gold"], wb_codes, start_year=2005)
    reserves.columns = [wb_to_short.get(c, c) for c in reserves.columns]

    fx_data = get_fx_rates(selected)

# --- Current Account Balance ---
st.subheader("Current Account Balance (% GDP)")
view_mode = st.radio("View", ["% GDP", "Time Series"], horizontal=True, key="ca_view")

if view_mode == "% GDP":
    # Latest values as bar chart
    latest_ca = ca_pct_gdp.iloc[-1]
    st.plotly_chart(
        bar_chart(latest_ca, "Current Account Balance (% GDP) — Latest"),
        use_container_width=True,
    )
else:
    st.plotly_chart(
        line_chart(ca_pct_gdp, "Current Account Balance (% GDP) — Time Series"),
        use_container_width=True,
    )

st.divider()

# --- Trade Balance & FDI ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Trade Balance")
    latest_trade = trade_balance.iloc[-1] / 1e9  # Convert to billions
    st.plotly_chart(
        bar_chart(latest_trade, "Trade Balance ($B) — Latest"),
        use_container_width=True,
    )

with col2:
    st.subheader("FDI (Inflows vs Outflows)")
    fdi_net = fdi_inflows - fdi_outflows
    fdi_net.columns = [c for c in fdi_net.columns]

    # Show latest as grouped bar
    fdi_compare = pd.DataFrame({
        "Inflows": fdi_inflows.iloc[-1] / 1e9,
        "Outflows": -fdi_outflows.iloc[-1] / 1e9,
        "Net": fdi_net.iloc[-1] / 1e9,
    })
    st.plotly_chart(
        grouped_bar_chart(fdi_compare.T, "FDI Flows ($B) — Latest"),
        use_container_width=True,
    )

st.divider()

# --- Foreign Reserves ---
st.subheader("Foreign Reserves (excl. Gold)")
reserves_bn = reserves / 1e9
st.plotly_chart(
    line_chart(reserves_bn, "Foreign Reserves ($B)", yaxis_title="$Billions"),
    use_container_width=True,
)

st.divider()

# --- Gold Reserves ---
st.subheader("Gold Reserves")
with st.spinner("Loading gold reserves..."):
    gold_data = {}
    for c in selected:
        imf_code = COUNTRIES[c]["imf_code"]
        gold_df = get_imf_gold_reserves(imf_code)
        if not gold_df.empty:
            gold_data[c] = gold_df["Tonnes"].iloc[-1]

if gold_data:
    gold_series = pd.Series(gold_data)
    st.plotly_chart(
        bar_chart(gold_series, "Gold Reserves (Tonnes) — Latest",
                  color_positive="#FFD700", color_negative="#FFD700"),
        use_container_width=True,
    )

st.divider()

# --- Capital Flow Signals Summary ---
st.subheader("Capital Flow Signals Summary")

# Compute flow signals
signals = compute_flow_signals(ca_pct_gdp, reserves, fdi_net, fx_data)

sortable_table(
    signals,
    "Flow Signal Dashboard",
    color_columns=["CA Trend", "Reserve Trend", "FDI Net", "FX Momentum", "Signal"],
)

st.markdown("""
**Signal interpretation:**
- **Inflow**: Multiple indicators suggest capital is flowing into the country
- **Outflow**: Multiple indicators suggest capital is leaving the country
- **Neutral**: Mixed signals, no clear direction
""")

# Store summary for Claude
st.session_state.flows_summary = {
    "current_account": {c: f"{ca_pct_gdp[c].dropna().iloc[-1]:.1f}% GDP" for c in ca_pct_gdp.columns if not ca_pct_gdp[c].dropna().empty},
    "reserves_bn": {c: f"${reserves_bn[c].dropna().iloc[-1]:,.0f}B" for c in reserves_bn.columns if not reserves_bn[c].dropna().empty},
    "flow_signals": signals.to_dict() if not signals.empty else {},
}
