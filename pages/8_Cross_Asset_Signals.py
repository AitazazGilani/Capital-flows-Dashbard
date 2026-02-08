"""
Page 8: Cross-Asset Signals — Relative value, carry trade, equity risk premium,
momentum signals, and macro catalyst synthesis.

This page turns the other 7 pages into a decision matrix.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from src.config import COUNTRIES, WB_INDICATORS, POLICY_RATES
from src.data_fetcher import (
    get_index_data, get_multiple_tickers, get_fx_rates, get_commodities,
    get_wb_indicator, get_fred_series, get_policy_events,
)
from src.chart_helpers import (
    line_chart, bar_chart, heatmap, sortable_table, metric_row,
    CHART_TEMPLATE, CHART_MARGINS, CHART_FONT, COLORS,
)
from src.processors import (
    compute_rate_differentials, compute_equity_risk_premium,
    compute_relative_value_matrix, compute_cross_asset_momentum,
    compute_flow_signals, compute_macro_catalyst_score,
)

st.session_state.current_page = "Cross-Asset Signals"
st.header("Cross-Asset Signals & Relative Value")
st.caption("Synthesizes macro data into actionable investment signals across countries and asset classes")

selected = st.session_state.get("selected_countries", ["US", "EU", "UK", "JP", "CN"])
wb_codes = [COUNTRIES[c]["wb_code"] for c in selected]
wb_to_short = {COUNTRIES[c]["wb_code"]: c for c in selected}

# ===================================================================
# SECTION 1: Relative Value Decision Matrix
# ===================================================================
st.subheader("Relative Value Decision Matrix")
st.markdown("""
Combines equity risk premium, carry trade attractiveness, and capital flow direction
into a single score per country. **Positive = attractive, Negative = avoid.**
""")

with st.spinner("Computing relative value signals..."):
    # Rate differentials
    carry_df = compute_rate_differentials(selected)

    # Equity risk premium
    erp_df = compute_equity_risk_premium(selected)

    # Flow signals (need World Bank data)
    ca_pct_gdp = get_wb_indicator(WB_INDICATORS["current_account_pct_gdp"], wb_codes, start_year=2005)
    ca_pct_gdp.columns = [wb_to_short.get(c, c) for c in ca_pct_gdp.columns]
    reserves = get_wb_indicator(WB_INDICATORS["reserves_excl_gold"], wb_codes, start_year=2005)
    reserves.columns = [wb_to_short.get(c, c) for c in reserves.columns]
    fdi_in = get_wb_indicator(WB_INDICATORS["fdi_inflows"], wb_codes, start_year=2005)
    fdi_in.columns = [wb_to_short.get(c, c) for c in fdi_in.columns]
    fdi_out = get_wb_indicator(WB_INDICATORS["fdi_outflows"], wb_codes, start_year=2005)
    fdi_out.columns = [wb_to_short.get(c, c) for c in fdi_out.columns]
    fdi_net = fdi_in - fdi_out
    fx_data = get_fx_rates(selected)
    flow_signals = compute_flow_signals(ca_pct_gdp, reserves, fdi_net, fx_data)

    # Combine into relative value matrix
    rv_matrix = compute_relative_value_matrix(selected, erp_df, carry_df, flow_signals)

# Display the decision matrix
sortable_table(
    rv_matrix[["Name", "ERP Score", "Carry Score", "Flow Score", "Total Score", "Signal"]],
    "Investment Signal by Country",
    color_columns=["ERP Score", "Carry Score", "Flow Score", "Total Score", "Signal"],
)

# Visual summary bar
scores = rv_matrix[rv_matrix.index != "US"]["Total Score"]
if not scores.empty:
    fig_rv = go.Figure(go.Bar(
        x=[COUNTRIES[c]["name"] if c in COUNTRIES else c for c in scores.index],
        y=scores.values,
        marker_color=[COLORS[2] if v > 0 else (COLORS[1] if v < 0 else COLORS[4]) for v in scores.values],
        text=[f"{v:+d}" for v in scores.values],
        textposition="outside",
    ))
    fig_rv.update_layout(
        title="Relative Value Score by Country (vs US)",
        height=350, template=CHART_TEMPLATE, margin=CHART_MARGINS, font=CHART_FONT,
        yaxis_title="Score (-3 to +3)",
    )
    st.plotly_chart(fig_rv, use_container_width=True)

st.divider()

# ===================================================================
# SECTION 2: Rate Differentials & Carry Trade
# ===================================================================
st.subheader("Rate Differentials & Carry Trade")

col1, col2 = st.columns(2)

with col1:
    st.markdown("**Policy Rate Differential vs US**")
    if not carry_df.empty:
        st.dataframe(
            carry_df[["Name", "Policy Rate (%)", "Differential (%)", "Carry Signal"]],
            use_container_width=True,
        )

with col2:
    st.markdown("**Carry Trade Attractiveness**")
    if not carry_df.empty:
        diff_series = carry_df["Differential (%)"]
        fig_carry = go.Figure(go.Bar(
            x=[COUNTRIES[c]["name"] if c in COUNTRIES else c for c in diff_series.index],
            y=diff_series.values,
            marker_color=[COLORS[2] if v > 0 else COLORS[1] for v in diff_series.values],
            text=[f"{v:+.2f}%" for v in diff_series.values],
            textposition="outside",
        ))
        fig_carry.update_layout(
            title="Rate Differential vs US (%)",
            height=350, template=CHART_TEMPLATE, margin=CHART_MARGINS, font=CHART_FONT,
            yaxis_title="Differential (%)",
        )
        st.plotly_chart(fig_carry, use_container_width=True)

st.caption(
    "Positive carry = country yields more than US. Capital tends to flow toward positive carry, "
    "but sudden reversals (carry unwind) can amplify volatility. Watch JPY carry trade closely."
)

st.divider()

# ===================================================================
# SECTION 3: Equity Risk Premium
# ===================================================================
st.subheader("Equity Risk Premium (ERP)")
st.markdown("""
ERP = Earnings Yield - Real Yield. Higher ERP means equities are **cheaper relative to bonds**.
Markets with high ERP and positive flow signals are the most attractive.
""")

if not erp_df.empty:
    col1, col2 = st.columns(2)

    with col1:
        st.dataframe(
            erp_df[["Name", "P/E", "Earnings Yield (%)", "Est. Real Yield (%)", "ERP (%)", "Valuation"]],
            use_container_width=True,
        )

    with col2:
        erp_vals = erp_df["ERP (%)"]
        fig_erp = go.Figure(go.Bar(
            x=[COUNTRIES[c]["name"] if c in COUNTRIES else c for c in erp_vals.index],
            y=erp_vals.values,
            marker_color=[COLORS[2] if v > 4 else (COLORS[1] if v < 1 else COLORS[0]) for v in erp_vals.values],
            text=[f"{v:.1f}%" for v in erp_vals.values],
            textposition="outside",
        ))
        fig_erp.add_hline(y=4, line_dash="dash", line_color="green",
                          annotation_text="Cheap", annotation_position="right")
        fig_erp.add_hline(y=1, line_dash="dash", line_color="red",
                          annotation_text="Rich", annotation_position="right")
        fig_erp.update_layout(
            title="Equity Risk Premium by Country (%)",
            height=350, template=CHART_TEMPLATE, margin=CHART_MARGINS, font=CHART_FONT,
            yaxis_title="ERP (%)",
        )
        st.plotly_chart(fig_erp, use_container_width=True)

st.divider()

# ===================================================================
# SECTION 4: Cross-Asset Momentum
# ===================================================================
st.subheader("Cross-Asset Momentum Dashboard")
st.markdown("1-month and 3-month return momentum across equities, FX, and commodities.")

with st.spinner("Computing momentum signals..."):
    # Equities
    index_tickers = {c: COUNTRIES[c]["index"] for c in selected}
    equity_data = get_multiple_tickers(list(index_tickers.values()))
    equity_data.columns = [c for c in selected]

    # Commodities
    comm_data = get_commodities()

    # Compute momentum
    momentum = compute_cross_asset_momentum(equity_data, fx_data, comm_data)

if not momentum.empty:
    # Color the trend column
    sortable_table(
        momentum,
        "Momentum Signals",
        color_columns=["1M Return (%)", "3M Return (%)", "Trend"],
    )

    # Heatmap of returns
    equity_momentum = momentum[momentum["Asset Class"] == "Equities"][["1M Return (%)", "3M Return (%)"]].astype(float)
    if not equity_momentum.empty:
        st.plotly_chart(
            heatmap(equity_momentum, "Equity Momentum (% Returns)", colorscale="RdYlGn", fmt=".1f"),
            use_container_width=True,
        )

st.divider()

# ===================================================================
# SECTION 5: Macro Catalyst Scorecard
# ===================================================================
st.subheader("Macro Catalyst Scorecard")
st.markdown("""
Net policy environment in the last 90 days. Counts bullish vs bearish catalysts
from trade policy, central bank actions, and geopolitical events.
""")

with st.spinner("Scoring macro catalysts..."):
    events = get_policy_events()
    catalyst_scores = compute_macro_catalyst_score(events, lookback_days=90)

if not catalyst_scores.empty:
    sortable_table(
        catalyst_scores,
        "Policy Environment by Country (Last 90 Days)",
        color_columns=["Net Score", "Policy Bias"],
    )
else:
    st.info("No policy events in the lookback window.")

st.markdown("""
**How to use this page:**
- Countries with **positive Total Score** in the decision matrix have the best combination of value, carry, and flows
- **ERP > 4%** suggests equities are cheap relative to bonds — look for entry points
- **Positive carry** attracts capital but watch for unwind risk (especially JPY)
- **Momentum** confirms whether the trend supports the value thesis
- **Catalyst scorecard** shows whether the policy environment is tailwind or headwind
""")

# Store summary for Claude
st.session_state.cross_asset_summary = {
    "relative_value": rv_matrix.to_dict() if not rv_matrix.empty else {},
    "carry_signals": carry_df.to_dict() if not carry_df.empty else {},
    "erp_signals": erp_df.to_dict() if not erp_df.empty else {},
}
