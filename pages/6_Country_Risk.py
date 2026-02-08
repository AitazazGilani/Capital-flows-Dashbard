"""
Page 6: Country Risk — Debt sustainability, fiscal position, risk scores.
"""

import streamlit as st
import pandas as pd
from config import COUNTRIES, WB_INDICATORS
from data_fetcher import (
    get_wb_indicator, get_imf_gold_reserves,
)
from chart_helpers import (
    line_chart, bar_chart, grouped_bar_chart, sortable_table, metric_row,
)
from processors import compute_risk_scores

st.session_state.current_page = "Country Risk"
st.header("Country Risk")

selected = st.session_state.get("selected_countries", ["US", "EU", "UK", "JP", "CN"])
wb_codes = [COUNTRIES[c]["wb_code"] for c in selected]
wb_to_short = {COUNTRIES[c]["wb_code"]: c for c in selected}

# --- Country Deep Dive Selector ---
deep_dive = st.selectbox(
    "Country Deep Dive",
    options=selected,
    format_func=lambda x: f"{COUNTRIES[x]['name']} ({x})",
)

# --- Load Data ---
with st.spinner("Loading country risk data..."):
    debt_gdp = get_wb_indicator(WB_INDICATORS["debt_to_gdp"], wb_codes, start_year=2005)
    debt_gdp.columns = [wb_to_short.get(c, c) for c in debt_gdp.columns]

    ca_gdp = get_wb_indicator(WB_INDICATORS["current_account_pct_gdp"], wb_codes, start_year=2005)
    ca_gdp.columns = [wb_to_short.get(c, c) for c in ca_gdp.columns]

    reserves = get_wb_indicator(WB_INDICATORS["reserves_excl_gold"], wb_codes, start_year=2005)
    reserves.columns = [wb_to_short.get(c, c) for c in reserves.columns]

    budget = get_wb_indicator(WB_INDICATORS["budget_balance_pct_gdp"], wb_codes, start_year=2005)
    budget.columns = [wb_to_short.get(c, c) for c in budget.columns]

    gdp = get_wb_indicator(WB_INDICATORS["gdp_current_usd"], wb_codes, start_year=2005)
    gdp.columns = [wb_to_short.get(c, c) for c in gdp.columns]

    gdp_growth = get_wb_indicator(WB_INDICATORS["gdp_growth"], wb_codes, start_year=2005)
    gdp_growth.columns = [wb_to_short.get(c, c) for c in gdp_growth.columns]

# --- Deep Dive Scorecard ---
st.subheader(f"Scorecard: {COUNTRIES[deep_dive]['name']}")

dd_metrics = []
if deep_dive in gdp_growth.columns and not gdp_growth[deep_dive].dropna().empty:
    dd_metrics.append({"label": "GDP Growth", "value": f"{gdp_growth[deep_dive].dropna().iloc[-1]:.1f}%"})
if deep_dive in debt_gdp.columns and not debt_gdp[deep_dive].dropna().empty:
    dd_metrics.append({"label": "Debt/GDP", "value": f"{debt_gdp[deep_dive].dropna().iloc[-1]:.0f}%"})
if deep_dive in ca_gdp.columns and not ca_gdp[deep_dive].dropna().empty:
    dd_metrics.append({"label": "CA % GDP", "value": f"{ca_gdp[deep_dive].dropna().iloc[-1]:.1f}%"})
if deep_dive in reserves.columns and not reserves[deep_dive].dropna().empty:
    dd_metrics.append({"label": "Reserves", "value": f"${reserves[deep_dive].dropna().iloc[-1] / 1e9:,.0f}B"})
if deep_dive in budget.columns and not budget[deep_dive].dropna().empty:
    dd_metrics.append({"label": "Budget Bal", "value": f"{budget[deep_dive].dropna().iloc[-1]:.1f}% GDP"})

# Gold reserves for deep dive
imf_code = COUNTRIES[deep_dive]["imf_code"]
gold_df = get_imf_gold_reserves(imf_code)
if not gold_df.empty:
    dd_metrics.append({"label": "Gold Reserves", "value": f"{gold_df['Tonnes'].iloc[-1]:,.0f} tonnes"})

if dd_metrics:
    metric_row(dd_metrics)

st.divider()

# --- Reserves & Gold ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Foreign Reserves")
    reserves_bn = reserves / 1e9
    st.plotly_chart(
        line_chart(reserves_bn, "Foreign Reserves ($B)"),
        use_container_width=True,
    )

with col2:
    st.subheader("Gold Reserves")
    gold_data = {}
    for c in selected:
        imf_c = COUNTRIES[c]["imf_code"]
        g = get_imf_gold_reserves(imf_c)
        if not g.empty:
            gold_data[c] = g["Tonnes"].iloc[-1]
    if gold_data:
        gold_series = pd.Series(gold_data)
        st.plotly_chart(
            bar_chart(gold_series, "Gold Reserves (Tonnes)",
                      color_positive="#FFD700", color_negative="#FFD700"),
            use_container_width=True,
        )

st.divider()

# --- Debt & Budget ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Debt-to-GDP")
    st.plotly_chart(
        line_chart(debt_gdp, "Government Debt (% GDP)"),
        use_container_width=True,
    )

with col2:
    st.subheader("Budget Balance (% GDP)")
    latest_budget = budget.iloc[-1]
    st.plotly_chart(
        bar_chart(latest_budget, "Budget Balance (% GDP) — Latest"),
        use_container_width=True,
    )

st.divider()

# --- Comparative Risk Table ---
st.subheader("Comparative Risk Scores")

risk_scores = compute_risk_scores(debt_gdp, ca_gdp, reserves, budget)

sortable_table(
    risk_scores,
    "Country Risk Assessment",
    color_columns=["CA/GDP", "Budget Bal (% GDP)", "Risk Score"],
)

st.markdown("""
**Risk Score (0-100):** Composite of debt/GDP, current account balance,
foreign reserves, and budget deficit. Higher = riskier.
""")

# Store summary for Claude
st.session_state.risk_summary = {
    "risk_scores": risk_scores.to_dict() if not risk_scores.empty else {},
    "deep_dive_country": deep_dive,
}
