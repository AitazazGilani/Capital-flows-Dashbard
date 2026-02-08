"""
Page 2: Liquidity — Net liquidity, Fed balance sheet, M2.
"""

import streamlit as st
import pandas as pd
import numpy as np
from config import FRED, YF_PERIOD_MAP
from data_fetcher import get_fred_series, get_index_data
from chart_helpers import line_chart, dual_axis_chart, stacked_area, metric_row
from processors import compute_net_liquidity

st.session_state.current_page = "Liquidity"
st.header("Liquidity")

period = YF_PERIOD_MAP.get(st.session_state.get("date_range", "3Y"), "3y")

# --- Load Data ---
with st.spinner("Loading liquidity data..."):
    fed_bs = get_fred_series(FRED["fed_balance_sheet"])
    rrp = get_fred_series(FRED["rrp"])
    tga = get_fred_series(FRED["tga"])
    m2 = get_fred_series(FRED["m2"])
    sp500 = get_index_data("^GSPC", period)

net_liq = compute_net_liquidity(fed_bs, tga, rrp)

# --- Metric Row ---
metric_row([
    {"label": "Net Liquidity", "value": f"${net_liq.iloc[-1] / 1e6:,.1f}T",
     "delta": f"{(net_liq.iloc[-1] - net_liq.iloc[-6]) / 1e6:+,.1f}T (1W)"},
    {"label": "Fed Balance Sheet", "value": f"${fed_bs.iloc[-1] / 1e6:,.2f}T",
     "delta": f"{(fed_bs.iloc[-1] - fed_bs.iloc[-6]) / 1e6:+,.2f}T (1W)"},
    {"label": "RRP", "value": f"${rrp.iloc[-1] / 1e6:,.2f}T",
     "delta": f"{(rrp.iloc[-1] - rrp.iloc[-6]) / 1e6:+,.2f}T (1W)"},
    {"label": "TGA", "value": f"${tga.iloc[-1] / 1e6:,.2f}T",
     "delta": f"{(tga.iloc[-1] - tga.iloc[-6]) / 1e6:+,.2f}T (1W)"},
])

st.divider()

# --- Net Liquidity vs S&P 500 ---
st.subheader("Net Liquidity vs S&P 500")
# Compute correlation
merged = pd.DataFrame({"Net Liquidity": net_liq, "S&P 500": sp500["Close"]}).dropna()
if len(merged) > 10:
    corr = merged["Net Liquidity"].corr(merged["S&P 500"])
    st.caption(f"Correlation coefficient: **{corr:.3f}**")
else:
    corr = None

st.plotly_chart(
    dual_axis_chart(
        net_liq, sp500["Close"],
        "Net Liquidity ($M)", "S&P 500",
        "Net Liquidity vs S&P 500"
    ),
    use_container_width=True,
)

st.divider()

# --- Fed Balance Sheet Components ---
st.subheader("Fed Balance Sheet Components")
components = pd.DataFrame({
    "Fed B/S": fed_bs,
    "minus TGA": -tga,
    "minus RRP": -rrp,
}).dropna()
st.plotly_chart(
    stacked_area(components, "Fed Balance Sheet Components"),
    use_container_width=True,
)

st.divider()

# --- M2 Money Supply ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("M2 Money Supply (US)")
    m2_df = m2.to_frame(name="M2")
    m2_df["M2 YoY %"] = m2_df["M2"].pct_change(periods=52) * 100  # approx weekly -> annual
    st.plotly_chart(
        dual_axis_chart(
            m2_df["M2"], m2_df["M2 YoY %"],
            "M2 ($B)", "YoY %",
            "M2 Money Supply with YoY Change"
        ),
        use_container_width=True,
    )

with col2:
    st.subheader("Global Central Bank Balance Sheets")
    st.caption("Fed only for MVP — ECB, BOJ, PBOC to be added with live data")
    fed_df = fed_bs.to_frame(name="Federal Reserve")
    st.plotly_chart(
        line_chart(fed_df, "Central Bank Balance Sheets ($M)"),
        use_container_width=True,
    )

# Store summary for Claude
st.session_state.liquidity_summary = {
    "net_liquidity": f"${net_liq.iloc[-1] / 1e6:,.1f}T",
    "fed_bs": f"${fed_bs.iloc[-1] / 1e6:,.2f}T",
    "rrp": f"${rrp.iloc[-1] / 1e6:,.2f}T",
    "tga": f"${tga.iloc[-1] / 1e6:,.2f}T",
    "m2": f"${m2.iloc[-1]:,.0f}B",
    "net_liq_sp_corr": f"{corr:.3f}" if corr else "N/A",
}
