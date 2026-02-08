"""
Page 1: Markets — Equity indices, FX, commodities, volatility.
"""

import streamlit as st
import pandas as pd
from src.config import COUNTRIES, YF_PERIOD_MAP
from src.data_fetcher import (
    get_index_data, get_multiple_tickers, get_fx_rates,
    get_dxy, get_commodities, get_volatility,
)
from src.chart_helpers import (
    line_chart, dual_axis_chart, metric_row, heatmap,
)
from src.processors import compute_copper_gold_ratio

st.session_state.current_page = "Markets"
st.header("Markets")

selected = st.session_state.get("selected_countries", ["US", "EU", "UK", "JP", "CN"])
period = YF_PERIOD_MAP.get(st.session_state.get("date_range", "3Y"), "3y")

# --- Metric Row ---
with st.spinner("Loading market data..."):
    sp500 = get_index_data("^GSPC", period)
    dxy_data = get_dxy(period)
    vol_data = get_volatility(period)
    comm_data = get_commodities(period)

sp_last = sp500["Close"].iloc[-1]
sp_chg = (sp500["Close"].iloc[-1] / sp500["Close"].iloc[-2] - 1) * 100
dxy_last = dxy_data["Close"].iloc[-1]
dxy_chg = (dxy_data["Close"].iloc[-1] / dxy_data["Close"].iloc[-2] - 1) * 100
vix_last = vol_data["VIX"].iloc[-1]
vix_chg = vol_data["VIX"].iloc[-1] - vol_data["VIX"].iloc[-2]
gold_last = comm_data["Gold"].iloc[-1]
gold_chg = (comm_data["Gold"].iloc[-1] / comm_data["Gold"].iloc[-2] - 1) * 100

metric_row([
    {"label": "S&P 500", "value": f"{sp_last:,.0f}", "delta": f"{sp_chg:+.2f}%"},
    {"label": "DXY", "value": f"{dxy_last:.2f}", "delta": f"{dxy_chg:+.2f}%"},
    {"label": "VIX", "value": f"{vix_last:.1f}", "delta": f"{vix_chg:+.1f}"},
    {"label": "Gold", "value": f"${gold_last:,.0f}", "delta": f"{gold_chg:+.2f}%"},
])

st.divider()

# --- Equity Indices ---
st.subheader("Equity Indices")
normalize = st.toggle("Normalize to 100", value=True, key="mkt_normalize")

index_tickers = [COUNTRIES[c]["index"] for c in selected]
index_names = {COUNTRIES[c]["index"]: f"{c} ({COUNTRIES[c]['index']})" for c in selected}

with st.spinner("Loading equity indices..."):
    indices_df = get_multiple_tickers(index_tickers, period)
    indices_df.columns = [index_names.get(c, c) for c in indices_df.columns]

st.plotly_chart(
    line_chart(indices_df, "Equity Indices", normalize=normalize),
    use_container_width=True,
)

st.divider()

# --- FX & DXY ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("FX Rates — % Change")
    with st.spinner("Loading FX data..."):
        fx_df = get_fx_rates(selected, period)
    if not fx_df.empty:
        # Compute percentage changes
        pct_changes = pd.DataFrame(index=fx_df.columns)
        pct_changes["1D %"] = ((fx_df.iloc[-1] / fx_df.iloc[-2] - 1) * 100).values
        pct_changes["1W %"] = ((fx_df.iloc[-1] / fx_df.iloc[-6] - 1) * 100).values if len(fx_df) > 5 else 0
        pct_changes["1M %"] = ((fx_df.iloc[-1] / fx_df.iloc[-22] - 1) * 100).values if len(fx_df) > 21 else 0
        st.plotly_chart(
            heatmap(pct_changes, "FX % Changes", fmt=".2f"),
            use_container_width=True,
        )
    else:
        st.info("No FX data for selected countries.")

with col2:
    st.subheader("DXY Index")
    # Add 200-day MA
    dxy_plot = dxy_data[["Close"]].copy()
    dxy_plot.columns = ["DXY"]
    dxy_plot["200D MA"] = dxy_plot["DXY"].rolling(200).mean()
    st.plotly_chart(
        line_chart(dxy_plot, "DXY with 200-Day MA"),
        use_container_width=True,
    )

st.divider()

# --- Commodities & Volatility ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Commodities")
    st.plotly_chart(
        line_chart(comm_data, "Commodities", normalize=True),
        use_container_width=True,
    )

with col2:
    st.subheader("Volatility")
    st.plotly_chart(
        dual_axis_chart(vol_data["VIX"], vol_data["MOVE"], "VIX", "MOVE", "VIX vs MOVE Index"),
        use_container_width=True,
    )

# --- Copper/Gold Ratio ---
st.subheader("Copper/Gold Ratio")
ratio = compute_copper_gold_ratio(comm_data["Copper"], comm_data["Gold"])
ratio_df = ratio.to_frame()
st.plotly_chart(
    line_chart(ratio_df, "Copper/Gold Ratio (Risk Appetite Indicator)"),
    use_container_width=True,
)

# Store summary for Claude chat context
st.session_state.market_summary = {
    "sp500": f"{sp_last:,.0f} ({sp_chg:+.2f}%)",
    "dxy": f"{dxy_last:.2f} ({dxy_chg:+.2f}%)",
    "vix": f"{vix_last:.1f}",
    "gold": f"${gold_last:,.0f} ({gold_chg:+.2f}%)",
    "copper_gold_ratio": f"{ratio.iloc[-1]:.3f}",
}
