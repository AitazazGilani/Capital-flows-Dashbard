"""
Page 3: Rates & Credit — Yield curve, rate expectations, credit spreads.
"""

import streamlit as st
import pandas as pd
from src.config import FRED
from src.data_fetcher import get_fred_series, get_fred_multiple, get_yield_curve_snapshot, get_fed_funds_futures
from src.chart_helpers import (
    line_chart, dual_axis_chart, yield_curve_chart,
    step_chart, metric_row,
)
from src.processors import compute_implied_rate_path

st.session_state.current_page = "Rates & Credit"
st.header("Rates & Credit")

# --- Load Data ---
with st.spinner("Loading rates data..."):
    fed_funds = get_fred_series(FRED["fed_funds"])
    us_10y = get_fred_series(FRED["us_10y"])
    us_2y = get_fred_series(FRED["us_2y"])
    us_2s10s = get_fred_series(FRED["us_2s10s"])
    hy_oas = get_fred_series(FRED["hy_oas"])
    ig_oas = get_fred_series(FRED["ig_oas"])
    nfci = get_fred_series(FRED["nfci"])
    real_yield = get_fred_series(FRED["real_yield_10y"])
    breakeven = get_fred_series(FRED["breakeven_10y"])
    yield_curve = get_yield_curve_snapshot()
    futures = get_fed_funds_futures()

# --- Metric Row ---
metric_row([
    {"label": "Fed Funds Rate", "value": f"{fed_funds.iloc[-1]:.2f}%",
     "delta": f"{fed_funds.iloc[-1] - fed_funds.iloc[-6]:+.2f}% (1W)"},
    {"label": "10Y Yield", "value": f"{us_10y.iloc[-1]:.2f}%",
     "delta": f"{us_10y.iloc[-1] - us_10y.iloc[-6]:+.2f}% (1W)"},
    {"label": "2s10s Spread", "value": f"{us_2s10s.iloc[-1]:.2f}%",
     "delta": f"{us_2s10s.iloc[-1] - us_2s10s.iloc[-6]:+.2f}% (1W)"},
    {"label": "HY OAS", "value": f"{hy_oas.iloc[-1]:.0f} bps",
     "delta": f"{(hy_oas.iloc[-1] - hy_oas.iloc[-6]) * 100:+.0f} bps (1W)"},
])

st.divider()

# --- Yield Curve ---
st.subheader("US Treasury Yield Curve")
if not yield_curve.empty:
    st.plotly_chart(
        yield_curve_chart(yield_curve),
        use_container_width=True,
    )
else:
    st.info("Yield curve snapshot data not available. Ingest yield curve data to display.")

st.divider()

# --- Fed Funds Futures Implied Rate Path ---
st.subheader("Fed Funds Futures — Implied Rate Path")
if not futures.empty and not fed_funds.empty:
    implied = compute_implied_rate_path(futures, fed_funds.iloc[-1])

    st.plotly_chart(
        step_chart(
            implied["contract_month"], implied["implied_rate"],
            "Implied Fed Funds Rate Path",
            yaxis_title="Implied Rate (%)"
        ),
        use_container_width=True,
    )

    # Table of implied rates
    st.dataframe(
        implied[["contract_month", "implied_rate", "cuts_25bp"]].rename(columns={
            "contract_month": "Month",
            "implied_rate": "Implied Rate (%)",
            "cuts_25bp": "Cumulative Cuts (25bp)",
        }).set_index("Month"),
        use_container_width=True,
    )
else:
    st.info("Fed funds futures data not available. Ingest futures data to display.")

st.divider()

# --- Credit Spreads & Financial Conditions ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Credit Spreads")
    spreads = pd.DataFrame({
        "HY OAS": hy_oas,
        "IG OAS": ig_oas,
    }).dropna()
    st.plotly_chart(
        line_chart(spreads, "Credit Spreads (OAS, %)"),
        use_container_width=True,
    )

with col2:
    st.subheader("Financial Conditions (NFCI)")
    nfci_df = nfci.to_frame(name="NFCI")
    st.plotly_chart(
        line_chart(nfci_df, "National Financial Conditions Index"),
        use_container_width=True,
    )

st.divider()

# --- Real Yields & Breakevens ---
st.subheader("Real Yields & Breakevens")
st.plotly_chart(
    dual_axis_chart(
        real_yield, breakeven,
        "10Y Real Yield (%)", "10Y Breakeven (%)",
        "Real Yields vs Inflation Breakevens"
    ),
    use_container_width=True,
)

# Store summary for Claude
rates_summary = {}
for name, series in [("fed_funds", fed_funds), ("us_10y", us_10y), ("us_2y", us_2y),
                      ("spread_2s10s", us_2s10s), ("hy_oas", hy_oas), ("ig_oas", ig_oas),
                      ("nfci", nfci), ("real_yield_10y", real_yield), ("breakeven_10y", breakeven)]:
    if not series.empty:
        fmt = ".0f" if name in ("hy_oas", "ig_oas") else ".2f"
        suffix = " bps" if "oas" in name else "%"
        if name == "nfci":
            suffix = ""
        rates_summary[name] = f"{series.iloc[-1]:{fmt}}{suffix}"
if not futures.empty and not fed_funds.empty:
    implied_data = compute_implied_rate_path(futures, fed_funds.iloc[-1])
    rates_summary["rate_cuts_priced_12m"] = f"{implied_data['cuts_25bp'].iloc[-1]:.1f} cuts"
st.session_state.rates_summary = rates_summary
