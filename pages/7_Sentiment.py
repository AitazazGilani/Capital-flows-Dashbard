"""
Page 7: Sentiment — VIX, copper/gold ratio, consumer sentiment.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from config import FRED
from data_fetcher import get_volatility, get_commodities, get_fred_series
from chart_helpers import line_chart, dual_axis_chart, metric_row, CHART_TEMPLATE, CHART_MARGINS, CHART_FONT, COLORS
from processors import compute_copper_gold_ratio

st.session_state.current_page = "Sentiment"
st.header("Sentiment")

# --- Load Data ---
with st.spinner("Loading sentiment data..."):
    vol_data = get_volatility()
    comm_data = get_commodities()
    sentiment = get_fred_series(FRED["consumer_sentiment"])

# --- VIX ---
st.subheader("VIX — Equity Volatility")

vix = vol_data["VIX"]
fig_vix = go.Figure()
fig_vix.add_trace(go.Scatter(
    x=vix.index, y=vix.values,
    name="VIX", mode="lines",
    line=dict(color=COLORS[0], width=2),
    fill="tozeroy", fillcolor="rgba(99, 110, 250, 0.1)",
))
# Add bands at 20 and 30
fig_vix.add_hline(y=20, line_dash="dash", line_color="yellow",
                  annotation_text="Elevated (20)", annotation_position="right")
fig_vix.add_hline(y=30, line_dash="dash", line_color="red",
                  annotation_text="High Fear (30)", annotation_position="right")
fig_vix.update_layout(
    title="VIX Index", height=400, template=CHART_TEMPLATE,
    margin=CHART_MARGINS, font=CHART_FONT,
    yaxis_title="VIX",
    hovermode="x unified",
)
st.plotly_chart(fig_vix, use_container_width=True)

# Metrics
vix_last = vix.iloc[-1]
vix_avg = vix.mean()
vix_pctile = (vix < vix_last).mean() * 100

metric_row([
    {"label": "Current VIX", "value": f"{vix_last:.1f}"},
    {"label": "Historical Avg", "value": f"{vix_avg:.1f}"},
    {"label": "Percentile", "value": f"{vix_pctile:.0f}th"},
    {"label": "Signal", "value": "Complacent" if vix_last < 15 else ("Elevated" if vix_last > 25 else "Normal")},
])

st.divider()

# --- Copper/Gold & MOVE ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Copper/Gold Ratio")
    ratio = compute_copper_gold_ratio(comm_data["Copper"], comm_data["Gold"])
    ratio_df = ratio.to_frame()
    st.plotly_chart(
        line_chart(ratio_df, "Copper/Gold Ratio (Risk Appetite)"),
        use_container_width=True,
    )
    st.caption(
        "Rising ratio = risk-on (industrial demand outpacing safe haven). "
        "Falling ratio = risk-off."
    )

with col2:
    st.subheader("MOVE Index — Bond Volatility")
    move = vol_data["MOVE"]
    move_df = move.to_frame(name="MOVE")
    st.plotly_chart(
        line_chart(move_df, "MOVE Index (Bond Volatility)"),
        use_container_width=True,
    )

    # VIX vs MOVE divergence
    st.caption(
        f"VIX/MOVE ratio: {(vix_last / vol_data['MOVE'].iloc[-1]):.2f} — "
        f"{'Equity vol compressed relative to bond vol' if vix_last / vol_data['MOVE'].iloc[-1] < 0.18 else 'Normal relationship'}"
    )

st.divider()

# --- Consumer Sentiment ---
st.subheader("Consumer Sentiment (University of Michigan)")

sent_df = pd.DataFrame({"Sentiment": sentiment})
hist_avg = sentiment.mean()

fig_sent = go.Figure()
fig_sent.add_trace(go.Scatter(
    x=sentiment.index, y=sentiment.values,
    name="U of M Sentiment", mode="lines",
    line=dict(color=COLORS[2], width=2),
))
# Historical average band
fig_sent.add_hline(y=hist_avg, line_dash="dash", line_color="yellow",
                   annotation_text=f"Hist. Avg ({hist_avg:.0f})", annotation_position="right")

fig_sent.update_layout(
    title="Consumer Sentiment with Historical Average",
    height=400, template=CHART_TEMPLATE,
    margin=CHART_MARGINS, font=CHART_FONT,
    yaxis_title="Index",
    hovermode="x unified",
)
st.plotly_chart(fig_sent, use_container_width=True)

sent_last = sentiment.iloc[-1]
sent_pctile = (sentiment < sent_last).mean() * 100
metric_row([
    {"label": "Current", "value": f"{sent_last:.1f}"},
    {"label": "Historical Avg", "value": f"{hist_avg:.1f}"},
    {"label": "Percentile", "value": f"{sent_pctile:.0f}th"},
    {"label": "vs Average", "value": f"{((sent_last / hist_avg) - 1) * 100:+.1f}%"},
])

# Store summary for Claude
st.session_state.sentiment_summary = {
    "vix": f"{vix_last:.1f} ({vix_pctile:.0f}th percentile)",
    "move": f"{vol_data['MOVE'].iloc[-1]:.1f}",
    "copper_gold_ratio": f"{ratio.iloc[-1]:.3f}",
    "consumer_sentiment": f"{sent_last:.1f} ({sent_pctile:.0f}th percentile)",
}
