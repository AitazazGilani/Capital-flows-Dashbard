"""
Page 7: Sentiment — VIX, copper/gold ratio, consumer sentiment.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from src.config import FRED
from src.data_fetcher import get_volatility, get_commodities, get_fred_series, get_cot_data, get_epu_index, get_gpr_index
from src.chart_helpers import line_chart, dual_axis_chart, metric_row, CHART_TEMPLATE, CHART_MARGINS, CHART_FONT, COLORS
from src.processors import compute_copper_gold_ratio

st.session_state.current_page = "Sentiment"
st.header("Sentiment")
st.markdown("""
Measures **fear and greed** across markets using volatility, positioning, and survey data.
Sentiment extremes are contrarian indicators — when everyone is fearful (VIX > 30, positioning very short),
markets often bottom. When complacency reigns (VIX < 15, max long), risk increases. This page combines
real-time volatility, speculative futures positioning (CFTC COT), and policy uncertainty to gauge the
market's emotional state.
""")

# --- Load Data ---
with st.spinner("Loading sentiment data..."):
    vol_data = get_volatility()
    comm_data = get_commodities()
    sentiment = get_fred_series(FRED["consumer_sentiment"])

# --- VIX ---
st.subheader("VIX — Equity Volatility")
st.markdown("The S&P 500 fear gauge. **Below 15** = complacency (potential for a sharp move higher in vol). **20-25** = elevated caution. **Above 30** = high fear, historically a contrarian buy signal. The percentile rank tells you how current vol compares to history.")

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
    st.markdown("Copper = industrial demand, Gold = fear. **Rising ratio = risk-on** (growth optimism). **Falling ratio = risk-off** (flight to safety). Often leads equity moves by 2-4 weeks.")
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
    st.markdown("Treasury market volatility. When MOVE spikes **before** VIX, it signals a rates-driven selloff (more dangerous). When VIX spikes alone, it's equity-specific. A low VIX/MOVE ratio means equity vol is compressed relative to bond vol — watch for catch-up.")
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
st.markdown("How consumers feel about the economy and their finances. Sentiment leads spending — when it collapses, retail and discretionary stocks follow. Readings **below the historical average** suggest pessimism; extreme lows can be contrarian buy signals.")

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

st.divider()

# --- CFTC COT Positioning ---
st.subheader("Futures Positioning (CFTC COT)")
st.markdown("Shows how **speculators** (hedge funds, CTAs) are positioned in futures markets. Net long = bullish consensus, net short = bearish. **Extreme positioning** often precedes reversals — when everyone is on the same side of the trade, the unwind can be violent. % Long above 70% or below 30% = crowded.")

cot_fx = get_cot_data("fx")
cot_rates = get_cot_data("rates")
cot_commodities = get_cot_data("commodities")

if not cot_fx.empty or not cot_rates.empty or not cot_commodities.empty:
    cot_tab1, cot_tab2, cot_tab3 = st.tabs(["FX", "Rates", "Commodities"])

    for tab, cot_df, label in [
        (cot_tab1, cot_fx, "FX"),
        (cot_tab2, cot_rates, "Rates"),
        (cot_tab3, cot_commodities, "Commodities"),
    ]:
        with tab:
            if not cot_df.empty:
                # Show latest net positioning per contract
                latest = cot_df.sort_values("date").groupby("contract").last().reset_index()
                fig_cot = go.Figure(go.Bar(
                    x=latest["contract"],
                    y=latest["net"],
                    marker_color=[COLORS[2] if v > 0 else COLORS[1] for v in latest["net"]],
                    text=[f"{v:,.0f}" for v in latest["net"]],
                    textposition="outside",
                ))
                fig_cot.update_layout(
                    title=f"{label} Net Speculative Positioning",
                    height=350, template=CHART_TEMPLATE, margin=CHART_MARGINS, font=CHART_FONT,
                    yaxis_title="Net Contracts (Long - Short)",
                )
                st.plotly_chart(fig_cot, use_container_width=True)

                # Show % long for context
                st.dataframe(
                    latest[["contract", "long", "short", "net", "pct_long"]].rename(columns={
                        "contract": "Contract", "long": "Long", "short": "Short",
                        "net": "Net", "pct_long": "% Long",
                    }).set_index("Contract"),
                    use_container_width=True,
                )
            else:
                st.info(f"No {label} COT data available.")

    st.caption(
        "Source: CFTC Commitments of Traders (Legacy Futures). "
        "Net = Non-commercial long - short. Extreme positioning often precedes reversals."
    )
else:
    st.info("CFTC COT data not available. Run `python ingestor.py --source cftc` to ingest.")

st.divider()

# --- Geopolitical & Policy Uncertainty ---
st.subheader("Geopolitical & Policy Risk")
st.markdown("Two complementary measures of uncertainty. **EPU** tracks economic policy uncertainty (taxes, regulation, trade policy). **GPR** tracks geopolitical threats and military actions. Spikes in both indices correlate with risk-off moves, VIX increases, and flight to safe havens (USD, gold, Treasuries).")

epu = get_epu_index()
gpr_df = get_gpr_index()

col1, col2 = st.columns(2)

with col1:
    if not epu.empty:
        epu_frame = epu.to_frame(name="EPU Index")
        fig_epu = go.Figure()
        fig_epu.add_trace(go.Scatter(
            x=epu.index, y=epu.values,
            name="EPU", mode="lines",
            line=dict(color=COLORS[3], width=1.5),
            fill="tozeroy", fillcolor="rgba(171, 99, 250, 0.1)",
        ))
        epu_avg = epu.mean()
        fig_epu.add_hline(y=epu_avg, line_dash="dash", line_color="yellow",
                          annotation_text=f"Avg ({epu_avg:.0f})", annotation_position="right")
        fig_epu.update_layout(
            title="Economic Policy Uncertainty Index (US)",
            height=400, template=CHART_TEMPLATE, margin=CHART_MARGINS, font=CHART_FONT,
            yaxis_title="EPU Index",
            hovermode="x unified",
        )
        st.plotly_chart(fig_epu, use_container_width=True)
    else:
        st.info("EPU Index not available. Ingest FRED data with FRED_API_KEY.")

with col2:
    if not gpr_df.empty:
        gpr_series = gpr_df["GPR"] if "GPR" in gpr_df.columns else gpr_df.iloc[:, 0]
        fig_gpr = go.Figure()
        fig_gpr.add_trace(go.Scatter(
            x=gpr_df.index, y=gpr_series.values,
            name="GPR", mode="lines",
            line=dict(color=COLORS[1], width=1.5),
            fill="tozeroy", fillcolor="rgba(239, 85, 59, 0.1)",
        ))
        gpr_avg = gpr_series.mean()
        fig_gpr.add_hline(y=gpr_avg, line_dash="dash", line_color="yellow",
                          annotation_text=f"Avg ({gpr_avg:.0f})", annotation_position="right")
        fig_gpr.update_layout(
            title="Geopolitical Risk Index (Caldara-Iacoviello)",
            height=400, template=CHART_TEMPLATE, margin=CHART_MARGINS, font=CHART_FONT,
            yaxis_title="GPR Index",
            hovermode="x unified",
        )
        st.plotly_chart(fig_gpr, use_container_width=True)
    else:
        st.info("GPR Index not available. Run `python ingestor.py --source fred` to ingest.")

st.caption(
    "EPU measures newspaper coverage of policy-related economic uncertainty. "
    "GPR measures geopolitical threats and actions. Spikes in both often precede risk-off moves."
)

# Store summary for Claude
st.session_state.sentiment_summary = {
    "vix": f"{vix_last:.1f} ({vix_pctile:.0f}th percentile)",
    "move": f"{vol_data['MOVE'].iloc[-1]:.1f}",
    "copper_gold_ratio": f"{ratio.iloc[-1]:.3f}",
    "consumer_sentiment": f"{sent_last:.1f} ({sent_pctile:.0f}th percentile)",
    "epu": f"{epu.iloc[-1]:.0f}" if not epu.empty else "N/A",
    "gpr": f"{gpr_df.iloc[-1, 0]:.0f}" if not gpr_df.empty else "N/A",
}
