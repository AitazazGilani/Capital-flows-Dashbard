"""
Page 10: Strategic Sectors — Semiconductor industry deep-dive.
Tracks SOX vs market, key stocks, revenue/inventory cycles, supply chain policy.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from src.config import SEMI_TICKERS, SEMI_ETFS
from src.data_fetcher import (
    get_semi_stocks, get_semi_etfs, get_semi_vs_market,
    get_semi_revenue_cycle, get_semi_inventory_cycle,
    get_policy_events,
)
from src.chart_helpers import (
    line_chart, dual_axis_chart, bar_chart, metric_row,
    CHART_TEMPLATE, CHART_MARGINS, CHART_FONT, COLORS,
)
from src.processors import compute_semi_relative_strength

st.session_state.current_page = "Strategic Sectors"
st.header("Strategic Sectors: Semiconductors")
st.caption("The semiconductor cycle drives capex, trade policy, and geopolitical tension — making it a key macro input")

# ===================================================================
# SECTION 1: SOX vs S&P 500 Relative Performance
# ===================================================================
st.subheader("Semiconductors vs Broad Market")

with st.spinner("Loading semiconductor data..."):
    semi_vs_mkt = get_semi_vs_market()
    semi_stocks = get_semi_stocks()
    semi_etf = get_semi_etfs()

# Metrics
sox_last = semi_vs_mkt["SOX (Semis)"].iloc[-1]
sox_1m = (semi_vs_mkt["SOX (Semis)"].iloc[-1] / semi_vs_mkt["SOX (Semis)"].iloc[-22] - 1) * 100
sp_last = semi_vs_mkt["S&P 500"].iloc[-1]
sp_1m = (semi_vs_mkt["S&P 500"].iloc[-1] / semi_vs_mkt["S&P 500"].iloc[-22] - 1) * 100

metric_row([
    {"label": "SOX Index", "value": f"{sox_last:,.0f}", "delta": f"{sox_1m:+.1f}% (1M)"},
    {"label": "S&P 500", "value": f"{sp_last:,.0f}", "delta": f"{sp_1m:+.1f}% (1M)"},
    {"label": "SOX Outperformance", "value": f"{sox_1m - sp_1m:+.1f}% (1M)"},
    {"label": "Signal", "value": "Outperforming" if sox_1m > sp_1m else "Underperforming"},
])

# Normalized performance chart
col1, col2 = st.columns(2)

with col1:
    st.plotly_chart(
        line_chart(semi_vs_mkt, "SOX vs S&P 500 (Indexed to 100)", normalize=True),
        use_container_width=True,
    )

with col2:
    # Relative strength line
    rel_strength = compute_semi_relative_strength(
        semi_vs_mkt["SOX (Semis)"], semi_vs_mkt["S&P 500"]
    )
    rs_df = rel_strength.to_frame()

    fig_rs = go.Figure()
    fig_rs.add_trace(go.Scatter(
        x=rs_df.index, y=rs_df.iloc[:, 0],
        mode="lines", name="Relative Strength",
        line=dict(color=COLORS[0], width=2),
        fill="tozeroy", fillcolor="rgba(99, 110, 250, 0.1)",
    ))
    fig_rs.add_hline(y=100, line_dash="dash", line_color="yellow",
                     annotation_text="Parity", annotation_position="right")
    fig_rs.update_layout(
        title="Semi vs Market Relative Strength (100 = parity)",
        height=400, template=CHART_TEMPLATE, margin=CHART_MARGINS, font=CHART_FONT,
        yaxis_title="Relative Strength",
        hovermode="x unified",
    )
    st.plotly_chart(fig_rs, use_container_width=True)

st.caption(
    "Rising relative strength = semis outperforming the broad market. "
    "Semis tend to lead the cycle — if relative strength breaks down, it often "
    "precedes broader market weakness by 2-3 months."
)

st.divider()

# ===================================================================
# SECTION 2: Key Semiconductor Stocks
# ===================================================================
st.subheader("Key Semiconductor Stocks")

# Performance table
perf_data = {}
for label in semi_stocks.columns:
    series = semi_stocks[label].dropna()
    if len(series) < 63:
        continue
    perf_data[label] = {
        "Price": f"${series.iloc[-1]:,.1f}",
        "1D %": f"{(series.iloc[-1] / series.iloc[-2] - 1) * 100:+.2f}",
        "1W %": f"{(series.iloc[-1] / series.iloc[-6] - 1) * 100:+.2f}" if len(series) > 5 else "N/A",
        "1M %": f"{(series.iloc[-1] / series.iloc[-22] - 1) * 100:+.2f}" if len(series) > 21 else "N/A",
        "3M %": f"{(series.iloc[-1] / series.iloc[-63] - 1) * 100:+.2f}" if len(series) > 62 else "N/A",
        "YTD %": f"{(series.iloc[-1] / series.iloc[max(0, len(series)-252)] - 1) * 100:+.2f}",
    }

perf_df = pd.DataFrame(perf_data).T
st.dataframe(perf_df, use_container_width=True)

# Normalized stock chart
normalize_toggle = st.toggle("Normalize to 100", value=True, key="semi_normalize")
st.plotly_chart(
    line_chart(semi_stocks, "Semiconductor Stocks", normalize=normalize_toggle),
    use_container_width=True,
)

st.divider()

# ===================================================================
# SECTION 3: Semiconductor Revenue & Inventory Cycle
# ===================================================================
st.subheader("Semiconductor Industry Cycle")

with st.spinner("Loading industry cycle data..."):
    revenue_cycle = get_semi_revenue_cycle()
    inventory_cycle = get_semi_inventory_cycle()

col1, col2 = st.columns(2)

with col1:
    st.markdown("**Global Semi Revenue (Quarterly)**")
    fig_rev = go.Figure()
    fig_rev.add_trace(go.Bar(
        x=revenue_cycle.index,
        y=revenue_cycle["Global Semi Revenue ($B)"],
        name="Revenue ($B)",
        marker_color=COLORS[0],
    ))
    fig_rev.add_trace(go.Scatter(
        x=revenue_cycle.index,
        y=revenue_cycle["QoQ Change (%)"],
        name="QoQ Change (%)",
        mode="lines+markers",
        line=dict(color=COLORS[1], width=2),
        yaxis="y2",
    ))
    fig_rev.update_layout(
        title="Global Semiconductor Revenue",
        height=400, template=CHART_TEMPLATE, margin=CHART_MARGINS, font=CHART_FONT,
        yaxis=dict(title="Revenue ($B)"),
        yaxis2=dict(title="QoQ %", side="right", overlaying="y"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig_rev, use_container_width=True)
    st.caption("Source: SIA (Semiconductor Industry Association) — free quarterly reports")

with col2:
    st.markdown("**Book-to-Bill & Inventory**")
    fig_inv = go.Figure()
    fig_inv.add_trace(go.Scatter(
        x=inventory_cycle.index,
        y=inventory_cycle["Book-to-Bill"],
        name="Book-to-Bill",
        mode="lines",
        line=dict(color=COLORS[2], width=2),
    ))
    fig_inv.add_hline(y=1.0, line_dash="dash", line_color="yellow",
                      annotation_text="Expansion/Contraction", annotation_position="right")
    fig_inv.add_trace(go.Scatter(
        x=inventory_cycle.index,
        y=inventory_cycle["Inventory Days"],
        name="Inventory Days",
        mode="lines",
        line=dict(color=COLORS[3], width=2),
        yaxis="y2",
    ))
    fig_inv.update_layout(
        title="Semi Book-to-Bill & Inventory Days",
        height=400, template=CHART_TEMPLATE, margin=CHART_MARGINS, font=CHART_FONT,
        yaxis=dict(title="Book-to-Bill Ratio"),
        yaxis2=dict(title="Inventory Days", side="right", overlaying="y"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    st.plotly_chart(fig_inv, use_container_width=True)
    st.caption("Book-to-Bill > 1.0 = orders exceeding shipments (expansion). High inventory days = glut risk.")

# Cycle interpretation
btb_last = inventory_cycle["Book-to-Bill"].iloc[-1]
inv_last = inventory_cycle["Inventory Days"].iloc[-1]

if btb_last > 1.05 and inv_last < 90:
    cycle_phase = "Early Upcycle"
    cycle_color = "green"
    cycle_desc = "Orders are outpacing shipments while inventories are lean. Best phase for semi stocks."
elif btb_last > 1.0 and inv_last > 100:
    cycle_phase = "Late Upcycle"
    cycle_color = "orange"
    cycle_desc = "Demand still positive but inventories building. Watch for order corrections."
elif btb_last < 1.0 and inv_last > 100:
    cycle_phase = "Downcycle"
    cycle_color = "red"
    cycle_desc = "Orders below shipments with elevated inventory. Semi stocks typically bottom before B2B troughs."
else:
    cycle_phase = "Recovery"
    cycle_color = "blue"
    cycle_desc = "Inventories normalizing, B2B stabilizing. Early positioning opportunity."

st.markdown(f"**Current cycle phase:** :{cycle_color}[{cycle_phase}]")
st.markdown(f"*{cycle_desc}*")

st.divider()

# ===================================================================
# SECTION 4: Semiconductor ETFs
# ===================================================================
st.subheader("Semiconductor ETFs")
st.plotly_chart(
    line_chart(semi_etf, "Semi ETFs (Indexed)", normalize=True),
    use_container_width=True,
)

st.divider()

# ===================================================================
# SECTION 5: Semi-Relevant Policy Events
# ===================================================================
st.subheader("Policy Events Affecting Semiconductors")

events = get_policy_events()
semi_events = events[events["sectors"].str.contains("Semi", case=False, na=False)]

if not semi_events.empty:
    for _, row in semi_events.iterrows():
        impact = row["impact"]
        color = "🟢" if impact == "Positive" else ("🔴" if impact == "Negative" else "🟡")
        with st.expander(f"{color} **{row['date'].strftime('%Y-%m-%d')}** | {row['country']} — {row['event']}"):
            st.markdown(f"**Category:** {row['category']}")
            st.markdown(f"**Detail:** {row['detail']}")
else:
    st.info("No semiconductor-specific policy events found.")

st.divider()

# ===================================================================
# SECTION 6: Investment Framework
# ===================================================================
st.subheader("Semi Sector Investment Framework")
st.markdown("""
**How to read this page for investment decisions:**

1. **Relative Strength** — If SOX relative strength is rising, stay overweight semis.
   If breaking down, reduce exposure before broad market follows.

2. **Cycle Phase** — The book-to-bill ratio is the single best leading indicator.
   B2B > 1.05 with low inventory = aggressive long. B2B < 0.95 = wait for trough.

3. **Revenue Trend** — QoQ revenue growth inflecting positive from negative is
   historically the best entry point for the sector.

4. **Policy Overlay** — Export controls create winners (US/allied equipment makers)
   and losers (China-exposed revenue). CHIPS Act subsidies are a multi-year tailwind
   for domestic capacity.

5. **Key Risks** — Taiwan concentration risk, inventory correction overshoot,
   China retaliation on critical minerals.

**Data sources for live implementation:**
- **yfinance** (free, no key) — All stock/ETF price data
- **SIA** (free) — Quarterly revenue and B2B data via web scraping
- **SEMI** (free reports) — Equipment billings and fab utilization
- **WSTS** (free) — World Semiconductor Trade Statistics forecasts
""")

# Store summary for Claude
st.session_state.semi_summary = {
    "sox_level": f"{sox_last:,.0f}",
    "sox_1m_return": f"{sox_1m:+.1f}%",
    "sox_vs_sp": f"{sox_1m - sp_1m:+.1f}% outperformance (1M)",
    "cycle_phase": cycle_phase,
    "book_to_bill": f"{btb_last:.2f}",
    "inventory_days": f"{inv_last:.0f}",
    "semi_policy_events": len(semi_events),
}
