"""
Page 9: Policy & Geopolitical Tracker — Trade policy, central bank calendar,
export controls, industrial policy, and their market impact.

This page tracks macro catalysts that move capital flows and markets.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from src.data_fetcher import get_policy_events, get_central_bank_calendar, get_tariff_tracker
from src.chart_helpers import (
    CHART_TEMPLATE, CHART_MARGINS, CHART_FONT, COLORS, metric_row,
)

st.session_state.current_page = "Policy Tracker"
st.header("Policy & Geopolitical Tracker")
st.markdown("""
Tracks **macro catalysts** that move markets and redirect capital flows. Trade policy (tariffs, sanctions),
central bank decisions (rate changes, QE/QT), and geopolitical events (conflicts, elections) are the
primary drivers of regime changes in markets. This page aggregates policy events, classifies their
likely market impact, and tracks the central bank calendar so you're never caught off guard.
""")

# ===================================================================
# SECTION 1: Key Metrics — Recent Policy Pulse
# ===================================================================
events = get_policy_events()

# Count events in last 30/90 days
now = pd.Timestamp("2026-02-08")
last_30 = events[events["date"] >= now - pd.Timedelta(days=30)]
last_90 = events[events["date"] >= now - pd.Timedelta(days=90)]
neg_90 = last_90[last_90["impact"] == "Negative"]
pos_90 = last_90[last_90["impact"] == "Positive"]

metric_row([
    {"label": "Events (30d)", "value": str(len(last_30))},
    {"label": "Events (90d)", "value": str(len(last_90))},
    {"label": "Bullish (90d)", "value": str(len(pos_90)), "delta": f"{len(pos_90)}/{len(last_90)}"},
    {"label": "Bearish (90d)", "value": str(len(neg_90)), "delta": f"{len(neg_90)}/{len(last_90)}"},
])

st.divider()

# ===================================================================
# SECTION 2: Policy Event Timeline
# ===================================================================
st.subheader("Policy Event Timeline")
st.markdown("Chronological feed of policy actions with impact classification. Use the filters to focus on specific categories, countries, or impact types. Click any event to see sector exposure and detailed analysis.")

# Filters
col_f1, col_f2, col_f3 = st.columns(3)
with col_f1:
    category_filter = st.multiselect(
        "Category",
        options=sorted(events["category"].unique()),
        default=sorted(events["category"].unique()),
    )
with col_f2:
    country_filter = st.multiselect(
        "Country",
        options=sorted(events["country"].unique()),
        default=sorted(events["country"].unique()),
    )
with col_f3:
    impact_filter = st.multiselect(
        "Impact",
        options=["Positive", "Negative", "Mixed", "Neutral"],
        default=["Positive", "Negative", "Mixed", "Neutral"],
    )

filtered = events[
    events["category"].isin(category_filter) &
    events["country"].isin(country_filter) &
    events["impact"].isin(impact_filter)
]

# Display as styled timeline
for _, row in filtered.iterrows():
    impact = row["impact"]
    if impact == "Positive":
        color = "🟢"
    elif impact == "Negative":
        color = "🔴"
    elif impact == "Mixed":
        color = "🟡"
    else:
        color = "⚪"

    with st.expander(f"{color} **{row['date'].strftime('%Y-%m-%d')}** | {row['category']} | {row['country']} — {row['event']}"):
        st.markdown(f"**Sectors affected:** {row['sectors']}")
        st.markdown(f"**Detail:** {row['detail']}")
        st.markdown(f"**Impact assessment:** {impact}")

st.divider()

# ===================================================================
# SECTION 3: Policy Impact by Category (visual)
# ===================================================================
st.subheader("Event Distribution")
st.markdown("Visual breakdown of policy activity. A skew toward **Negative events** suggests a challenging macro environment for risk assets. High concentration in one category (e.g., Trade Policy) means that sector is the primary source of uncertainty.")

col1, col2 = st.columns(2)

with col1:
    # Events by category
    cat_counts = events["category"].value_counts()
    fig_cat = go.Figure(go.Bar(
        x=cat_counts.values,
        y=cat_counts.index,
        orientation="h",
        marker_color=COLORS[:len(cat_counts)],
    ))
    fig_cat.update_layout(
        title="Events by Category",
        height=350, template=CHART_TEMPLATE, margin=CHART_MARGINS, font=CHART_FONT,
        xaxis_title="Count",
    )
    st.plotly_chart(fig_cat, use_container_width=True)

with col2:
    # Impact distribution
    impact_counts = events["impact"].value_counts()
    colors_map = {"Positive": "#00CC96", "Negative": "#EF553B", "Mixed": "#FFA15A", "Neutral": "#636EFA"}
    fig_impact = go.Figure(go.Pie(
        labels=impact_counts.index,
        values=impact_counts.values,
        marker=dict(colors=[colors_map.get(i, "#888") for i in impact_counts.index]),
        hole=0.4,
    ))
    fig_impact.update_layout(
        title="Impact Distribution",
        height=350, template=CHART_TEMPLATE, margin=CHART_MARGINS, font=CHART_FONT,
    )
    st.plotly_chart(fig_impact, use_container_width=True)

st.divider()

# ===================================================================
# SECTION 4: Central Bank Calendar
# ===================================================================
st.subheader("Central Bank Meeting Calendar")
st.markdown("Upcoming rate decisions and market expectations. Central bank surprises are among the largest market catalysts.")

cb_calendar = get_central_bank_calendar()

# Highlight upcoming meetings
for _, row in cb_calendar.iterrows():
    days_until = (row["date"] - now).days
    if days_until < 0:
        continue

    urgency = "🔴" if days_until <= 7 else ("🟡" if days_until <= 30 else "🟢")
    action_color = "green" if "Cut" in row["expected_action"] else ("red" if "Hike" in row["expected_action"] else "gray")

    st.markdown(
        f"{urgency} **{row['date'].strftime('%b %d')}** — "
        f"**{row['bank']}** ({row['country']}) | "
        f"Current: {row['current_rate']:.2f}% | "
        f"Expected: :{action_color}[{row['expected_action']}] | "
        f"Probability: {row['market_probability']}"
    )

st.caption(
    "Market impact is driven by the *surprise* component — the difference between the actual "
    "decision and what was priced in. A 'hold' when cuts are priced at 80% is effectively hawkish."
)

st.divider()

# ===================================================================
# SECTION 5: Tariff & Trade Policy Tracker
# ===================================================================
st.subheader("US Tariff & Trade Policy Tracker")
st.markdown("Current vs pre-2025 tariff rates by sector. **Red-highlighted rates** have increased. Rising tariffs are inflationary (higher import costs), disruptive to supply chains, and trigger retaliation — all of which redirect capital flows. Sectors with the biggest rate increases face the most disruption.")

tariff_data = get_tariff_tracker()

# Style the table
def highlight_tariff_changes(row):
    styles = [""] * len(row)
    try:
        current = row["US Tariff Rate (%)"]
        previous = row["Pre-2025 Rate (%)"]
        if current > previous:
            styles[2] = "color: #EF553B; font-weight: bold"  # Current rate in red if increased
    except (KeyError, TypeError):
        pass
    return styles

styled_tariff = tariff_data.style.apply(highlight_tariff_changes, axis=1)
st.dataframe(styled_tariff, use_container_width=True, hide_index=True)

# Tariff escalation visual
fig_tariff = go.Figure()
fig_tariff.add_trace(go.Bar(
    x=tariff_data["Sector"],
    y=tariff_data["Pre-2025 Rate (%)"],
    name="Pre-2025 Rate",
    marker_color=COLORS[0],
))
fig_tariff.add_trace(go.Bar(
    x=tariff_data["Sector"],
    y=tariff_data["US Tariff Rate (%)"],
    name="Current Rate",
    marker_color=COLORS[1],
))
fig_tariff.update_layout(
    title="Tariff Escalation by Sector (%)",
    height=400, template=CHART_TEMPLATE, margin=CHART_MARGINS, font=CHART_FONT,
    barmode="group",
    yaxis_title="Tariff Rate (%)",
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
)
st.plotly_chart(fig_tariff, use_container_width=True)

st.divider()

# ===================================================================
# SECTION 6: Key Themes & Investment Implications
# ===================================================================
st.subheader("Key Macro Themes")
st.markdown("The structural narratives shaping capital allocation over months to years. Each theme has an associated status and investment direction. **Escalating/Elevated** themes require active risk management. **Improving** themes create opportunities.")

themes = [
    {
        "theme": "US-China Tech Decoupling",
        "status": "Escalating",
        "description": "Export controls expanding from leading-edge to mature-node chips and AI compute. "
                       "China accelerating domestic substitution. Two-track semiconductor ecosystem emerging.",
        "sectors": "Semiconductors, AI, Cloud",
        "direction": "Negative for global semi supply chains, positive for US/allied fab buildout",
    },
    {
        "theme": "Global Tariff Escalation",
        "status": "Active",
        "description": "Broad-based tariff increases across trading partners. Retaliation cycles underway. "
                       "Supply chain diversification (friend-shoring) accelerating.",
        "sectors": "Industrials, Consumer, Agriculture",
        "direction": "Inflationary, negative for trade-dependent economies, positive for domestic producers",
    },
    {
        "theme": "Synchronized Rate Cutting Cycle",
        "status": "Paused",
        "description": "ECB, BOE cutting. Fed paused on tariff inflation concerns. BOJ normalizing. "
                       "Divergent paths create FX volatility and carry trade opportunities.",
        "sectors": "Bonds, FX, Real Estate",
        "direction": "Eventually bullish for risk assets, but timing uncertain due to inflation risks",
    },
    {
        "theme": "Industrial Policy Renaissance",
        "status": "Ongoing",
        "description": "CHIPS Act, IRA, EU Green Deal, China Big Fund III. Governments directing capital "
                       "into strategic sectors at unprecedented scale.",
        "sectors": "Semiconductors, Clean Energy, Defense",
        "direction": "Positive for subsidy recipients, distortive for global competition",
    },
    {
        "theme": "Taiwan Strait Risk Premium",
        "status": "Elevated",
        "description": "Military exercises and diplomatic tensions keeping geopolitical risk elevated. "
                       "Semiconductor supply concentration in Taiwan remains a systemic vulnerability.",
        "sectors": "Semiconductors, Defense, Shipping",
        "direction": "Tail risk — low probability but extreme impact. Supports supply chain diversification thesis",
    },
]

for t in themes:
    status_color = "red" if t["status"] in ["Escalating", "Elevated"] else (
        "green" if t["status"] in ["Improving", "Resolved"] else "orange"
    )
    with st.expander(f"**{t['theme']}** — :{status_color}[{t['status']}]"):
        st.markdown(f"**Description:** {t['description']}")
        st.markdown(f"**Sectors:** {t['sectors']}")
        st.markdown(f"**Investment direction:** {t['direction']}")

st.markdown("""
---
**Data sources for live implementation:**
- **Federal Register API** (free, no key) — US executive orders, rules, regulations
- **GDELT Project** (free, no key) — Global events, tone analysis, geographic coding
- **ProPublica Congress API** (free, key required) — US legislation tracking
- **Trade.gov** (free, no key) — Tariff and trade policy data
- **Central bank websites** (free) — Meeting schedules and statements
""")

# Store summary for Claude
st.session_state.policy_summary = {
    "events_30d": len(last_30),
    "events_90d": len(last_90),
    "bullish_90d": len(pos_90),
    "bearish_90d": len(neg_90),
    "key_themes": [t["theme"] + " (" + t["status"] + ")" for t in themes],
}
