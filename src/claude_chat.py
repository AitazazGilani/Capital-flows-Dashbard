"""
Claude AI chat integration for the macro dashboard sidebar.
"""

import json
import streamlit as st


SYSTEM_PROMPT = """You are a macro analyst embedded in a capital flow dashboard.
You have access to the current dashboard data provided as context.

Focus on:
- Capital flow analysis and what drives them
- How monetary policy and liquidity affect capital movements
- Connecting indicators to equity market implications
- Identifying divergences and emerging trends across countries

Reference specific numbers from the context. Be direct and analytical.
You are NOT a financial advisor — frame everything as analysis, not recommendations."""

SUGGESTED_PROMPTS = {
    "Markets": [
        "What's the DXY telling us about capital flows?",
        "Is copper/gold signaling risk-on or risk-off?",
    ],
    "Liquidity": [
        "How does net liquidity compare to prior equity tops?",
        "What happens when RRP drains fully?",
    ],
    "Rates & Credit": [
        "What are futures pricing for rate cuts this year?",
        "Are credit spreads consistent with equity valuations?",
    ],
    "Economy": [
        "Are jobless claims trending recessionary?",
        "What does the LEI trajectory suggest for equities?",
    ],
    "Capital Flows": [
        "Which countries are seeing capital flight?",
        "How do rate differentials map to flow direction?",
        "Where are the biggest current account divergences?",
    ],
    "Country Risk": [
        "Which countries have the weakest reserve cover?",
        "Compare debt sustainability across selected countries.",
    ],
    "Sentiment": [
        "Is sentiment at a contrarian extreme?",
        "What does the VIX term structure imply?",
    ],
    "Cross-Asset Signals": [
        "Which countries offer the best risk-reward right now?",
        "Where is carry trade most attractive and most dangerous?",
        "What does the relative value matrix say about EM vs DM?",
    ],
    "Policy Tracker": [
        "What are the biggest policy catalysts in the next 3 months?",
        "How do tariff escalations affect capital flows?",
        "Which central bank decisions will surprise the market?",
    ],
    "Strategic Sectors": [
        "Where are we in the semiconductor cycle?",
        "Is SOX relative strength signaling broader market direction?",
        "How do export controls reshape the semi supply chain?",
    ],
}


def build_context() -> str:
    """Serialize current dashboard state from st.session_state into a JSON string.
    Keep it under ~4000 tokens — summarize, don't dump raw dataframes."""
    context = {
        "selected_countries": st.session_state.get("selected_countries", []),
        "date_range": st.session_state.get("date_range", "3Y"),
        "current_page": st.session_state.get("current_page", "Home"),
    }

    # Add summary data if available in session state
    for key in ["market_summary", "liquidity_summary", "rates_summary",
                "economy_summary", "flows_summary", "risk_summary", "sentiment_summary",
                "cross_asset_summary", "policy_summary", "semi_summary"]:
        if key in st.session_state:
            context[key] = st.session_state[key]

    return json.dumps(context, indent=2, default=str)


def render_chat_sidebar():
    """Render the Claude chat in st.sidebar."""
    st.sidebar.divider()
    st.sidebar.subheader("AI Analyst (Claude)")

    # Initialize chat history
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = []

    # Show suggested prompts based on current page
    current_page = st.session_state.get("current_page", "Markets")
    prompts = SUGGESTED_PROMPTS.get(current_page, SUGGESTED_PROMPTS["Markets"])

    st.sidebar.caption("Suggested questions:")
    for prompt in prompts:
        if st.sidebar.button(prompt, key=f"suggest_{hash(prompt)}", use_container_width=True):
            _handle_message(prompt)

    # Display chat history
    for msg in st.session_state.chat_history:
        role = msg["role"]
        with st.sidebar.chat_message(role):
            st.write(msg["content"])

    # Chat input
    user_input = st.sidebar.chat_input("Ask about the data...")
    if user_input:
        _handle_message(user_input)


def _handle_message(user_message: str):
    """Process a user message and generate a response."""
    # Add user message to history
    st.session_state.chat_history.append({"role": "user", "content": user_message})

    # Build context from current dashboard state
    context = build_context()

    # Since no API key is configured, provide a mock analytical response
    response = _mock_response(user_message, context)

    st.session_state.chat_history.append({"role": "assistant", "content": response})
    st.rerun()


def _mock_response(user_message: str, context: str) -> str:
    """Generate a mock analytical response when no API key is available.
    In production, replace this with actual Anthropic API call."""
    msg_lower = user_message.lower()

    if "dxy" in msg_lower or "dollar" in msg_lower:
        return ("The DXY has been trading in the 103-106 range, reflecting mixed signals. "
                "A strong dollar generally indicates capital flowing into US assets, which can "
                "pressure emerging market currencies and commodities. Watch for divergence between "
                "DXY strength and declining US yields — that would signal safe-haven flows rather "
                "than growth-driven dollar demand.")

    elif "copper" in msg_lower or "gold" in msg_lower:
        return ("The copper/gold ratio is a classic risk appetite indicator. Copper reflects "
                "industrial demand (growth expectations) while gold reflects defensive positioning. "
                "A declining ratio suggests the market is shifting risk-off. Currently the ratio "
                "suggests caution — copper has underperformed gold over the past quarter, which "
                "historically correlates with slowing global PMIs.")

    elif "liquidity" in msg_lower or "net liq" in msg_lower:
        return ("Net liquidity (Fed B/S minus TGA minus RRP) has been the dominant driver of "
                "equity markets since 2020. The RRP facility has been draining steadily, which "
                "is actually liquidity-positive as reserves flow back into the system. Watch the "
                "TGA — large Treasury issuance can temporarily drain liquidity. The correlation "
                "between net liquidity and S&P 500 remains elevated around 0.85.")

    elif "rate" in msg_lower or "cut" in msg_lower or "fed" in msg_lower:
        return ("Fed funds futures are pricing in approximately 4-5 rate cuts over the next "
                "12 months, bringing the terminal rate to around 4.10-4.25%. This is more "
                "aggressive than the Fed's own dot plot. The key risk is if inflation re-accelerates, "
                "which would force a repricing of the rate path. Watch the 2s10s spread — "
                "un-inversion historically precedes recessions by 6-12 months.")

    elif "capital" in msg_lower or "flow" in msg_lower:
        return ("Capital flow dynamics show divergence across regions. The US continues to "
                "attract portfolio flows due to higher yields and tech outperformance. China "
                "is seeing reserve depletion despite a current account surplus, suggesting "
                "capital flight through unofficial channels. EM flows are mixed — India and "
                "Brazil attracting FDI while facing portfolio outflows due to dollar strength.")

    elif "risk" in msg_lower or "debt" in msg_lower:
        return ("Country risk assessment highlights Japan (260% debt/GDP) and the US (120% "
                "debt/GDP) as having the most stretched fiscal positions among developed markets. "
                "However, Japan's risk is mitigated by domestic ownership of government debt. "
                "Among EMs, Brazil and India face higher refinancing risk due to external debt "
                "exposure and tighter global financial conditions.")

    elif "sentiment" in msg_lower or "vix" in msg_lower:
        return ("VIX at current levels (~18) suggests complacency. The VIX/MOVE divergence "
                "is notable — bond volatility (MOVE) remains elevated while equity vol is "
                "compressed. Historically this divergence resolves with equity vol catching up. "
                "Consumer sentiment remains below historical averages, which creates a wall of "
                "worry that can actually support equity prices.")

    elif "claim" in msg_lower or "recession" in msg_lower or "economy" in msg_lower:
        return ("Initial claims remain well below recessionary thresholds (~300K+), suggesting "
                "labor market resilience. However, continuing claims have been trending higher, "
                "indicating longer job search durations. The LEI has been negative for 20+ months "
                "but the signal has been muddied by post-COVID distortions. Watch for claims "
                "breaking above their 4-week MA trend — that would be the first concrete "
                "recession signal.")

    elif "carry" in msg_lower or "relative value" in msg_lower or "erp" in msg_lower:
        return ("The relative value matrix currently favors high-yielding EM markets (Brazil, Mexico, "
                "India) on a carry basis — rate differentials of 5-6% over the US are attractive. However, "
                "carry trades are vulnerable to sudden unwinds, especially if the Fed pauses cuts. The "
                "biggest risk is the JPY carry trade — if BOJ continues normalizing, a rapid yen "
                "appreciation could trigger forced deleveraging across global risk assets. On equity "
                "risk premium, China and Korea screen as cheapest (P/E < 11), but low ERP alone isn't "
                "enough — you need positive flow signals to confirm.")

    elif "tariff" in msg_lower or "trade war" in msg_lower or "policy" in msg_lower or "catalyst" in msg_lower:
        return ("The policy environment is the dominant macro catalyst right now. Key catalysts "
                "to watch in the next 90 days:\n\n"
                "1. **US tariff implementation**: 60% China tariff proposal is the single largest "
                "risk to global trade. Phase-in schedule will determine market impact.\n"
                "2. **FOMC March meeting**: Market expects hold, but forward guidance on tariff "
                "inflation will set the tone for H1 rate expectations.\n"
                "3. **ECB March cut**: Most consensus event — priced in. Surprise would be a hold.\n"
                "4. **China policy response**: Watch for RRR cuts, property stimulus, or CNY "
                "management signals in response to tariff pressure.\n\n"
                "Net policy bias is bearish over 90 days due to tariff escalation, but central "
                "bank easing provides a partial offset.")

    elif "semi" in msg_lower or "chip" in msg_lower or "semiconductor" in msg_lower:
        return ("The semiconductor cycle is in a critical phase. Key observations:\n\n"
                "1. **Cycle position**: Book-to-bill is near 1.0, suggesting we're transitioning "
                "from correction to early recovery. Inventory days are normalizing.\n"
                "2. **SOX relative strength** has been improving — semis tend to lead the market "
                "by 2-3 months, so this is a positive signal for broader equities.\n"
                "3. **AI capex** remains the dominant driver — NVDA, AVGO, and TSM are benefiting "
                "from hyperscaler spending, masking weakness in traditional end markets.\n"
                "4. **Policy overlay**: US export controls continue to bifurcate the market. "
                "Long US/allied equipment makers, cautious on China-exposed revenue.\n"
                "5. **Biggest risk**: Taiwan Strait tensions — any escalation would disrupt "
                "~60% of global leading-edge chip production.")

    elif "where" in msg_lower and ("invest" in msg_lower or "allocat" in msg_lower or "money" in msg_lower):
        return ("Based on the cross-asset signal synthesis, here's the current macro picture "
                "for allocation decisions (analysis, not advice):\n\n"
                "**Attractive on relative value:**\n"
                "- EM with high carry (Brazil, India) — but size positions for unwind risk\n"
                "- Value markets with positive flow signals (Korea if tech cycle confirms)\n\n"
                "**Neutral/Benchmark:**\n"
                "- US equities: expensive on ERP but strongest flow support and liquidity\n"
                "- EU: ECB cutting, but tariff exposure and growth concerns\n\n"
                "**Caution:**\n"
                "- China: cheap but capital outflow signals persist\n"
                "- Japan: BOJ normalization creates carry unwind risk\n\n"
                "**Key catalysts to watch:**\n"
                "- Tariff implementation timeline (biggest single variable)\n"
                "- FOMC forward guidance on tariff-driven inflation\n"
                "- Semi cycle inflection (early indicator for risk appetite)")

    else:
        return (f"Based on the current dashboard data for "
                f"{', '.join(st.session_state.get('selected_countries', ['US']))}:\n\n"
                "The macro backdrop shows tension between resilient economic data and tightening "
                "financial conditions. Key themes to watch:\n"
                "1. Net liquidity trajectory and its correlation with equity markets\n"
                "2. Rate cut expectations vs. inflation stickiness\n"
                "3. EM capital flow direction given dollar strength\n"
                "4. Credit spread behavior as a leading indicator\n\n"
                "Ask about specific indicators for deeper analysis.")
