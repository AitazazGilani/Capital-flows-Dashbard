"""
Claude AI chat integration for the macro dashboard sidebar.
"""

import json
import os
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

    # Check for API key
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        st.sidebar.info(
            "Set ANTHROPIC_API_KEY in .env to enable AI analysis."
        )

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
    """Process a user message and generate a response via Anthropic API."""
    # Add user message to history
    st.session_state.chat_history.append({"role": "user", "content": user_message})

    # Build context from current dashboard state
    context = build_context()

    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        response = "ANTHROPIC_API_KEY not configured. Set it in your .env file to enable AI analysis."
        st.session_state.chat_history.append({"role": "assistant", "content": response})
        st.rerun()
        return

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        # Build messages from chat history
        messages = []
        for msg in st.session_state.chat_history:
            messages.append({"role": msg["role"], "content": msg["content"]})

        result = client.messages.create(
            model="claude-sonnet-4-5-20250929",
            max_tokens=1024,
            system=f"{SYSTEM_PROMPT}\n\nCurrent dashboard context:\n{context}",
            messages=messages,
        )
        response = result.content[0].text
    except ImportError:
        response = "anthropic package not installed. Run: pip install anthropic"
    except Exception as e:
        response = f"API error: {e}"

    st.session_state.chat_history.append({"role": "assistant", "content": response})
    st.rerun()
