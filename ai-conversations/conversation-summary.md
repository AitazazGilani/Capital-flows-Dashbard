# Macro Dashboard — AI Conversation Summary

**Date:** February 8, 2026
**Branch:** `claude/macro-dashboard-mvp-1hob1`
**Focus:** Macro catalyst tracking, capital flows dashboard, and investment decision support

---

## Overview

This conversation covered the full design and implementation of a **Streamlit-based macro economic dashboard** for tracking global capital flows and deriving investment decisions. The project was built in two phases:

- **Phase 1 (MVP):** 7-page dashboard covering Markets, Liquidity, Rates & Credit, Economy, Capital Flows, Country Risk, and Sentiment
- **Phase 2 (Investment Signals):** 3 additional pages focused on actionable investment signals — Cross-Asset Signals, Policy & Geopolitical Tracker, and Strategic Sectors (Semiconductors)

All data uses **seeded mock generators** (reproducible random walks and mean-reverting processes) that can be swapped for live APIs when keys are available.

---

## Macro Catalyst Tracking — Key Discussion

### The Problem

The initial MVP dashboard provided a solid view of macro *state* — where rates are, what liquidity looks like, how economies are performing. But it lacked insight into macro *catalysts* — the events and policy shifts that actually move markets and create investment opportunities.

The user specifically highlighted:

> "An very important use case I need from this dashboard is to figure out macro catalysts that can impact markets and derive investment decisions."

### The Solution: Three New Pages

We identified three high-priority additions, implemented in order of impact:

#### 1. Cross-Asset Signals (Page 8) — The Decision Page

**Purpose:** Synthesize all other pages into actionable per-country investment signals.

**Key components:**
- **Relative Value Decision Matrix** — Combines equity risk premium (ERP), carry trade attractiveness, and capital flow direction into a single score per country (-3 to +3). Positive = attractive, negative = avoid.
- **Rate Differentials & Carry Trade** — Policy rate differential vs US for each country. Positive carry attracts capital but watch for unwind risk (especially JPY).
- **Equity Risk Premium** — ERP = Earnings Yield - Real Yield. Markets with ERP > 4% are cheap relative to bonds.
- **Cross-Asset Momentum** — 1-month and 3-month return-based trend signals across equities, FX, and commodities.
- **Macro Catalyst Scorecard** — Net bullish/bearish policy environment per country based on recent events.

**Investment logic:** Countries with positive Total Score + rising momentum + bullish catalyst environment = highest conviction opportunities.

#### 2. Policy & Geopolitical Tracker (Page 9) — The Catalyst Page

**Purpose:** Track the specific events that drive capital reallocation and market repricing.

**Key components:**
- **Policy Event Timeline** — 20 curated realistic events (2025-2026) with filterable categories, countries, and impact assessment. Each event includes affected sectors and detailed analysis.
- **Event Distribution** — Visual breakdown by category and impact (positive/negative/mixed/neutral).
- **Central Bank Meeting Calendar** — 10 upcoming rate decisions with expected actions, market probabilities, and urgency coding (red = within 7 days, yellow = within 30 days).
- **US Tariff & Trade Policy Tracker** — Current vs pre-2025 tariff rates by sector with escalation visualization.
- **Key Macro Themes** — Five major themes with status tracking:
  1. US-China Tech Decoupling (Escalating)
  2. Global Tariff Escalation (Active)
  3. Synchronized Rate Cutting Cycle (Paused)
  4. Industrial Policy Renaissance (Ongoing)
  5. Taiwan Strait Risk Premium (Elevated)

**Policy event categories tracked:**
- Trade & Tariffs
- Export Controls & Sanctions
- Central Bank Policy
- Industrial Policy
- Capital Controls
- Regulatory Change
- Geopolitical Event

**Investment logic:** The surprise component matters most — a "hold" when cuts are priced at 80% is effectively hawkish. Track what's priced in vs what actually happens.

#### 3. Strategic Sectors: Semiconductors (Page 10) — The Sector Deep-Dive

**Purpose:** Semiconductors drive capex cycles, trade policy, and geopolitical tension — making them a key macro input signal.

**Key components:**
- **SOX vs S&P 500** — Normalized performance comparison with relative strength line. Semis tend to lead the cycle by 2-3 months.
- **Key Semiconductor Stocks** — 11 stocks tracked (NVDA, TSM, ASML, AMD, AVGO, INTC, QCOM, MU, LRCX, AMAT, TXN) with 1D/1W/1M/3M/YTD performance.
- **Revenue & Inventory Cycle** — Quarterly global semi revenue with QoQ changes, book-to-bill ratio, and inventory days.
- **Automated Cycle Phase Detection:**
  - Early Upcycle: B2B > 1.05, Inventory < 90 days → Best phase for semi stocks
  - Late Upcycle: B2B > 1.0, Inventory > 100 days → Watch for corrections
  - Downcycle: B2B < 1.0, Inventory > 100 days → Wait for trough
  - Recovery: Inventory normalizing, B2B stabilizing → Early positioning
- **Semi-Relevant Policy Events** — Filtered from the policy tracker for semiconductor-specific catalysts.

**Investment logic:** Book-to-bill is the single best leading indicator. B2B > 1.05 with low inventory = aggressive long. QoQ revenue inflecting positive from negative is historically the best entry point.

---

## Technical Implementation

### Architecture

```
Capital-flows-Dashboard/
├── app.py                    # Entry point, sidebar controls, home page
├── config.py                 # All constants (13 countries, tickers, indicators)
├── data_fetcher.py           # Mock data generators (swap for live APIs)
├── processors.py             # Derived indicators and signal computation
├── chart_helpers.py          # Reusable Plotly charts (dark theme)
├── claude_chat.py            # AI sidebar with context-aware responses
└── pages/
    ├── 1_Markets.py          # Global market indices and FX
    ├── 2_Liquidity.py        # Fed balance sheet, net liquidity
    ├── 3_Rates_Credit.py     # Yield curves, credit spreads
    ├── 4_Economy.py          # GDP, inflation, employment
    ├── 5_Capital_Flows.py    # THE primary page — capital flow analysis
    ├── 6_Country_Risk.py     # Risk scoring by country
    ├── 7_Sentiment.py        # VIX, put/call, fear & greed
    ├── 8_Cross_Asset_Signals.py  # Decision matrix (Phase 2)
    ├── 9_Policy_Tracker.py       # Macro catalysts (Phase 2)
    └── 10_Strategic_Sectors.py   # Semiconductor deep-dive (Phase 2)
```

### New Processors (Phase 2)

| Function | Purpose |
|----------|---------|
| `compute_rate_differentials()` | Policy rate differential vs US, carry signal |
| `compute_equity_risk_premium()` | ERP = earnings yield - real yield per country |
| `compute_relative_value_matrix()` | Composite -3 to +3 score (ERP + carry + flows) |
| `compute_cross_asset_momentum()` | 1M/3M return-based trend classification |
| `compute_macro_catalyst_score()` | Net bullish/bearish per country from policy events |
| `compute_semi_relative_strength()` | SOX vs S&P 500 rebased ratio |

### New Data Fetchers (Phase 2)

| Function | Live API Source | Notes |
|----------|----------------|-------|
| `get_semi_stocks()` | yfinance | Free, no key, ~2000 req/hr |
| `get_semi_etfs()` | yfinance | SMH, SOXX |
| `get_semi_vs_market()` | yfinance | SOX vs SPY |
| `get_semi_revenue_cycle()` | SIA web scraping | Free quarterly reports |
| `get_semi_inventory_cycle()` | SEMI reports | Free |
| `get_policy_events()` | Federal Register API, GDELT | Free, no key |
| `get_central_bank_calendar()` | Central bank websites | Free |
| `get_tariff_tracker()` | Trade.gov | Free, no key |

### Config Extensions (Phase 2)

```python
SEMI_TICKERS = {"SOX": "^SOX", "NVDA": "NVDA", "TSM": "TSM", ...}  # 11 stocks
SEMI_ETFS = {"SMH": "SMH", "SOXX": "SOXX"}
POLICY_CATEGORIES = [7 categories]
POLICY_RATES = {"US": 5.33, "EU": 4.50, ...}  # 13 countries
COUNTRY_PE_ESTIMATES = {"US": 22.5, "EU": 13.5, ...}  # 13 countries
```

---

## Free API Sources for Live Data

| Source | Key Required? | Data |
|--------|--------------|------|
| FRED API | Yes (free) | US macro indicators, rates, spreads |
| World Bank (wbgapi) | No | GDP, current account, reserves, FDI |
| yfinance | No | Stock/ETF/index prices, FX rates |
| Federal Register API | No | US executive orders, regulations |
| GDELT Project | No | Global events, tone analysis |
| Trade.gov | No | Tariff and trade policy data |
| SIA Reports | No | Semiconductor revenue, B2B data |
| WSTS | No | World Semiconductor Trade Statistics |

---

## Key Design Decisions

1. **Mock data with seeded random walks** — Ensures reproducibility across sessions. Each mock function uses `np.random.RandomState(seed)` with consistent seeds.

2. **Composite scoring approach** — Rather than showing raw data, the dashboard synthesizes multiple signals into actionable scores (-3 to +3 for relative value, 0-100 for risk).

3. **Policy events as structured data** — Events include category, country, impact, affected sectors, and detailed analysis. This structure supports filtering, aggregation, and catalyst scoring.

4. **Cycle phase detection** — Automated classification of the semiconductor cycle based on book-to-bill and inventory thresholds, removing subjectivity.

5. **Session state for AI context** — Each page stores its summary in `st.session_state`, making it available to the Claude chat sidebar for context-aware responses.

6. **Free-first API strategy** — All recommended data sources are free or have free tiers. No paid data vendor dependencies.

---

## Commits

1. **MVP implementation** — Full 7-page dashboard with mock data, chart helpers, processors, and Claude chat integration
2. **README** — Setup guide with API keys, virtual environment, and project structure
3. **Phase 2: Investment signal pages** — Cross-Asset Signals, Policy Tracker, Strategic Sectors with all supporting processors, data fetchers, and config extensions

All work was committed and pushed to `claude/macro-dashboard-mvp-1hob1`.
