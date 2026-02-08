# Macro Dashboard MVP — Claude Code Implementation Plan

## What This Is

A Streamlit app that tracks global macro indicators, capital flows, and market data. Has an AI chat sidebar powered by Claude that can analyze all the data in context. Primary goal: understand capital flows to drive investment decisions.

---

## Tech Stack

- **Streamlit** (multipage app)
- **Plotly** for all charts
- **yfinance** for market data (no API key)
- **fredapi** for US economic data (free API key in `.env`)
- **wbgapi** for World Bank data (no API key)
- **requests** for IMF/BIS/ECB APIs (no API key)
- **anthropic** SDK for Claude chat (API key in `.env`)
- **pandas** for everything

---

## Folder Structure

```
macro-dashboard/
├── app.py                    # Entry point, sidebar, global controls
├── .env                      # FRED_API_KEY, ANTHROPIC_API_KEY
├── requirements.txt
├── config.py                 # All constants: tickers, FRED IDs, country codes, WB indicators
├── data_fetcher.py           # ALL data fetching functions in one file
├── processors.py             # Computed indicators (net liquidity, implied rates, signals)
├── chart_helpers.py          # Reusable plotly chart functions
├── claude_chat.py            # Claude integration + context builder
├── pages/
│   ├── 1_Markets.py
│   ├── 2_Liquidity.py
│   ├── 3_Rates_Credit.py
│   ├── 4_Economy.py
│   ├── 5_Capital_Flows.py
│   ├── 6_Country_Risk.py
│   └── 7_Sentiment.py
```

That's it. No nested folders, no YAML configs, no cache layer. Use `st.cache_data` with TTL for caching. Keep it flat and simple.

---

## .env

```
FRED_API_KEY=your_fred_key_here
ANTHROPIC_API_KEY=your_anthropic_key_here
```

Get FRED key free at: https://fred.stlouisfed.org/docs/api/api_key.html

---

## requirements.txt

```
streamlit>=1.30
plotly>=5.18
yfinance>=0.2.30
fredapi>=0.5
wbgapi>=1.0
pandas>=2.1
numpy>=1.26
requests>=2.31
anthropic>=0.40
python-dotenv>=1.0
beautifulsoup4>=4.12
lxml>=5.1
```

---

## config.py

Single file with all constants. No external config files.

```python
# -- Countries --
COUNTRIES = {
    "US": {"name": "United States", "wb_code": "USA", "imf_code": "US", "index": "^GSPC", "currency_pair": None},
    "EU": {"name": "Eurozone", "wb_code": "EMU", "imf_code": "U2", "index": "^STOXX50E", "currency_pair": "EURUSD=X"},
    "UK": {"name": "United Kingdom", "wb_code": "GBR", "imf_code": "GB", "index": "^FTSE", "currency_pair": "GBPUSD=X"},
    "JP": {"name": "Japan", "wb_code": "JPN", "imf_code": "JP", "index": "^N225", "currency_pair": "USDJPY=X"},
    "CN": {"name": "China", "wb_code": "CHN", "imf_code": "CN", "index": "000001.SS", "currency_pair": "USDCNY=X"},
    "CA": {"name": "Canada", "wb_code": "CAN", "imf_code": "CA", "index": "^GSPTSE", "currency_pair": "USDCAD=X"},
    "AU": {"name": "Australia", "wb_code": "AUS", "imf_code": "AU", "index": "^AXJO", "currency_pair": "AUDUSD=X"},
    "CH": {"name": "Switzerland", "wb_code": "CHE", "imf_code": "CH", "index": "^SSMI", "currency_pair": "USDCHF=X"},
    "KR": {"name": "South Korea", "wb_code": "KOR", "imf_code": "KR", "index": "^KS11", "currency_pair": "USDKRW=X"},
    "IN": {"name": "India", "wb_code": "IND", "imf_code": "IN", "index": "^BSESN", "currency_pair": "USDINR=X"},
    "BR": {"name": "Brazil", "wb_code": "BRA", "imf_code": "BR", "index": "^BVSP", "currency_pair": "USDBRL=X"},
    "MX": {"name": "Mexico", "wb_code": "MEX", "imf_code": "MX", "index": "^MXX", "currency_pair": "USDMXN=X"},
    "DE": {"name": "Germany", "wb_code": "DEU", "imf_code": "DE", "index": "^GDAXI", "currency_pair": None},
}

DEFAULT_COUNTRIES = ["US", "EU", "UK", "JP", "CN"]

# -- FRED Series IDs --
FRED = {
    # Rates
    "fed_funds": "DFF",
    "us_10y": "DGS10",
    "us_2y": "DGS2",
    "us_2s10s": "T10Y2Y",
    "real_yield_10y": "DFII10",
    "breakeven_10y": "T10YIE",
    # Liquidity
    "fed_balance_sheet": "WALCL",
    "rrp": "RRPONTSYD",
    "tga": "WTREGEN",
    "m2": "WM2NS",
    # Credit
    "hy_oas": "BAMLH0A0HYM2",
    "ig_oas": "BAMLC0A0CM",
    "nfci": "NFCI",
    # Economy
    "initial_claims": "ICSA",
    "continuing_claims": "CCSA",
    "consumer_sentiment": "UMCSENT",
    "cpi": "CPIAUCSL",
    "unemployment": "UNRATE",
    "personal_savings": "PSAVERT",
    "industrial_production": "INDPRO",
    "lei": "USALOLITONOSTSAM",
}

# -- Market Tickers (yfinance) --
MARKET_TICKERS = {
    "DXY": "DX-Y.NYB",
    "VIX": "^VIX",
    "MOVE": "^MOVE",
    "BDI": "^BDI",
    "Gold": "GC=F",
    "Copper": "HG=F",
    "WTI": "CL=F",
    "Brent": "BZ=F",
}

# -- Fed Funds Futures (yfinance) --
# ZQH25, ZQJ25, etc. — these rotate, build dynamically in data_fetcher
FF_FUTURES_BASE = "ZQ"

# -- World Bank Indicators --
WB_INDICATORS = {
    "current_account_pct_gdp": "BN.CAB.XOKA.GD.ZS",
    "trade_balance": "NE.RSB.GNFS.CD",
    "fdi_inflows": "BX.KLT.DINV.CD.WD",
    "fdi_outflows": "BM.KLT.DINV.CD.WD",
    "reserves_excl_gold": "FI.RES.TOTL.CD",
    "external_debt": "DT.DOD.DECT.CD",
    "debt_to_gdp": "GC.DOD.TOTL.GD.ZS",
    "budget_balance_pct_gdp": "GC.BAL.CASH.GD.ZS",
    "gdp_current_usd": "NY.GDP.MKTP.CD",
    "gdp_growth": "NY.GDP.MKTP.KD.ZG",
    "inflation_cpi": "FP.CPI.TOTL.ZG",
    "unemployment": "SL.UEM.TOTL.ZS",
}

# -- Date range options --
DATE_RANGES = {
    "1M": 30, "3M": 90, "6M": 180, "1Y": 365,
    "3Y": 1095, "5Y": 1825, "10Y": 3650, "MAX": None,
}
```

---

## data_fetcher.py

All data fetching in one file. Every function is cached with `@st.cache_data(ttl=...)`.

### Functions to implement:

```python
import streamlit as st
import yfinance as yf
import pandas as pd
from fredapi import Fred
import wbgapi as wb
import requests
from config import *

fred = Fred(api_key=os.getenv("FRED_API_KEY"))

# -- Market Data (yfinance) --

@st.cache_data(ttl=900)  # 15 min
def get_index_data(ticker: str, period: str = "5y") -> pd.DataFrame:
    """Single index/ticker history. Returns OHLCV df."""

@st.cache_data(ttl=900)
def get_multiple_tickers(tickers: list[str], period: str = "5y") -> pd.DataFrame:
    """Multiple tickers, returns df with Close prices as columns."""

@st.cache_data(ttl=900)
def get_fx_rates(country_codes: list[str], period: str = "5y") -> pd.DataFrame:
    """Get FX rates for selected countries from config currency_pairs."""

@st.cache_data(ttl=900)
def get_dxy(period: str = "5y") -> pd.DataFrame:
    """DXY index."""

@st.cache_data(ttl=900)
def get_commodities(period: str = "5y") -> pd.DataFrame:
    """Gold, Copper, WTI, Brent, BDI."""

@st.cache_data(ttl=900)
def get_volatility(period: str = "5y") -> pd.DataFrame:
    """VIX and MOVE index."""

# -- FRED Data --

@st.cache_data(ttl=21600)  # 6 hours
def get_fred_series(series_id: str, start: str = "2000-01-01") -> pd.Series:
    """Single FRED series."""

@st.cache_data(ttl=21600)
def get_fred_multiple(series_ids: list[str], start: str = "2000-01-01") -> pd.DataFrame:
    """Multiple FRED series merged into one df."""

@st.cache_data(ttl=21600)
def get_yield_curve_snapshot() -> pd.DataFrame:
    """Current yield curve: 1M, 3M, 6M, 1Y, 2Y, 3Y, 5Y, 7Y, 10Y, 20Y, 30Y.
    FRED series: DGS1MO, DGS3MO, DGS6MO, DGS1, DGS2, DGS3, DGS5, DGS7, DGS10, DGS20, DGS30"""

# -- World Bank Data --

@st.cache_data(ttl=604800)  # 7 days
def get_wb_indicator(indicator: str, countries: list[str], start_year: int = 2000) -> pd.DataFrame:
    """Fetch a World Bank indicator for given countries.
    Use wbgapi: wb.data.DataFrame(indicator, economy=countries, time=range(start_year, 2026))
    Returns df with countries as columns, years as index."""

@st.cache_data(ttl=604800)
def get_wb_multiple_indicators(indicators: dict, countries: list[str]) -> dict[str, pd.DataFrame]:
    """Fetch multiple WB indicators. Returns dict of indicator_name -> df."""

# -- IMF Data --

@st.cache_data(ttl=2592000)  # 30 days
def get_imf_bop(country_code: str, indicator: str = "BCA_BP6_USD") -> pd.DataFrame:
    """Fetch IMF Balance of Payments data.
    URL: http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/BOP/A.{country}.{indicator}
    Parse JSON response into df."""

@st.cache_data(ttl=2592000)
def get_imf_gold_reserves(country_code: str) -> pd.DataFrame:
    """Fetch gold reserves from IMF IFS.
    URL: http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/A.{country}.RAFA_IX"""

# -- ECB Data (backup FX) --

@st.cache_data(ttl=86400)  # 24 hours
def get_ecb_fx(currency: str = "USD") -> pd.DataFrame:
    """ECB reference rate.
    URL: https://data-api.ecb.europa.eu/service/data/EXR/D.{currency}.EUR.SP00.A?format=csvdata"""

# -- BIS Data --

@st.cache_data(ttl=2592000)  # 30 days
def get_bis_reer(country: str) -> pd.DataFrame:
    """Real Effective Exchange Rate from BIS.
    URL: https://stats.bis.org/api/v2/data/WS_EER/M.{country}.R.B.A?format=csv"""

# -- Fed Funds Futures --

@st.cache_data(ttl=3600)  # 1 hour
def get_fed_funds_futures() -> pd.DataFrame:
    """Fetch ZQ futures contracts from yfinance.
    Build contract tickers dynamically for next 12 months.
    Convert price to implied rate: 100 - price = implied rate.
    Return df with columns: contract_month, implied_rate."""
```

---

## processors.py

Computed/derived indicators.

```python
# -- Net Liquidity --
def compute_net_liquidity(fed_bs: pd.Series, tga: pd.Series, rrp: pd.Series) -> pd.Series:
    """Net Liquidity = Fed Balance Sheet - TGA - RRP. Align dates, forward fill, subtract."""

# -- Fed Funds Implied Rate Path --
def compute_implied_rate_path(futures_df: pd.DataFrame, current_rate: float) -> pd.DataFrame:
    """From futures prices, compute implied rate per meeting and number of cuts/hikes priced in."""

# -- Copper/Gold Ratio --
def compute_copper_gold_ratio(copper: pd.Series, gold: pd.Series) -> pd.Series:
    """Simple ratio. Normalize if needed."""

# -- Capital Flow Signal --
def compute_flow_signals(ca_data: pd.DataFrame, reserves_data: pd.DataFrame,
                         fdi_data: pd.DataFrame, fx_data: pd.DataFrame) -> pd.DataFrame:
    """Per-country composite signal based on:
    - Current account trend (improving/deteriorating)
    - Reserve changes (accumulating/depleting)
    - FDI trend
    - FX momentum
    Returns df with country rows and signal columns."""

# -- Country Risk Score --
def compute_risk_scores(debt_gdp: pd.DataFrame, ca_gdp: pd.DataFrame,
                        reserves: pd.DataFrame, budget: pd.DataFrame) -> pd.DataFrame:
    """Simple percentile-based risk score per country.
    Higher debt/GDP = worse, bigger deficit = worse, lower reserves = worse.
    Return df with countries and composite score 0-100."""
```

---

## chart_helpers.py

Reusable Plotly chart functions. Every page calls these.

```python
import plotly.graph_objects as go
import plotly.express as px

def line_chart(df, title, yaxis_title=None, height=400, normalize=False):
    """Multi-line chart from a df where columns are series. If normalize=True, rebase all to 100."""

def dual_axis_chart(series1, series2, name1, name2, title, height=400):
    """Two series on different y-axes."""

def bar_chart(df, title, color_positive="green", color_negative="red", height=400):
    """Bar chart with conditional coloring for positive/negative values."""

def stacked_area(df, title, height=400):
    """Stacked area chart from df columns."""

def heatmap(df, title, colorscale="RdYlGn", height=400):
    """Heatmap from df. Good for FX % changes, risk tables."""

def metric_row(metrics: list[dict]):
    """Display a row of st.metric cards.
    metrics = [{"label": "S&P 500", "value": "5,842", "delta": "+0.3%"}, ...]
    Use st.columns to lay them out."""

def yield_curve_chart(current, historical_snapshots: dict, height=400):
    """Yield curve lines overlaid. historical_snapshots = {"3M ago": df, "1Y ago": df}"""

def sortable_table(df, title, color_columns=None):
    """Styled dataframe with color coding on specified columns."""
```

---

## claude_chat.py

Claude AI chat that lives in the sidebar.

```python
import anthropic
import json
import streamlit as st

def build_context() -> str:
    """Serialize current dashboard state from st.session_state into a JSON string.
    Include: selected countries, date range, current page,
    and whatever data is currently loaded in session_state.
    Keep it under ~4000 tokens — summarize, don't dump raw dataframes.
    For dataframes: include latest values, recent trends, min/max."""

SYSTEM_PROMPT = """You are a macro analyst embedded in a capital flow dashboard.
You have access to the current dashboard data provided as context.

Focus on:
- Capital flow analysis and what drives them
- How monetary policy and liquidity affect capital movements
- Connecting indicators to equity market implications
- Identifying divergences and emerging trends across countries

Reference specific numbers from the context. Be direct and analytical.
You are NOT a financial advisor — frame everything as analysis, not recommendations."""

def render_chat_sidebar():
    """Render the Claude chat in st.sidebar.
    - Initialize st.session_state.chat_history = []
    - Show chat history
    - Text input at bottom
    - On submit: build context, prepend to user message, call anthropic API
    - Append response to history
    - Show suggested prompts based on st.session_state.current_page"""

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
}
```

---

## app.py

Main entry point. Sets up sidebar with global controls and chat.

```python
import streamlit as st
from config import COUNTRIES, DEFAULT_COUNTRIES, DATE_RANGES
from claude_chat import render_chat_sidebar

st.set_page_config(page_title="Macro Dashboard", layout="wide", page_icon="🌍")

# -- Sidebar: Global Controls --
with st.sidebar:
    st.title("🌍 Macro Dashboard")

    # Country selector
    selected = st.multiselect(
        "Countries",
        options=list(COUNTRIES.keys()),
        default=DEFAULT_COUNTRIES,
        format_func=lambda x: f"{COUNTRIES[x]['name']} ({x})"
    )
    st.session_state.selected_countries = selected

    # Date range
    date_range = st.select_slider("Date Range", options=list(DATE_RANGES.keys()), value="3Y")
    st.session_state.date_range = date_range

    st.divider()

    # Claude Chat
    render_chat_sidebar()
```

---

## Page Implementations

### pages/1_Markets.py

```
Layout:
- Metric row: S&P 500, DXY, VIX, Gold (latest value + daily % change)
- Equity Indices chart: multi-line, normalized to 100, for selected countries
  - Toggle: absolute / normalized
- Two columns:
  - Left: FX rates heatmap (1D, 1W, 1M % change for each pair)
  - Right: DXY line chart with 200-day MA
- Two columns:
  - Left: Commodities multi-line (Gold, Copper, Oil, BDI)
  - Right: VIX + MOVE dual axis chart
- Copper/Gold ratio line chart

Data sources:
- yfinance for everything on this page
```

### pages/2_Liquidity.py

```
Layout:
- Metric row: Net Liquidity, Fed B/S, RRP, TGA (latest values)
- Net Liquidity vs S&P 500 dual-axis chart (main chart, full width)
  - Show correlation coefficient in subtitle
- Fed Balance Sheet Components stacked area (B/S, minus TGA, minus RRP)
- Two columns:
  - Left: M2 Money Supply (US) line chart with YoY % change
  - Right: Global central bank balance sheets multi-line
    - Fed (FRED: WALCL), ECB, BOJ, PBOC
    - Note: ECB/BOJ/PBOC may need IMF or manual data — start with Fed only for MVP, add others later

Data sources:
- FRED for all US liquidity series
- yfinance for S&P 500 overlay
- processors.compute_net_liquidity() for derived series
```

### pages/3_Rates_Credit.py

```
Layout:
- Metric row: Fed Funds Rate, 10Y Yield, 2s10s Spread, HY OAS
- US Yield Curve: current vs 3M ago vs 1Y ago (overlaid lines)
- Fed Funds Futures implied rate path step chart
  - Table below: meeting month, implied rate, cuts priced
- Two columns:
  - Left: Credit Spreads (HY OAS + IG OAS) dual line with recession shading
  - Right: Financial Conditions (NFCI) line chart
- Real Yields + Breakevens dual-axis chart
- Interest rates table for selected countries
  - For MVP: use a simple table with manually maintained or FRED-sourced policy rates
  - FRED has some international rates, or use World Bank lending rate data

Data sources:
- FRED for all US rates and credit indicators
- yfinance for Fed Funds Futures (ZQ contracts)
- processors.compute_implied_rate_path()
```

### pages/4_Economy.py

```
Layout:
- GDP Growth grouped bar chart (selected countries, from World Bank)
  - Toggle: annual
- Inflation (CPI YoY) multi-line (selected countries, from World Bank)
  - 2% target reference line
- Two columns:
  - Left: Initial Claims weekly line + 4-week MA (FRED)
  - Right: Continuing Claims line + 4-week MA (FRED)
- Two columns:
  - Left: Consumer Sentiment (Michigan) line chart (FRED)
  - Right: Baltic Dry Index line chart (yfinance)
- Unemployment rates multi-line (selected countries, from World Bank)
- LEI line chart (FRED: USALOLITONOSTSAM)

Data sources:
- FRED for US economic indicators
- World Bank (wbgapi) for international GDP, CPI, unemployment
- yfinance for BDI
```

### pages/5_Capital_Flows.py ⭐ PRIMARY PAGE

```
Layout:
- Current Account Balance bar chart (selected countries, % GDP)
  - Green = surplus, Red = deficit
  - Toggle: % GDP / absolute USD
- Current Account evolution multi-line (time series, selected countries)
- Two columns:
  - Left: Trade Balance bar chart by country
  - Right: FDI bar chart (inflows vs outflows stacked, net line overlay)
- Portfolio Investment flows (from IMF BOP if available, otherwise skip for MVP)
- Foreign Reserves multi-line (selected countries, from World Bank)
- Gold Reserves bar chart (from IMF IFS, latest available for each country)
- Capital Flow Signals Summary table:
  - Columns: Country | CA Trend | Reserve Trend | FDI Net | FX Momentum | Signal
  - Color coded
  - Generated by processors.compute_flow_signals()

Data sources:
- World Bank (wbgapi) for: current account, trade balance, FDI, reserves
- IMF BOP API for: portfolio flows, detailed BOP components
- IMF IFS API for: gold reserves
- processors.compute_flow_signals() for composite signals

Note for implementation: IMF APIs can be flaky. Wrap in try/except. If IMF fails,
fall back to World Bank data which covers most of the same ground. The portfolio
investment breakdown is nice-to-have — current account + FDI + reserves from World Bank
covers 80% of the story.
```

### pages/6_Country_Risk.py

```
Layout:
- Country deep-dive dropdown (select one country for scorecard)
- Scorecard metric row: GDP Growth, Debt/GDP, CA %GDP, Reserves, Gold, Budget Balance
- Two columns:
  - Left: Foreign Reserves multi-line (selected countries)
  - Right: Gold Reserves grouped bar (selected countries)
- Two columns:
  - Left: Debt-to-GDP multi-line
  - Right: Budget Deficit (% GDP) bar chart
- REER line chart (from BIS, selected countries)
  - Note: BIS REER can be tricky. For MVP, skip or use FRED REER series for US only.
- Comparative Risk Table (all selected countries):
  - Sortable, color-coded
  - Columns: Country | Debt/GDP | CA/GDP | Reserves | Budget Bal | Risk Score
  - From processors.compute_risk_scores()

Data sources:
- World Bank for debt/GDP, budget balance, reserves, GDP
- IMF IFS for gold reserves
- BIS for REER (optional for MVP)
```

### pages/7_Sentiment.py

```
Layout:
- VIX line chart with bands at 20 and 30
- Two columns:
  - Left: Copper/Gold ratio line chart
  - Right: Put/Call ratio (skip for MVP — hard to get free, add later)
- Consumer Sentiment (Michigan) with historical average band

This is the lightest page. For MVP, VIX + copper/gold + sentiment is enough.
AAII data requires scraping — add in v2.

Data sources:
- yfinance for VIX, copper, gold
- FRED for consumer sentiment
- processors.compute_copper_gold_ratio()
```

---

## Implementation Order

Build in this exact order. Each step should be testable independently.

### Step 1: Skeleton
- Create folder structure
- `requirements.txt` and `pip install`
- `config.py` with all constants
- `app.py` with sidebar (country selector + date range only, no chat yet)
- Verify `streamlit run app.py` works

### Step 2: Data Fetcher — Market Data
- Implement all yfinance functions in `data_fetcher.py`
- `get_index_data`, `get_multiple_tickers`, `get_fx_rates`, `get_dxy`, `get_commodities`, `get_volatility`
- Test each function independently

### Step 3: Page 1 — Markets
- `chart_helpers.py` — implement `line_chart`, `dual_axis_chart`, `metric_row`, `heatmap`
- Build `pages/1_Markets.py` using data_fetcher + chart_helpers
- This is the "proof of concept" page

### Step 4: Data Fetcher — FRED
- Implement `get_fred_series`, `get_fred_multiple`, `get_yield_curve_snapshot`
- Test with a few series

### Step 5: Page 2 — Liquidity
- `processors.py` — implement `compute_net_liquidity`
- Build `pages/2_Liquidity.py`
- Net liquidity vs S&P chart is the key visualization here

### Step 6: Page 3 — Rates & Credit
- Implement `get_fed_funds_futures` in data_fetcher
- `processors.py` — implement `compute_implied_rate_path`
- Build `pages/3_Rates_Credit.py`
- `chart_helpers.py` — add `yield_curve_chart`

### Step 7: Data Fetcher — World Bank
- Implement `get_wb_indicator`, `get_wb_multiple_indicators`
- Test with current account and GDP data

### Step 8: Page 4 — Economy
- Build `pages/4_Economy.py`
- Mix of FRED (US specific) and World Bank (international)

### Step 9: Data Fetcher — IMF
- Implement `get_imf_bop`, `get_imf_gold_reserves`
- These are the trickiest APIs — test thoroughly
- Wrap everything in try/except with fallbacks

### Step 10: Page 5 — Capital Flows ⭐
- `processors.py` — implement `compute_flow_signals`
- Build `pages/5_Capital_Flows.py`
- `chart_helpers.py` — add `bar_chart`, `sortable_table`
- This is the most important page — spend extra time here

### Step 11: Page 6 — Country Risk
- `processors.py` — implement `compute_risk_scores`
- Build `pages/6_Country_Risk.py`

### Step 12: Page 7 — Sentiment
- `processors.py` — implement `compute_copper_gold_ratio`
- Build `pages/7_Sentiment.py` (lightest page)

### Step 13: Claude Chat Integration
- Build `claude_chat.py`
- `build_context()` — serialize session_state data into summary JSON
- `render_chat_sidebar()` — chat UI in sidebar
- Add suggested prompts per page
- Wire into `app.py` sidebar

### Step 14: Polish
- Error handling for all API calls (show st.warning on failure, don't crash)
- Loading spinners (`st.spinner`) on data fetches
- Consistent chart styling (dark theme if preferred)
- Add `st.session_state.current_page` tracking for Claude context

---

## Key Implementation Notes

1. **yfinance is the backbone** — it's the easiest and covers markets, FX, DXY, commodities, VIX, BDI, and futures. No auth. Start every page with yfinance data.

2. **FRED is second priority** — covers all US-specific monetary, rates, credit, and economic data. One API key, generous free tier (120 req/min).

3. **World Bank is third** — covers international macro data. No auth. `wbgapi` library makes it easy. Data is annual and lagged but it's the best free source for cross-country comparisons.

4. **IMF APIs are flaky** — always wrap in try/except. The SDMX JSON format is annoying to parse. If IMF fails, show a warning and skip that component.

5. **Don't over-engineer caching** — `st.cache_data` with TTL is enough. No Parquet files, no SQLite, no background refresh. Keep it simple.

6. **Chart consistency** — use `plotly.graph_objects` not `plotly.express` for more control. Set a consistent template (`plotly_dark` or `plotly_white`). All charts should have consistent height, margins, and font sizes.

7. **Session state is your database** — store fetched data in `st.session_state` so Claude chat can access it. Don't re-fetch for the chat context.

8. **Claude context budget** — keep the serialized context under ~4000 tokens. Summarize dataframes into latest values + trends, don't dump raw data.

9. **Fail gracefully** — if any data source fails, the page should still render with whatever data is available. Use `st.warning("Could not fetch X data")` and continue.

10. **Date range handling** — convert the sidebar date range selection into appropriate parameters for each data source:
    - yfinance: use `period` parameter ("1mo", "3mo", "1y", "5y", "max")
    - FRED: use `start` date parameter
    - World Bank: use year range
    - IMF: typically annual, just filter the result

---

## API Quick Reference

| Source | Auth | Base URL / Library | Rate Limit |
|--------|------|--------------------|------------|
| Yahoo Finance | None | `yfinance` library | Unofficial, ~2000/hr reasonable |
| FRED | API key | `fredapi` library | 120 req/min |
| World Bank | None | `wbgapi` library | Generous |
| IMF BOP | None | `http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/BOP/` | Unknown, be gentle |
| IMF IFS | None | `http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/IFS/` | Same |
| ECB | None | `https://data-api.ecb.europa.eu/service/data/EXR/` | Generous |
| BIS | None | `https://stats.bis.org/api/v2/data/` | Generous |
| Anthropic | API key | `anthropic` library | Per plan |

---

## Phase 2: Investment Decision Pages (Implemented)

### pages/8_Cross_Asset_Signals.py ⭐ DECISION PAGE

```
Layout:
- Relative Value Decision Matrix: combines ERP, carry trade, and flow signals
  into a single score per country (-3 to +3). Color-coded table + bar chart.
- Rate Differentials & Carry Trade: policy rate differentials vs US,
  carry signal per country, visual bar chart
- Equity Risk Premium (ERP): earnings yield - real yield per country.
  Cheap/Fair/Rich classification. Bar chart with threshold lines.
- Cross-Asset Momentum Dashboard: 1M and 3M returns across equities,
  FX, and commodities. Trend classification (Strong Up/Down/Mixed).
  Heatmap of equity momentum.
- Macro Catalyst Scorecard: net positive/negative policy events per country
  in last 90 days. Bullish/Bearish/Neutral classification.

Data sources:
- config.py: POLICY_RATES, COUNTRY_PE_ESTIMATES
- processors: compute_rate_differentials, compute_equity_risk_premium,
  compute_relative_value_matrix, compute_cross_asset_momentum,
  compute_macro_catalyst_score
- data_fetcher: existing WB/FX data + get_policy_events
```

### pages/9_Policy_Tracker.py ⭐ CATALYST PAGE

```
Layout:
- Policy pulse metrics: event counts (30d/90d), bullish vs bearish
- Filterable policy event timeline with expandable details
  - Categories: Trade & Tariffs, Export Controls, Central Bank, Industrial Policy,
    Capital Controls, Regulatory, Geopolitical
  - Impact tagging: Positive/Negative/Mixed/Neutral
- Event distribution charts: by category (horizontal bar), by impact (donut)
- Central Bank Meeting Calendar: upcoming meetings with expected actions
  and market probability. Urgency color coding by days until meeting.
- US Tariff & Trade Policy Tracker: current vs pre-2025 rates by sector,
  tariff escalation grouped bar chart, retaliation tracking
- Key Macro Themes: curated strategic themes with status, description,
  affected sectors, and investment direction

Data sources (for live implementation):
- Federal Register API (free, no key): US executive orders, rules
- GDELT Project (free, no key): global geopolitical events
- ProPublica Congress API (free, key required): US legislation
- Trade.gov (free, no key): tariff and trade data
- Central bank websites (free): meeting schedules, statements
```

### pages/10_Strategic_Sectors.py

```
Layout:
- SOX vs S&P 500: normalized performance chart + relative strength line
  - Metrics: SOX level, 1M return, outperformance vs S&P
- Key Semiconductor Stocks: 11 stocks (NVDA, TSM, ASML, AMD, etc.)
  - Performance table (1D, 1W, 1M, 3M, YTD returns)
  - Normalized multi-line chart with toggle
- Semiconductor Industry Cycle:
  - Global revenue (quarterly bar + QoQ % line, dual axis)
  - Book-to-Bill ratio + Inventory Days (dual axis)
  - Automated cycle phase detection (Early Upcycle/Late Upcycle/Downcycle/Recovery)
- Semiconductor ETFs: SMH, SOXX normalized chart
- Semi-Relevant Policy Events: filtered from policy tracker (export controls,
  CHIPS Act, China Big Fund, Taiwan Strait)
- Investment Framework: how to use the page for allocation decisions

Data sources (for live implementation):
- yfinance (free, no key): all stock/ETF price data
- SIA (free): quarterly semiconductor revenue + book-to-bill
- SEMI (free reports): equipment billings, fab utilization
- WSTS (free): semiconductor trade statistics forecasts
```

### New Processors (processors.py additions)

```python
compute_rate_differentials(countries) -> pd.DataFrame
compute_equity_risk_premium(countries) -> pd.DataFrame
compute_relative_value_matrix(countries, erp_df, carry_df, flow_signals) -> pd.DataFrame
compute_cross_asset_momentum(equity_df, fx_df, commodity_df) -> pd.DataFrame
compute_macro_catalyst_score(policy_events, lookback_days) -> pd.DataFrame
compute_semi_relative_strength(semi_series, market_series) -> pd.Series
```

### New Data Fetchers (data_fetcher.py additions)

```python
get_semi_stocks(period) -> pd.DataFrame
get_semi_etfs(period) -> pd.DataFrame
get_semi_vs_market(period) -> pd.DataFrame
get_semi_revenue_cycle() -> pd.DataFrame
get_semi_inventory_cycle() -> pd.DataFrame
get_policy_events() -> pd.DataFrame
get_central_bank_calendar() -> pd.DataFrame
get_tariff_tracker() -> pd.DataFrame
```
