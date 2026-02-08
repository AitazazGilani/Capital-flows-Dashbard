# Macro Dashboard MVP

A Streamlit dashboard for tracking global macro indicators, capital flows, and market data. Includes an AI chat sidebar powered by Claude for contextual analysis. Primary goal: understand capital flows to drive investment decisions.

## Pages

1. **Markets** — Equity indices, FX heatmap, DXY, commodities, VIX/MOVE, copper/gold ratio
2. **Liquidity** — Net liquidity vs S&P 500, Fed balance sheet components, M2
3. **Rates & Credit** — Yield curve, Fed funds futures implied path, credit spreads, NFCI
4. **Economy** — GDP growth, inflation, jobless claims, consumer sentiment, LEI
5. **Capital Flows** ⭐ — Current account, trade balance, FDI, reserves, flow signals
6. **Country Risk** — Debt/GDP, budget balance, reserves, composite risk scores
7. **Sentiment** — VIX with bands, copper/gold ratio, MOVE, consumer sentiment

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/AitazazGilani/Capital-flows-Dashbard.git
cd Capital-flows-Dashbard
```

### 2. Create a virtual environment

```bash
python -m venv venv
source venv/bin/activate        # Linux / macOS
venv\Scripts\activate           # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure API keys

Copy the `.env` file and add your keys:

```bash
cp .env .env.local   # optional — .env is already gitignored
```

Edit `.env` with your keys:

```
FRED_API_KEY=your_fred_key_here
ANTHROPIC_API_KEY=your_anthropic_key_here
```

| Key | Where to get it | Required? |
|-----|-----------------|-----------|
| `FRED_API_KEY` | Free at https://fred.stlouisfed.org/docs/api/api_key.html | No — mock data is used when missing |
| `ANTHROPIC_API_KEY` | https://console.anthropic.com/ | No — mock chat responses are used when missing |

The other data sources (yfinance, World Bank, IMF, ECB, BIS) do not require API keys.

### 5. Run the dashboard

```bash
streamlit run app.py
```

The app opens at `http://localhost:8501`. Use the sidebar to select countries, adjust the date range, and chat with the AI analyst.

## Mock data

The dashboard ships with mock data generators so it works out of the box without any API keys. Mock data uses seeded random walks for reproducibility and covers all data sources (market prices, FRED economic series, World Bank annual indicators, IMF balance of payments, and more).

To switch to live data, add your API keys to `.env`. The fetching functions in `data_fetcher.py` are the only file that needs changes — swap the mock implementations for real API calls.

## Project structure

```
├── app.py              # Entry point — sidebar controls and home page
├── config.py           # All constants (countries, FRED IDs, WB indicators, tickers)
├── data_fetcher.py     # Data fetching functions (mock data by default)
├── processors.py       # Derived indicators (net liquidity, flow signals, risk scores)
├── chart_helpers.py    # Reusable Plotly chart functions
├── claude_chat.py      # AI chat sidebar with context builder
├── requirements.txt    # Python dependencies
├── .env                # API keys (gitignored)
└── pages/
    ├── 1_Markets.py
    ├── 2_Liquidity.py
    ├── 3_Rates_Credit.py
    ├── 4_Economy.py
    ├── 5_Capital_Flows.py
    ├── 6_Country_Risk.py
    └── 7_Sentiment.py
```

## Data sources

| Source | Library | Auth | What it covers |
|--------|---------|------|----------------|
| Yahoo Finance | `yfinance` | None | Equity indices, FX, commodities, VIX, futures |
| FRED | `fredapi` | API key | US rates, liquidity, credit spreads, economic data |
| World Bank | `wbgapi` | None | International GDP, inflation, current account, FDI, reserves |
| IMF | `requests` | None | Balance of payments, gold reserves |
| ECB | `requests` | None | EUR reference rates |
| BIS | `requests` | None | Real effective exchange rates |
| Anthropic | `anthropic` | API key | Claude AI chat |
