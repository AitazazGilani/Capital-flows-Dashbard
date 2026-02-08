# Macro Dashboard MVP

A Streamlit dashboard for tracking global macro indicators, capital flows, and market data. Includes an AI chat sidebar powered by Claude for contextual analysis. Primary goal: understand capital flows and macro catalysts to drive investment decisions.

## Pages

### Core Macro Monitoring
1. **Markets** — Equity indices, FX heatmap, DXY, commodities, VIX/MOVE, copper/gold ratio
2. **Liquidity** — Net liquidity vs S&P 500, Fed balance sheet components, M2
3. **Rates & Credit** — Yield curve, Fed funds futures implied path, credit spreads, NFCI
4. **Economy** — GDP growth, inflation, jobless claims, consumer sentiment, LEI
5. **Capital Flows** ⭐ — Current account, trade balance, FDI, reserves, flow signals
6. **Country Risk** — Debt/GDP, budget balance, reserves, composite risk scores
7. **Sentiment** — VIX with bands, copper/gold ratio, MOVE, consumer sentiment

### Investment Decision Pages
8. **Cross-Asset Signals** ⭐ — Relative value decision matrix, carry trade analysis, equity risk premium, cross-asset momentum, macro catalyst scorecard
9. **Policy & Geopolitical Tracker** ⭐ — Trade policy timeline, tariff tracker, central bank calendar, export controls, key macro themes
10. **Strategic Sectors** — Semiconductor cycle deep-dive: SOX relative strength, key stocks, revenue/inventory cycles, book-to-bill, sector policy events

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

### 5. Load historical data (optional but recommended)

```bash
python ingestor.py --full
```

This populates the `data/` directory with Parquet files for fast historical data loading. The dashboard works without this step (falls back to in-memory mock data), but Parquet is faster and supports real API data when keys are configured.

See [docs/ingestor-usage.md](docs/ingestor-usage.md) for full CLI reference.

### 6. Run the dashboard

```bash
streamlit run app.py
```

The app opens at `http://localhost:8501`. Use the sidebar to select countries, adjust the date range, and chat with the AI analyst.

## Data Layer

The dashboard uses a three-tier data loading strategy:

```
1. Parquet files (data/)  →  Fastest, populated by ingestor.py
2. Live API calls         →  When API keys are set (FRED, yfinance, etc.)
3. In-memory mock         →  Always works, no setup needed
```

**Historical data** (FRED series, World Bank indicators, market history) is loaded from Parquet files in `data/`. Run `python ingestor.py --full` to populate these. Update them with `python ingestor.py --update`.

**Real-time data** (current FX rates, live index prices, intraday quotes) is fetched at runtime by the dashboard with short cache TTLs.

The dashboard works out of the box without any API keys or running the ingestor — mock data generators provide reproducible data using seeded random walks.

See [docs/architecture.md](docs/architecture.md) for the full system architecture.

## Project structure

```
├── app.py              # Entry point — sidebar controls and home page
├── ingestor.py         # CLI tool — historical data ingestion to Parquet
├── requirements.txt    # Python dependencies
├── .env                # API keys (gitignored)
├── src/                # Application source code
│   ├── config.py       # All constants (countries, FRED IDs, WB indicators, tickers)
│   ├── data_fetcher.py # Data access layer (Parquet -> API -> Mock fallback)
│   ├── processors.py   # Derived indicators (net liquidity, ERP, flow signals, risk scores)
│   ├── chart_helpers.py# Reusable Plotly chart functions
│   └── claude_chat.py  # AI chat sidebar with context builder
├── pages/              # Streamlit pages (10 total)
├── data/               # Parquet file store (gitignored, created by ingestor.py)
│   ├── manifest.json   # Ingestion metadata
│   ├── fred/           # 21 FRED series
│   ├── world_bank/     # 12 World Bank indicators
│   ├── market/         # Equity indices, FX, commodities, volatility
│   ├── imf/            # BOP, gold reserves
│   ├── semi/           # Semiconductor stocks, cycles
│   └── policy/         # Policy events, CB calendar, tariffs
├── docs/               # Architecture and usage documentation
└── ai-conversations/   # AI session summaries
```

## Data sources

| Source | Library | Auth | What it covers |
|--------|---------|------|----------------|
| Yahoo Finance | `yfinance` | None | Equity indices, FX, commodities, VIX, futures, semi stocks |
| FRED | `fredapi` | API key (free) | US rates, liquidity, credit spreads, economic data |
| World Bank | `wbgapi` | None | International GDP, inflation, current account, FDI, reserves |
| IMF | `requests` | None | Balance of payments, gold reserves |
| ECB | `requests` | None | EUR reference rates |
| BIS | `requests` | None | Real effective exchange rates |
| Anthropic | `anthropic` | API key | Claude AI chat |

### Additional free APIs for live data (no keys required unless noted)

| Source | Auth | What it covers |
|--------|------|----------------|
| [Federal Register API](https://www.federalregister.gov/developers/documentation/api/v1) | None | US executive orders, rules, regulations |
| [GDELT Project](https://www.gdeltproject.org) | None | Global geopolitical events, tone analysis |
| [ProPublica Congress API](https://projects.propublica.org/api-docs/congress-api/) | Free key | US legislation tracking |
| [Trade.gov](https://developer.trade.gov) | None | US tariff and trade policy data |
| [SIA](https://www.semiconductors.org) | None | Quarterly semiconductor revenue reports (scrape) |
| Central bank websites | None | Meeting schedules, statements, rate decisions |
