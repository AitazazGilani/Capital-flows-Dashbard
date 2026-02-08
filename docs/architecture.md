# Macro Dashboard — Architecture

## System Overview

```
                 +------------------+
                 |   ingestor.py    |  CLI tool — run once or on a schedule
                 |  (historical)    |
                 +--------+---------+
                          |
              Parquet     |     Live APIs (FRED, yfinance, wbgapi)
              write       |
                          v
                 +------------------+
                 |    data/         |  Parquet file store
                 |  fred/           |  ~84 files, ~3 MB total
                 |  world_bank/     |
                 |  market/         |
                 |  imf/            |
                 |  semi/           |
                 |  policy/         |
                 |  manifest.json   |
                 +--------+---------+
                          |
              Parquet     |
              read        |
                          v
          +-------------------------------+
          |        data_fetcher.py        |  Data access layer
          |  Parquet -> Live API          |  Two-tier loading
          +-------------------------------+
                    |           |
          +---------+     +----+-----+
          |               |          |
    +-----v------+  +-----v---+ +---v---------+
    | processors |  | chart_  | | claude_chat |
    |   .py      |  | helpers | |    .py      |
    +-----+------+  +----+----+ +------+------+
          |               |            |
          +-------+-------+------------+
                  |
          +-------v-------+
          |    pages/     |  10 Streamlit pages
          | 1_Markets.py  |
          | 2_Liquidity   |
          | ...           |
          | 10_Strategic  |
          +-------+-------+
                  |
          +-------v-------+
          |    app.py     |  Entry point, sidebar, home
          +---------------+
```

## Data Flow

### Historical Data (via Ingestor)

```
Live API  -->  ingestor.py  -->  data/*.parquet  -->  data_fetcher.py  -->  pages/
```

1. **Ingestor** fetches data from live APIs and writes Parquet files
2. **data_fetcher.py** reads Parquet on first call, caches in memory via `st.cache_data`
3. **Pages** call data_fetcher functions and render charts

### Real-Time Data (Runtime)

```
yfinance  -->  data_fetcher.py  -->  pages/
```

1. For short periods (1M, 3M), data_fetcher skips Parquet and fetches live
2. Cached for 15 minutes via `st.cache_data(ttl=900)`

### Fallback Chain

Every data function follows a two-tier priority:

```
1. Parquet file exists?  -->  Yes: read and return (fast, ~1ms)
                          -->  No: continue
2. Live API available?   -->  Yes: fetch and return (slower, ~500ms-2s)
                          -->  No: return empty DataFrame
```

**NaN Handling**: Real API data (especially FRED) may contain NaN gaps (weekends,
holidays, reporting delays). The data_fetcher forward-fills (`ffill()`) FRED series
before returning them, ensuring `.iloc[-1]` always returns a valid value for metrics.

## File Structure

```
Capital-flows-Dashboard/
├── app.py                     # Entry point, sidebar, home page
├── ingestor.py                # CLI tool for historical data ingestion
├── requirements.txt           # Python dependencies
├── .env                       # API keys (gitignored)
│
├── src/                       # Application source code
│   ├── __init__.py
│   ├── config.py              # All constants (countries, tickers, indicators)
│   ├── data_fetcher.py        # Data access: Parquet -> Live API
│   ├── processors.py          # Derived indicators (net liquidity, ERP, etc.)
│   ├── chart_helpers.py       # Reusable Plotly charts (dark theme)
│   └── claude_chat.py         # AI sidebar with context-aware responses
│
├── data/                      # Parquet store (gitignored, created by ingestor)
│   ├── manifest.json          # Tracks what was ingested, when, status
│   ├── fred/                  # 21 FRED series (one file each)
│   │   ├── DFF.parquet
│   │   ├── DGS10.parquet
│   │   └── ...
│   ├── world_bank/            # 12 WB indicators (countries as columns)
│   │   ├── BN.CAB.XOKA.GD.ZS.parquet
│   │   └── ...
│   ├── market/                # Equity indices, FX, commodities, volatility
│   │   ├── GSPC.parquet       # S&P 500 (^ stripped from filename)
│   │   ├── EURUSD_X.parquet   # FX pairs (= replaced with _)
│   │   ├── GC_F.parquet       # Gold futures
│   │   ├── DXY.parquet
│   │   └── ...
│   ├── imf/                   # BOP and gold reserves (countries as columns)
│   │   ├── bop.parquet
│   │   └── gold_reserves.parquet
│   ├── semi/                  # Semiconductor sector data
│   │   ├── NVDA.parquet
│   │   ├── revenue_cycle.parquet
│   │   ├── inventory_cycle.parquet
│   │   └── ...
│   └── policy/                # Policy events, CB calendar, tariffs
│       ├── events.parquet
│       ├── cb_calendar.parquet
│       └── tariff_tracker.parquet
│
├── pages/                     # Streamlit pages
│   ├── 1_Markets.py           # Equity indices, FX, commodities
│   ├── 2_Liquidity.py         # Fed balance sheet, net liquidity
│   ├── 3_Rates_Credit.py      # Yield curve, credit spreads
│   ├── 4_Economy.py           # GDP, inflation, employment
│   ├── 5_Capital_Flows.py     # Current account, FDI, reserves
│   ├── 6_Country_Risk.py      # Risk scoring
│   ├── 7_Sentiment.py         # VIX, consumer sentiment
│   ├── 8_Cross_Asset_Signals.py  # Relative value, carry, ERP
│   ├── 9_Policy_Tracker.py    # Policy events, CB calendar, tariffs
│   └── 10_Strategic_Sectors.py   # Semiconductor cycle
│
├── docs/                      # Documentation
│   ├── architecture.md        # This file
│   └── ingestor-usage.md      # Ingestor CLI reference
│
└── ai-conversations/          # AI session summaries
    ├── conversation-summary.md
    └── ingestor-implementation.md
```

## Key Design Decisions

### Why Parquet over CSV or SQLite?

| Factor | Parquet | CSV | SQLite |
|--------|---------|-----|--------|
| Read speed (pandas) | ~10x faster | Baseline | ~2x faster |
| File size | ~5x smaller | Baseline | ~2x smaller |
| Type preservation | Yes (dates, floats) | No (re-parse) | Yes |
| Human inspectable | `pd.read_parquet()` | Text editor | SQL client |
| Server process | No | No | No |
| Schema migrations | Not needed | Not needed | Needed |

For ~50 data series at MVP scale, Parquet files are the sweet spot: fast, compact, no server, no schema management.

### Why not a database?

At MVP scale (50 series, <10MB total), adding a database introduces:
- A dependency to install and manage (even SQLite needs schema migrations)
- Query abstraction when `pd.read_parquet()` is simpler
- No performance benefit at this data volume

If the project grows to 500+ series or needs concurrent writes, SQLite or DuckDB would be the next step.

### Historical vs Real-Time Split

The dashboard doesn't merge historical Parquet with live API data. Instead:
- **Long periods** (1Y+): served from Parquet, updated by running `ingestor.py --update`
- **Short periods** (1M, 3M): served from live API (yfinance)

This avoids complex merging logic. To keep historical data fresh, run the ingestor periodically (daily cron, or manually before analysis sessions).

## Data Sources

| Source | Type | Auth | Update Frequency |
|--------|------|------|-----------------|
| FRED | API | Free key | Daily (rates), Monthly (econ) |
| World Bank | API | None | Annual/Quarterly |
| yfinance | API | None | Real-time (unofficial) |
| IMF | API | None | Quarterly |
| SIA | Scrape | None | Quarterly (semi revenue) |
| Federal Register | API | None | Daily (policy events) |
| GDELT | API | None | Real-time (global events) |

## Countries Tracked

13 countries: US, EU, UK, JP, CN, CA, AU, CH, KR, IN, BR, MX, DE

Each country has: equity index ticker, FX pair, World Bank code, IMF code, policy rate, P/E estimate.
