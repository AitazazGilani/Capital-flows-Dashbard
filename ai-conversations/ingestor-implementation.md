# AI Conversation — Historical Data Ingestor Implementation

**Date:** February 8, 2026
**Branch:** `claude/macro-dashboard-mvp-1hob1`
**Focus:** Designing and building a historical data ingestion layer with Parquet storage

---

## The Problem

The dashboard was generating all data in-memory at runtime — every page load recreated mock time series from scratch. This approach has two issues:

1. **No persistence** — When switching to real APIs, each page load would make expensive API calls (FRED rate limits, yfinance unofficial limits, World Bank flakiness)
2. **No separation of concerns** — Historical data (years of GDP, rates, price history) and real-time data (current FX quotes, live index prices) were handled identically

The user identified this:
> "For historical data for all the indicators and indices, I can't just load them at run time. Would a historical ingestor make sense? For daily and real time data like for viewing FX or indices or commodities should happen at run time or real time."

## The Discussion

### Options Considered

| Option | Pros | Cons |
|--------|------|------|
| **SQLite** | Structured queries, ACID | Schema migrations, extra dependency |
| **CSV files** | Human readable, simple | Slow reads, no type preservation |
| **Parquet files** | Fast reads, compact, pandas-native | Less human-readable than CSV |
| **PostgreSQL** | Production-grade, concurrent | Way overkill for MVP |

### Decision: Parquet + Standalone Ingestor

For an MVP with ~50 data series and <10MB total, Parquet files with a CLI ingestor script is the sweet spot:
- Parquet reads are ~10x faster than CSV
- Column types are preserved (no date re-parsing)
- Files are ~5x smaller than CSV
- No server process, no schema migrations
- Just files — `ls`, copy, delete, inspect with `pd.read_parquet()`

### Historical vs Real-Time Split

| Data Type | Source | Storage | Update Method |
|-----------|--------|---------|--------------|
| FRED macro series | Ingestor | Parquet | `--update` (daily/weekly) |
| World Bank indicators | Ingestor | Parquet | `--update` (quarterly) |
| Historical prices (>1mo) | Ingestor | Parquet | `--update` (daily) |
| Policy events | Ingestor | Parquet | `--update` (as needed) |
| **Live FX rates** | **Runtime** | **st.cache_data (5min)** | **Automatic** |
| **Live index prices** | **Runtime** | **st.cache_data (5min)** | **Automatic** |
| **Live commodity prices** | **Runtime** | **st.cache_data (5min)** | **Automatic** |

Dashboard functions use a simple heuristic: long periods (1Y+) check Parquet first, short periods (1M, 3M) skip Parquet for freshness.

---

## What Was Built

### 1. `ingestor.py` — CLI Ingestion Tool

A standalone CLI script (no Streamlit dependency) with five modes:

```bash
python ingestor.py --full              # Bulk load all sources
python ingestor.py --update            # Incremental (new data only)
python ingestor.py --source fred       # Single source
python ingestor.py --status            # Print manifest summary
python ingestor.py --clean             # Remove all data files
```

**Six data sources:**
- `fred` — 21 FRED series (rates, liquidity, credit, economy)
- `world_bank` — 12 indicators x 13 countries
- `market` — Equity indices, FX, commodities, volatility, DXY
- `imf` — Balance of payments, gold reserves
- `semi` — Semiconductor stocks, ETFs, revenue cycle, inventory cycle
- `policy` — Policy events, CB calendar, tariff tracker

**Three-tier fetching per source:**
1. Try live API (when key/library available)
2. Fall back to mock generator (same seeds as data_fetcher.py)
3. Log errors and continue to next series

**Manifest tracking** (`data/manifest.json`):
```json
{
  "fred/DFF": {
    "last_updated": "2026-02-08T12:00:01",
    "status": "ok",
    "rows": 1305,
    "date_range": ["2021-02-06", "2026-02-06"]
  }
}
```

### 2. `data_fetcher.py` — Parquet Loading Layer

Added three helpers at the top of data_fetcher.py:

```python
DATA_DIR = Path(__file__).parent / "data"
_LONG_PERIODS = {"1y", "3y", "5y", "10y", "max", ...}

def _load_parquet(category, name) -> DataFrame | None
def _filter_by_period(df, period) -> DataFrame
def _sanitize_filename(name) -> str
```

Every existing data function gained a Parquet check at the top:

```python
@st.cache_data(ttl=900)
def get_fred_series(series_id, start="2000-01-01"):
    # 1. Try Parquet (fast, ~1ms)
    pq = _load_parquet("fred", series_id)
    if pq is not None:
        return pq.iloc[:, 0]

    # 2. Mock fallback (always works)
    idx = _date_index(1825)
    ...
```

Functions modified: `get_index_data`, `get_fx_rates`, `get_dxy`, `get_commodities`, `get_volatility`, `get_fred_series`, `get_wb_indicator`, `get_imf_bop`, `get_imf_gold_reserves`, `get_semi_stocks`, `get_semi_etfs`, `get_semi_vs_market`, `get_semi_revenue_cycle`, `get_semi_inventory_cycle`, `get_policy_events`, `get_central_bank_calendar`, `get_tariff_tracker`.

### 3. Data Directory Structure

```
data/
├── manifest.json
├── fred/           # 21 files — one per FRED series
├── world_bank/     # 12 files — one per WB indicator (countries as columns)
├── market/         # ~29 files — indices, FX pairs, commodities, DXY, VIX, MOVE
├── imf/            # 2 files — bop.parquet, gold_reserves.parquet
├── semi/           # ~15 files — stocks, ETFs, revenue/inventory cycles
└── policy/         # 3 files — events, cb_calendar, tariff_tracker
```

Total: ~52 Parquet files, ~5 MB disk usage.

### 4. Documentation

- `docs/architecture.md` — Full system architecture with data flow diagrams
- `docs/ingestor-usage.md` — CLI reference with examples and troubleshooting
- Updated `README.md` — Added ingestor setup step, data layer explanation, updated project structure

### 5. Supporting Changes

- `requirements.txt` — Added `pyarrow>=15.0`
- `.gitignore` — Added `data/` directory (Parquet files are generated, not committed)

---

## Key Design Choices

1. **Standalone ingestor** — No Streamlit dependency. Runs as a plain Python CLI script. This means it can be called from cron, CI, or scripts without importing the full dashboard.

2. **Same mock seeds** — The ingestor uses identical `np.random.RandomState` seeds as data_fetcher.py, so Parquet mock data matches in-memory mock data exactly.

3. **Filename sanitization** — Tickers like `^GSPC` and `EURUSD=X` have special characters stripped for Parquet filenames (`GSPC.parquet`, `EURUSD_X.parquet`). The same sanitization is used in both ingestor and data_fetcher.

4. **No merge complexity** — Historical (Parquet) and real-time (runtime) data are served independently. The dashboard doesn't try to stitch them together. This keeps the MVP simple and debuggable.

5. **Graceful degradation** — If a single series fails to ingest, the ingestor logs the error and continues. The dashboard still works because it falls back to mock data for any missing Parquet file.

---

## User Workflow

```bash
# First time setup
git clone <repo>
cd Capital-flows-Dashboard
pip install -r requirements.txt

# Populate historical data
python ingestor.py --full        # Takes ~5s with mock, longer with real APIs

# Run dashboard
streamlit run app.py             # Reads from Parquet, fast page loads

# Daily update (optional)
python ingestor.py --update      # Fetches only new data

# Debug a source
python ingestor.py --source fred --status
```
