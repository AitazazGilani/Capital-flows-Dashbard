# Ingestor — Usage Guide

The ingestor (`ingestor.py`) is a standalone CLI tool that populates the `data/` directory with Parquet files. The dashboard reads from these files for fast historical data access.

## Quick Start

```bash
# First-time setup: load all historical data
python ingestor.py --full

# Check what's been ingested
python ingestor.py --status

# Run the dashboard (now reads from Parquet)
streamlit run app.py
```

## Commands

### `--full` — Full Historical Load

Fetches all sources and writes Parquet files. Use on first setup or to rebuild from scratch.

```bash
python ingestor.py --full
```

Output:
```
Macro Dashboard Ingestor — Full run
Started: 2026-02-08T12:00:00

[FRED] Ingesting US macro series...
  Using live FRED API
  ok  fed_funds (DFF): 1593 rows
  ok  us_10y (DGS10): 1593 rows
  ...

[World Bank] Ingesting international indicators...
  Using live World Bank API
  ok  current_account_pct_gdp (BN.CAB.XOKA.GD.ZS): 6 rows x 13 countries
  ...

[Market] Ingesting market price history...
  Using live yfinance API
  ...

Finished in 12.5s — 84 ok, 0 errors
Manifest saved to data/manifest.json
```

**Note:** FRED requires `FRED_API_KEY` in `.env`. IMF, World Bank, Federal Register, and yfinance APIs need no authentication.

### `--update` — Incremental Update

Fetches only new data since the last run. For FRED with a live API key, this appends only missing dates. Incremental updates require the same API access as full runs.

```bash
python ingestor.py --update
```

### `--source <name>` — Single Source

Ingest only one data source. Useful for debugging or targeted updates.

```bash
python ingestor.py --source fred         # FRED macro series + yield curve + EPU + GPR
python ingestor.py --source world_bank   # World Bank indicators only
python ingestor.py --source market       # Equity, FX, commodities, volatility, BDI
python ingestor.py --source imf          # IMF BOP and gold reserves
python ingestor.py --source semi         # Semiconductor stocks + ISM cycle proxy
python ingestor.py --source policy       # Policy events, CB calendar, tariffs
python ingestor.py --source bis          # BIS Real Effective Exchange Rates
python ingestor.py --source cftc         # CFTC Commitments of Traders
```

Combine with `--update` for incremental single-source updates:
```bash
python ingestor.py --update --source fred
```

### `--status` — View Manifest

Shows what has been ingested, when, and disk usage.

```bash
python ingestor.py --status
```

Output:
```
Data Manifest (52 entries)
----------------------------------------------------------------------
  fred             21 ok, 0 errors
  imf               2 ok, 0 errors
  market           29 ok, 0 errors
  policy            3 ok, 0 errors
  semi             15 ok, 0 errors
  world_bank       12 ok, 0 errors
----------------------------------------------------------------------
  TOTAL            52 ok, 0 errors

  Oldest update: 2026-02-08T12:00:01
  Newest update: 2026-02-08T12:00:04
  Total Parquet size: 4.8 MB
```

### `--clean` — Remove All Data

Deletes the entire `data/` directory. The dashboard will show empty data until the ingestor is run again.

```bash
python ingestor.py --clean
```

## Sources Reference

| Source | Parquet Directory | # Files | Live API | Status |
|--------|------------------|---------|----------|--------|
| `fred` | `data/fred/` | 21+ | FRED API (needs `FRED_API_KEY`) + yield curve + EPU | Requires API key |
| `world_bank` | `data/world_bank/` | 12 | wbgapi (no key needed) | Ready |
| `market` | `data/market/` | ~33 | yfinance (no key needed) — includes BDI | Ready |
| `imf` | `data/imf/` | 2 | IMF SDMX JSON API (no key needed) | Ready |
| `semi` | `data/semi/` | ~15 | yfinance + ISM PMI cycle proxy (needs `FRED_API_KEY`) | Ready |
| `policy` | `data/policy/` | 4+ | Federal Register API + GPR Index (no key needed) | Ready |
| `bis` | `data/bis/` | 13 | BIS SDMX REST API (no key needed) | Ready |
| `cftc` | `data/cftc/` | 3 | CFTC Socrata API (no key needed) | Ready |

## Parquet File Format

Each file is a standard pandas DataFrame saved with `pyarrow`:

```python
# Reading a file directly
import pandas as pd
df = pd.read_parquet("data/fred/DFF.parquet")
print(df.head())
```

**FRED files** — Date index, single `value` column:
```
                     value
2021-02-08         5.280
2021-02-09         5.315
```

**Market files** — Date index, OHLCV columns:
```
                    Open       High        Low      Close    Volume
2021-02-08     4215.30   4248.10   4198.50   4230.00  3845201
```

**World Bank files** — Year index, country columns:
```
       USA      GBR       JPN       CHN
2000  -3.12    -3.45     3.61      1.48
2001  -3.08    -3.52     3.55      1.52
```

**IMF files** — Year index, IMF country code columns:
```
         US       GB        JP        CN
2005   -502.3   -98.1    153.2    198.5
```

## Manifest File

`data/manifest.json` tracks metadata for each ingested series:

```json
{
  "fred/DFF": {
    "last_updated": "2026-02-08T12:00:01",
    "status": "ok",
    "rows": 1305,
    "date_range": ["2021-02-06", "2026-02-06"]
  },
  "market/^GSPC": {
    "last_updated": "2026-02-08T12:00:02",
    "status": "ok",
    "rows": 1305,
    "date_range": ["2021-02-08", "2026-02-06"]
  }
}
```

Use this to verify freshness and debug issues.

## Scheduling Updates

For daily updates, add a cron job:

```bash
# Run ingestor daily at 6am (after US market close for previous day data)
0 6 * * * cd /path/to/Capital-flows-Dashboard && python ingestor.py --update >> logs/ingestor.log 2>&1
```

Or run manually before analysis sessions:
```bash
python ingestor.py --update && streamlit run app.py
```

## How the Dashboard Uses Parquet

In `data_fetcher.py`, every function follows this pattern:

```python
@st.cache_data(ttl=900)
def get_some_data(period="5y"):
    # 1. Try Parquet (fast, historical)
    pq = _load_parquet("category", "name")
    if pq is not None:
        return _filter_by_period(pq, period)

    # 2. No data available
    return pd.DataFrame()
```

- **Parquet available**: read and filter by period
- **No Parquet**: returns empty DataFrame (pages handle gracefully)

The ingestor must be run to populate data before the dashboard shows content.

## Recently Added Integrations

The following integrations were added and are ready to use:

| # | Integration | Source | Pages | Ingest Command |
|---|-------------|--------|-------|---------------|
| 1 | Semi Industry Cycles (ISM proxy) | FRED `NAPM`, `NEWORDER`, `NAPMII` | Page 10 | `--source semi` (needs `FRED_API_KEY`) |
| 2 | Yield Curve Snapshot | FRED `DGS*` maturities (1M–30Y) | Page 3 | `--source fred` (needs `FRED_API_KEY`) |
| 3 | Fed Funds Futures | yfinance SOFR futures `SR3*.CME` | Page 3 | `--source fred` |
| 4 | BIS REER | `stats.bis.org` SDMX REST API | Pages 5, 6 | `--source bis` |
| 5 | Baltic Dry Index | yfinance `^BDI` | Page 4 | `--source market` |
| 6 | CFTC COT Positioning | CFTC Socrata API | Page 7 | `--source cftc` |
| 7 | EPU Index | FRED `USEPUINDXD` | Page 7 | `--source fred` (needs `FRED_API_KEY`) |
| 8 | GPR Index | Caldara-Iacoviello XLS download | Page 7 | `--source fred` |

## Future Integrations

| Integration | Why | Difficulty |
|-------------|-----|-----------|
| SIA actual revenue data | Replace ISM proxy with real semi revenue | Medium (web scrape) |
| ECB Statistical Data Warehouse | Official EUR reference rates | Low (SDMX REST) |
| OECD CLI (Composite Leading Indicators) | Better cross-country leading indicators | Low (SDMX REST) |
| IIF / EPFR Fund Flows | Actual portfolio flow data (equity/bond by region) | High (paid API) |

---

## Troubleshooting

**"No data ingested yet"** — Run `python ingestor.py --full`

**Errors for specific sources** — Run that source alone to see the error:
```bash
python ingestor.py --source fred
```

**Stale data** — Check the manifest dates:
```bash
python ingestor.py --status
```

**Start fresh** — Clean and re-ingest:
```bash
python ingestor.py --clean
python ingestor.py --full
```

**Missing pyarrow** — Install it:
```bash
pip install pyarrow
```
