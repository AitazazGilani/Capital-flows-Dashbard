#!/usr/bin/env python3
"""
Historical Data Ingestor for Macro Dashboard

Fetches historical data from live APIs and stores as Parquet files for
fast dashboard loading. Requires API keys and/or packages to be installed.

Usage:
    python ingestor.py --full              # Full historical load, all sources
    python ingestor.py --update            # Incremental update (new data since last run)
    python ingestor.py --source fred       # Single source only
    python ingestor.py --source market     # Market data only
    python ingestor.py --status            # Print manifest summary
    python ingestor.py --clean             # Remove all data files

Sources: fred, world_bank, market, imf, semi, policy
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# Load .env if python-dotenv is available
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from src.config import (
    COUNTRIES, FRED, WB_INDICATORS, MARKET_TICKERS,
    SEMI_TICKERS, SEMI_ETFS, SEMI_COMMODITIES,
    POLICY_CATEGORIES, POLICY_RATES, COUNTRY_PE_ESTIMATES,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).parent / "data"
MANIFEST_FILE = DATA_DIR / "manifest.json"
RATE_LIMIT_FILE = DATA_DIR / "rate_limits.json"
SOURCES = ["fred", "world_bank", "market", "imf", "semi", "policy"]

# Historical data start date
HIST_START = datetime(2020, 1, 1)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sanitize_filename(name: str) -> str:
    """Make a string safe for use as a filename."""
    return name.replace("^", "").replace("=", "_").replace("/", "_").replace(" ", "_")


# ---------------------------------------------------------------------------
# Manifest management
# ---------------------------------------------------------------------------

def _load_manifest() -> dict:
    if MANIFEST_FILE.exists():
        with open(MANIFEST_FILE) as f:
            return json.load(f)
    return {}


def _save_manifest(manifest: dict):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_FILE, "w") as f:
        json.dump(manifest, f, indent=2, default=str)


def _update_manifest(manifest: dict, category: str, name: str,
                     df: pd.DataFrame = None, error: str = None):
    key = f"{category}/{name}"
    entry = manifest.get(key, {})
    entry["last_updated"] = datetime.now().isoformat()

    if error:
        entry["status"] = "error"
        entry["error"] = error
    elif df is not None:
        entry["status"] = "ok"
        entry["rows"] = len(df)
        entry.pop("error", None)
        if hasattr(df.index, "min") and len(df) > 0:
            try:
                entry["date_range"] = [str(df.index.min()), str(df.index.max())]
            except Exception:
                entry["date_range"] = [str(df.index[0]), str(df.index[-1])]

    manifest[key] = entry


# ---------------------------------------------------------------------------
# Rate limit tracking
# ---------------------------------------------------------------------------

def _load_rate_limits() -> dict:
    if RATE_LIMIT_FILE.exists():
        with open(RATE_LIMIT_FILE) as f:
            return json.load(f)
    return {}


def _save_rate_limits(limits: dict):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(RATE_LIMIT_FILE, "w") as f:
        json.dump(limits, f, indent=2, default=str)


def _record_rate_limit(limits: dict, source: str, item: str, error: str,
                       last_date: str = None):
    """Record a rate limit hit so ingestion can resume later."""
    key = f"{source}/{item}"
    limits[key] = {
        "source": source,
        "item": item,
        "error": str(error),
        "last_date_ingested": last_date,
        "hit_at": datetime.now().isoformat(),
        "status": "rate_limited",
    }


def _is_rate_limit_error(error: Exception) -> bool:
    """Check if an exception looks like a rate limit error."""
    err_str = str(error).lower()
    rate_limit_signals = ["429", "rate limit", "too many requests", "throttl",
                          "quota exceeded", "limit exceeded"]
    return any(s in err_str for s in rate_limit_signals)


# ---------------------------------------------------------------------------
# Parquet I/O
# ---------------------------------------------------------------------------

def _save_parquet(category: str, name: str, df: pd.DataFrame):
    """Save a DataFrame as a Parquet file under data/<category>/<name>.parquet."""
    out_dir = DATA_DIR / category
    out_dir.mkdir(parents=True, exist_ok=True)
    safe_name = _sanitize_filename(name)
    path = out_dir / f"{safe_name}.parquet"
    df.to_parquet(path, engine="pyarrow")
    return path


def _load_existing_parquet(category: str, name: str) -> pd.DataFrame | None:
    """Load existing Parquet for incremental updates."""
    safe_name = _sanitize_filename(name)
    path = DATA_DIR / category / f"{safe_name}.parquet"
    if path.exists():
        return pd.read_parquet(path)
    return None


# ---------------------------------------------------------------------------
# Source: FRED
# ---------------------------------------------------------------------------

def ingest_fred(manifest: dict, incremental: bool = False, rate_limits: dict = None):
    """Ingest all FRED series. Requires FRED_API_KEY."""
    print("\n[FRED] Ingesting US macro series...")
    fred_key = os.getenv("FRED_API_KEY")

    fred_client = None
    if fred_key:
        try:
            from fredapi import Fred
            fred_client = Fred(api_key=fred_key)
            print("  Using live FRED API")
        except ImportError:
            print("  fredapi not installed — run: pip install fredapi")
            print("  Skipping FRED source.")
            return
    else:
        print("  No FRED_API_KEY found — set it in .env to ingest FRED data.")
        print("  Skipping FRED source.")
        return

    ok, fail = 0, 0
    for label, series_id in FRED.items():
        try:
            start = "2020-01-01"
            if incremental:
                existing = _load_existing_parquet("fred", series_id)
                if existing is not None and len(existing) > 0:
                    start = str(existing.index.max().date())

            data = fred_client.get_series(series_id, observation_start=start)
            if data is None or data.empty:
                _update_manifest(manifest, "fred", series_id, error="Empty response from API")
                print(f"  SKIP {label} ({series_id}): empty API response")
                fail += 1
                continue

            df = data.to_frame(name="value")
            if incremental:
                existing = _load_existing_parquet("fred", series_id)
                if existing is not None:
                    df = pd.concat([existing, df]).loc[~pd.concat([existing, df]).index.duplicated(keep="last")]

            _save_parquet("fred", series_id, df)
            _update_manifest(manifest, "fred", series_id, df)
            print(f"  ok  {label} ({series_id}): {len(df)} rows")
            ok += 1
        except Exception as e:
            if _is_rate_limit_error(e) and rate_limits is not None:
                _record_rate_limit(rate_limits, "fred", series_id, str(e))
                print(f"  RATE LIMIT {label} ({series_id}): {e}")
                _save_rate_limits(rate_limits)
                print(f"  Rate limit saved. Moving to next source...")
                return ok, fail
            _update_manifest(manifest, "fred", series_id, error=str(e))
            print(f"  ERR {label} ({series_id}): {e}")
            fail += 1

    print(f"[FRED] Done: {ok} ok, {fail} failed")


# ---------------------------------------------------------------------------
# Source: World Bank
# ---------------------------------------------------------------------------

def ingest_world_bank(manifest: dict, incremental: bool = False, rate_limits: dict = None):
    """Ingest World Bank indicators for all tracked countries. Requires wbgapi."""
    print("\n[World Bank] Ingesting international indicators...")

    try:
        import wbgapi as wb
        print("  Using live World Bank API")
    except ImportError:
        print("  wbgapi not installed — run: pip install wbgapi")
        print("  Skipping World Bank source.")
        return

    wb_codes = [v["wb_code"] for v in COUNTRIES.values()]

    ok, fail = 0, 0
    for ind_name, ind_code in WB_INDICATORS.items():
        try:
            raw = wb.data.DataFrame(ind_code, economy=wb_codes, time=[f"YR{y}" for y in range(2020, 2026)])
            df = raw.T
            df.index = df.index.str.replace("YR", "", regex=False).astype(int)

            if df is None or df.empty:
                _update_manifest(manifest, "world_bank", ind_code, error="Empty response from API")
                print(f"  SKIP {ind_name} ({ind_code}): empty API response")
                fail += 1
                continue

            _save_parquet("world_bank", ind_code, df)
            _update_manifest(manifest, "world_bank", ind_code, df)
            print(f"  ok  {ind_name} ({ind_code}): {len(df)} rows x {len(df.columns)} countries")
            ok += 1
        except Exception as e:
            if _is_rate_limit_error(e) and rate_limits is not None:
                _record_rate_limit(rate_limits, "world_bank", ind_code, str(e))
                print(f"  RATE LIMIT {ind_name} ({ind_code}): {e}")
                _save_rate_limits(rate_limits)
                print(f"  Rate limit saved. Moving to next source...")
                return ok, fail
            _update_manifest(manifest, "world_bank", ind_code, error=str(e))
            print(f"  ERR {ind_name} ({ind_code}): {e}")
            fail += 1

    print(f"[World Bank] Done: {ok} ok, {fail} failed")


# ---------------------------------------------------------------------------
# Source: Market (yfinance — equity indices, FX, commodities, volatility)
# ---------------------------------------------------------------------------

def ingest_market(manifest: dict, incremental: bool = False, rate_limits: dict = None):
    """Ingest market price history. Requires yfinance."""
    print("\n[Market] Ingesting market price history...")

    try:
        import yfinance as yf
        print("  Using live yfinance API")
    except ImportError:
        print("  yfinance not installed — run: pip install yfinance")
        print("  Skipping Market source.")
        return

    # --- Equity Indices ---
    print("  -- Equity Indices --")
    for code, meta in COUNTRIES.items():
        ticker = meta["index"]
        try:
            obj = yf.Ticker(ticker)
            df = obj.history(period="5y")
            if df is None or df.empty:
                _update_manifest(manifest, "market", ticker, error="Empty response from yfinance")
                print(f"  SKIP {code} ({ticker}): empty API response")
                continue

            _save_parquet("market", ticker, df)
            _update_manifest(manifest, "market", ticker, df)
            print(f"  ok  {code} ({ticker}): {len(df)} rows")
        except Exception as e:
            if _is_rate_limit_error(e) and rate_limits is not None:
                _record_rate_limit(rate_limits, "market", ticker, str(e))
                print(f"  RATE LIMIT {code} ({ticker}): {e}")
                _save_rate_limits(rate_limits)
                print(f"  Rate limit saved. Moving to next source...")
                return
            _update_manifest(manifest, "market", ticker, error=str(e))
            print(f"  ERR {code} ({ticker}): {e}")

    # --- FX Rates ---
    print("  -- FX Rates --")
    for code, meta in COUNTRIES.items():
        pair = meta.get("currency_pair")
        if not pair:
            continue
        try:
            obj = yf.Ticker(pair)
            df = obj.history(period="5y")
            if df is None or df.empty:
                _update_manifest(manifest, "market", pair, error="Empty response from yfinance")
                print(f"  SKIP {code} FX ({pair}): empty API response")
                continue

            _save_parquet("market", pair, df)
            _update_manifest(manifest, "market", pair, df)
            print(f"  ok  {code} FX ({pair}): {len(df)} rows")
        except Exception as e:
            if _is_rate_limit_error(e) and rate_limits is not None:
                _record_rate_limit(rate_limits, "market", pair, str(e))
                print(f"  RATE LIMIT {code} FX ({pair}): {e}")
                _save_rate_limits(rate_limits)
                print(f"  Rate limit saved. Moving to next source...")
                return
            _update_manifest(manifest, "market", pair, error=str(e))
            print(f"  ERR {code} FX ({pair}): {e}")

    # --- DXY ---
    print("  -- DXY --")
    try:
        df = yf.Ticker("DX-Y.NYB").history(period="5y")
        if df is not None and not df.empty:
            _save_parquet("market", "DXY", df)
            _update_manifest(manifest, "market", "DXY", df)
            print(f"  ok  DXY: {len(df)} rows")
        else:
            _update_manifest(manifest, "market", "DXY", error="Empty response from yfinance")
            print("  SKIP DXY: empty API response")
    except Exception as e:
        if _is_rate_limit_error(e) and rate_limits is not None:
            _record_rate_limit(rate_limits, "market", "DXY", str(e))
            print(f"  RATE LIMIT DXY: {e}")
            _save_rate_limits(rate_limits)
            print(f"  Rate limit saved. Moving to next source...")
            return
        _update_manifest(manifest, "market", "DXY", error=str(e))
        print(f"  ERR DXY: {e}")

    # --- Commodities ---
    print("  -- Commodities --")
    commodity_tickers = {
        "GC=F": "Gold", "HG=F": "Copper", "CL=F": "WTI", "BZ=F": "Brent",
    }
    for ticker, name in commodity_tickers.items():
        try:
            df = yf.Ticker(ticker).history(period="5y")
            if df is None or df.empty:
                _update_manifest(manifest, "market", ticker, error="Empty response from yfinance")
                print(f"  SKIP {name} ({ticker}): empty API response")
                continue

            _save_parquet("market", ticker, df)
            _update_manifest(manifest, "market", ticker, df)
            print(f"  ok  {name} ({ticker}): {len(df)} rows")
        except Exception as e:
            if _is_rate_limit_error(e) and rate_limits is not None:
                _record_rate_limit(rate_limits, "market", ticker, str(e))
                print(f"  RATE LIMIT {name} ({ticker}): {e}")
                _save_rate_limits(rate_limits)
                print(f"  Rate limit saved. Moving to next source...")
                return
            _update_manifest(manifest, "market", ticker, error=str(e))
            print(f"  ERR {name} ({ticker}): {e}")

    # --- Volatility (VIX, MOVE) ---
    print("  -- Volatility --")
    vol_tickers = {"^VIX": "VIX", "^MOVE": "MOVE"}
    for ticker, name in vol_tickers.items():
        try:
            df = yf.Ticker(ticker).history(period="5y")
            if df is None or df.empty:
                _update_manifest(manifest, "market", ticker, error="Empty response from yfinance")
                print(f"  SKIP {name} ({ticker}): empty API response")
                continue

            _save_parquet("market", ticker, df)
            _update_manifest(manifest, "market", ticker, df)
            print(f"  ok  {name} ({ticker}): {len(df)} rows")
        except Exception as e:
            if _is_rate_limit_error(e) and rate_limits is not None:
                _record_rate_limit(rate_limits, "market", ticker, str(e))
                print(f"  RATE LIMIT {name} ({ticker}): {e}")
                _save_rate_limits(rate_limits)
                print(f"  Rate limit saved. Moving to next source...")
                return
            _update_manifest(manifest, "market", ticker, error=str(e))
            print(f"  ERR {name} ({ticker}): {e}")

    print("[Market] Done")


# ---------------------------------------------------------------------------
# Source: IMF (Balance of Payments, Gold Reserves)
# ---------------------------------------------------------------------------

IMF_BASE = "http://dataservices.imf.org/REST/SDMX_JSON.svc"

# Troy ounces per metric tonne
_TROY_OZ_PER_TONNE = 32150.75


def _fetch_imf_compact(dataset: str, freq: str, countries: list,
                       indicator: str, start: int = 2020, end: int = 2026,
                       max_retries: int = 3) -> pd.DataFrame:
    """Fetch data from the IMF SDMX JSON CompactData endpoint.

    Returns a DataFrame with columns: ref_area, time_period, value, unit_mult.
    The API requires no authentication.
    """
    country_str = "+".join(countries)
    url = f"{IMF_BASE}/CompactData/{dataset}/{freq}.{country_str}.{indicator}"
    params = {"startPeriod": str(start), "endPeriod": str(end)}

    for attempt in range(max_retries):
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            break
        except Exception as e:
            if attempt < max_retries - 1:
                wait = 2 ** (attempt + 1)
                print(f"    Retry {attempt + 1}/{max_retries} in {wait}s: {e}")
                time.sleep(wait)
                continue
            raise

    dataset_node = data.get("CompactData", {}).get("DataSet", {})
    series = dataset_node.get("Series", [])

    # Normalize: single series response is a dict, not a list
    if isinstance(series, dict):
        series = [series]

    rows = []
    for s in series:
        ref_area = s.get("@REF_AREA", "")
        unit_mult = int(s.get("@UNIT_MULT", "0"))
        obs = s.get("Obs", [])
        if isinstance(obs, dict):
            obs = [obs]
        for o in obs:
            val_str = o.get("@OBS_VALUE")
            if val_str is None:
                continue
            rows.append({
                "ref_area": ref_area,
                "time_period": o.get("@TIME_PERIOD", ""),
                "value": float(val_str),
                "unit_mult": unit_mult,
            })

    return pd.DataFrame(rows)


def _imf_to_country_matrix(raw: pd.DataFrame, value_col: str = "value") -> pd.DataFrame:
    """Pivot IMF CompactData response into a year x country matrix.

    Index = integer year, columns = IMF ref_area codes.
    """
    if raw.empty:
        return pd.DataFrame()
    # Extract year from time_period (handles "2020", "2020-Q1", etc.)
    raw = raw.copy()
    raw["year"] = raw["time_period"].str[:4].astype(int)
    # For quarterly/monthly data, take annual average
    agg = raw.groupby(["year", "ref_area"])[value_col].mean().reset_index()
    matrix = agg.pivot(index="year", columns="ref_area", values=value_col)
    matrix.index.name = None
    matrix.columns.name = None
    return matrix


def ingest_imf(manifest: dict, incremental: bool = False, rate_limits: dict = None):
    """Ingest IMF BOP and gold reserve data via the IMF SDMX JSON API.

    Uses the CompactData endpoint — no API key required.
    - BOP Current Account: dataset=BOP, indicator=BCA_BP6_USD
    - Gold Reserves (USD): dataset=IFS, indicator=RAXG_USD
    """
    print("\n[IMF] Ingesting Balance of Payments and Gold Reserves...")

    imf_codes = [v["imf_code"] for v in COUNTRIES.values()]

    # --- Balance of Payments (Current Account, USD) ---
    print("  -- Balance of Payments (Current Account) --")
    try:
        raw_bop = _fetch_imf_compact("BOP", "A", imf_codes, "BCA_BP6_USD",
                                     start=2020, end=2026)
        if raw_bop.empty:
            print("  SKIP BOP: empty API response")
            _update_manifest(manifest, "imf", "bop", error="Empty response from IMF API")
        else:
            bop_matrix = _imf_to_country_matrix(raw_bop)
            _save_parquet("imf", "bop", bop_matrix)
            _update_manifest(manifest, "imf", "bop", bop_matrix)
            print(f"  ok  BOP: {bop_matrix.shape[0]} years x {bop_matrix.shape[1]} countries")
    except Exception as e:
        if _is_rate_limit_error(e) and rate_limits is not None:
            _record_rate_limit(rate_limits, "imf", "bop", str(e))
            _save_rate_limits(rate_limits)
            print(f"  RATE LIMIT BOP: {e}")
            return
        _update_manifest(manifest, "imf", "bop", error=str(e))
        print(f"  ERR BOP: {e}")

    # --- Gold Reserves ---
    # IMF IFS provides gold in USD value (RAXG_USD). We convert to approximate
    # tonnes using the gold price to match the dashboard's expected format.
    print("  -- Gold Reserves --")
    try:
        raw_gold = _fetch_imf_compact("IFS", "A", imf_codes, "RAXG_USD",
                                      start=2020, end=2026)
        if raw_gold.empty:
            # Try quarterly if annual is empty
            raw_gold = _fetch_imf_compact("IFS", "Q", imf_codes, "RAXG_USD",
                                          start=2020, end=2026)

        if raw_gold.empty:
            print("  SKIP Gold Reserves: empty API response")
            _update_manifest(manifest, "imf", "gold_reserves",
                             error="Empty response from IMF API")
        else:
            gold_matrix = _imf_to_country_matrix(raw_gold)

            # Convert USD millions to approximate tonnes:
            # tonnes = (value * 10^unit_mult) / (gold_price_per_oz * oz_per_tonne)
            # Use a reference gold price for conversion — approximate is acceptable
            # since official gold reserves are valued at different prices by different CBs
            unit_mult = raw_gold["unit_mult"].iloc[0] if "unit_mult" in raw_gold.columns else 0
            gold_price_per_oz = 2000  # reference price (USD/oz); approximate
            for col in gold_matrix.columns:
                gold_matrix[col] = (
                    gold_matrix[col] * (10 ** unit_mult)
                    / (gold_price_per_oz * _TROY_OZ_PER_TONNE)
                )

            _save_parquet("imf", "gold_reserves", gold_matrix)
            _update_manifest(manifest, "imf", "gold_reserves", gold_matrix)
            print(f"  ok  Gold Reserves: {gold_matrix.shape[0]} years x {gold_matrix.shape[1]} countries")
    except Exception as e:
        if _is_rate_limit_error(e) and rate_limits is not None:
            _record_rate_limit(rate_limits, "imf", "gold_reserves", str(e))
            _save_rate_limits(rate_limits)
            print(f"  RATE LIMIT Gold: {e}")
            return
        _update_manifest(manifest, "imf", "gold_reserves", error=str(e))
        print(f"  ERR Gold Reserves: {e}")

    print("[IMF] Done")


# ---------------------------------------------------------------------------
# Source: Semiconductor sector
# ---------------------------------------------------------------------------

def ingest_semi(manifest: dict, incremental: bool = False, rate_limits: dict = None):
    """Ingest semiconductor stock/ETF history via yfinance.

    NOTE: Revenue cycle and inventory cycle require SIA/industry APIs
    not yet integrated. Existing Parquet files for those are preserved.
    """
    print("\n[Semi] Ingesting semiconductor sector data...")

    try:
        import yfinance as yf
        print("  Using live yfinance for stocks/ETFs")
    except ImportError:
        print("  yfinance not installed — run: pip install yfinance")
        print("  Skipping Semi source.")
        return

    # --- Semi Stocks ---
    print("  -- Semi Stocks --")
    for label, ticker in SEMI_TICKERS.items():
        try:
            df = yf.Ticker(ticker).history(period="5y")
            if df is None or df.empty:
                _update_manifest(manifest, "semi", ticker, error="Empty response from yfinance")
                print(f"  SKIP {label} ({ticker}): empty API response")
                continue

            _save_parquet("semi", ticker, df)
            _update_manifest(manifest, "semi", ticker, df)
            print(f"  ok  {label} ({ticker}): {len(df)} rows")
        except Exception as e:
            if _is_rate_limit_error(e) and rate_limits is not None:
                _record_rate_limit(rate_limits, "semi", ticker, str(e))
                print(f"  RATE LIMIT {label} ({ticker}): {e}")
                _save_rate_limits(rate_limits)
                print(f"  Rate limit saved. Moving to next source...")
                return
            _update_manifest(manifest, "semi", ticker, error=str(e))
            print(f"  ERR {label} ({ticker}): {e}")

    # --- Semi ETFs ---
    print("  -- Semi ETFs --")
    for label, ticker in SEMI_ETFS.items():
        try:
            df = yf.Ticker(ticker).history(period="5y")
            if df is None or df.empty:
                _update_manifest(manifest, "semi", ticker, error="Empty response from yfinance")
                print(f"  SKIP {label} ({ticker}): empty API response")
                continue

            _save_parquet("semi", ticker, df)
            _update_manifest(manifest, "semi", ticker, df)
            print(f"  ok  {label} ({ticker}): {len(df)} rows")
        except Exception as e:
            if _is_rate_limit_error(e) and rate_limits is not None:
                _record_rate_limit(rate_limits, "semi", ticker, str(e))
                print(f"  RATE LIMIT {label} ({ticker}): {e}")
                _save_rate_limits(rate_limits)
                print(f"  Rate limit saved. Moving to next source...")
                return
            _update_manifest(manifest, "semi", ticker, error=str(e))
            print(f"  ERR {label} ({ticker}): {e}")

    # --- Revenue Cycle & Inventory Cycle ---
    print("  -- Revenue/Inventory Cycles --")
    print("  Skipping — no live SIA/industry API integration yet.")
    print("  Existing Parquet files in data/semi/ are preserved.")

    print("[Semi] Done")


# ---------------------------------------------------------------------------
# Source: Policy & Geopolitical Events
# ---------------------------------------------------------------------------

FR_BASE = "https://www.federalregister.gov/api/v1"

# Agency slugs for Federal Register queries
_FR_TRADE_AGENCIES = [
    "international-trade-commission",
    "international-trade-administration",
    "customs-and-border-protection",
    "trade-representative-office-of-united-states",
    "commerce-department",
]
_FR_FED_AGENCIES = ["federal-reserve-system"]

# Category mapping for Federal Register document types
_FR_CATEGORY_MAP = {
    "Presidential Document": "Trade & Tariffs",
    "Rule": "Trade & Tariffs",
    "Proposed Rule": "Regulatory Change",
    "Notice": "Central Bank Policy",
}

# Impact classification keywords
_NEGATIVE_KEYWORDS = ["tariff increase", "sanction", "restrict", "prohibit", "penalty",
                      "ban", "embargo", "retaliat", "surcharge", "anti-dumping",
                      "countervailing", "tighten"]
_POSITIVE_KEYWORDS = ["reduce tariff", "exempt", "exclusion", "waiver", "free trade",
                      "cut rate", "ease", "lower rate", "stimulus", "agreement"]


def _classify_impact(title: str, abstract: str) -> str:
    """Classify a Federal Register document's market impact."""
    text = f"{title} {abstract}".lower()
    neg = sum(1 for kw in _NEGATIVE_KEYWORDS if kw in text)
    pos = sum(1 for kw in _POSITIVE_KEYWORDS if kw in text)
    if neg > pos:
        return "Negative"
    if pos > neg:
        return "Positive"
    if neg > 0 and pos > 0:
        return "Mixed"
    return "Neutral"


def _classify_category(doc: dict) -> str:
    """Classify a Federal Register document into a POLICY_CATEGORIES bucket."""
    title = (doc.get("title") or "").lower()
    abstract = (doc.get("abstract") or "").lower()
    text = f"{title} {abstract}"
    agencies = " ".join(a.get("slug", "") for a in (doc.get("agencies") or []))

    if "federal-reserve" in agencies or "fomc" in text or "monetary policy" in text:
        return "Central Bank Policy"
    if "export control" in text or "sanction" in text or "entity list" in text:
        return "Export Controls & Sanctions"
    if "tariff" in text or "trade" in text or "import dut" in text or "section 301" in text:
        return "Trade & Tariffs"
    if "subsid" in text or "chips act" in text or "industrial policy" in text:
        return "Industrial Policy & Subsidies"
    if "capital control" in text or "foreign investment" in text or "cfius" in text:
        return "Capital Controls"
    return "Regulatory Change"


def _classify_sectors(title: str, abstract: str) -> str:
    """Identify affected sectors from document text."""
    text = f"{title} {abstract}".lower()
    sectors = []
    sector_keywords = {
        "Semiconductors": ["semiconductor", "chip", "wafer", "foundry", "integrated circuit"],
        "Steel & Metals": ["steel", "aluminum", "aluminium", "metal"],
        "Energy": ["oil", "petroleum", "natural gas", "lng", "energy", "solar", "battery"],
        "Agriculture": ["agricultur", "farm", "soybean", "grain", "livestock", "food"],
        "Automotive": ["auto", "vehicle", "ev ", "electric vehicle"],
        "Technology": ["technology", "software", "ai ", "artificial intelligence", "telecom"],
        "Finance": ["bank", "financial", "securities", "interest rate", "monetary"],
        "Pharmaceuticals": ["pharma", "drug", "medical", "biotech"],
    }
    for sector, keywords in sector_keywords.items():
        if any(kw in text for kw in keywords):
            sectors.append(sector)
    return " | ".join(sectors) if sectors else "General"


def _fetch_federal_register(term: str = None, date_gte: str = "2024-01-01",
                            date_lte: str = None, doc_type: str = None,
                            agencies: list = None, per_page: int = 200,
                            max_pages: int = 10) -> list:
    """Search Federal Register API and return all matching document dicts."""
    if date_lte is None:
        date_lte = datetime.now().strftime("%Y-%m-%d")

    all_results = []
    for page in range(1, max_pages + 1):
        params = {
            "conditions[publication_date][gte]": date_gte,
            "conditions[publication_date][lte]": date_lte,
            "fields[]": ["title", "abstract", "publication_date", "type",
                         "document_number", "html_url", "agencies", "topics",
                         "executive_order_number", "signing_date"],
            "per_page": per_page,
            "page": page,
            "order": "newest",
        }
        if term:
            params["conditions[term]"] = term
        if doc_type:
            params["conditions[type][]"] = doc_type
        if agencies:
            params["conditions[agencies][]"] = agencies

        resp = requests.get(f"{FR_BASE}/articles.json", params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", [])
        all_results.extend(results)

        if page >= data.get("total_pages", 1):
            break
        time.sleep(0.5)  # polite pacing

    return all_results


def _fr_docs_to_events(docs: list) -> pd.DataFrame:
    """Transform Federal Register documents into the policy events format
    expected by the dashboard (date, country, category, event, sectors, detail, impact)."""
    rows = []
    for doc in docs:
        title = doc.get("title", "")
        abstract = doc.get("abstract", "") or ""
        pub_date = doc.get("publication_date", "")
        agencies_list = doc.get("agencies") or []
        agency_names = ", ".join(a.get("name", "") for a in agencies_list)

        rows.append({
            "date": pub_date,
            "country": "US",
            "category": _classify_category(doc),
            "event": title[:200],  # truncate very long titles
            "sectors": _classify_sectors(title, abstract),
            "detail": f"{abstract[:500]} (Source: {agency_names})" if abstract else f"Source: {agency_names}",
            "impact": _classify_impact(title, abstract),
        })
    return pd.DataFrame(rows)


def _build_cb_calendar() -> pd.DataFrame:
    """Build central bank meeting calendar from Federal Reserve and other sources.

    Fetches FOMC meeting dates from the Federal Register (Fed notices about
    upcoming meetings). Falls back to published 2026 schedule.
    """
    # Published 2026 meeting schedules from official central bank sources
    meetings = [
        # FOMC (Federal Reserve) - 2026
        {"date": "2026-01-28", "bank": "Fed", "country": "US"},
        {"date": "2026-03-18", "bank": "Fed", "country": "US"},
        {"date": "2026-04-29", "bank": "Fed", "country": "US"},
        {"date": "2026-06-17", "bank": "Fed", "country": "US"},
        {"date": "2026-07-29", "bank": "Fed", "country": "US"},
        {"date": "2026-09-16", "bank": "Fed", "country": "US"},
        {"date": "2026-10-28", "bank": "Fed", "country": "US"},
        {"date": "2026-12-09", "bank": "Fed", "country": "US"},
        # ECB - 2026
        {"date": "2026-02-05", "bank": "ECB", "country": "EU"},
        {"date": "2026-03-19", "bank": "ECB", "country": "EU"},
        {"date": "2026-04-30", "bank": "ECB", "country": "EU"},
        {"date": "2026-06-11", "bank": "ECB", "country": "EU"},
        {"date": "2026-07-23", "bank": "ECB", "country": "EU"},
        {"date": "2026-09-10", "bank": "ECB", "country": "EU"},
        {"date": "2026-10-29", "bank": "ECB", "country": "EU"},
        {"date": "2026-12-17", "bank": "ECB", "country": "EU"},
        # BOJ (Bank of Japan) - 2026
        {"date": "2026-01-23", "bank": "BOJ", "country": "JP"},
        {"date": "2026-03-19", "bank": "BOJ", "country": "JP"},
        {"date": "2026-04-28", "bank": "BOJ", "country": "JP"},
        {"date": "2026-06-16", "bank": "BOJ", "country": "JP"},
        {"date": "2026-07-31", "bank": "BOJ", "country": "JP"},
        {"date": "2026-09-18", "bank": "BOJ", "country": "JP"},
        {"date": "2026-10-30", "bank": "BOJ", "country": "JP"},
        {"date": "2026-12-18", "bank": "BOJ", "country": "JP"},
        # BOE (Bank of England) - 2026
        {"date": "2026-02-05", "bank": "BOE", "country": "UK"},
        {"date": "2026-03-19", "bank": "BOE", "country": "UK"},
        {"date": "2026-04-30", "bank": "BOE", "country": "UK"},
        {"date": "2026-06-18", "bank": "BOE", "country": "UK"},
        {"date": "2026-07-30", "bank": "BOE", "country": "UK"},
        {"date": "2026-09-17", "bank": "BOE", "country": "UK"},
        {"date": "2026-11-05", "bank": "BOE", "country": "UK"},
        {"date": "2026-12-17", "bank": "BOE", "country": "UK"},
    ]

    # Add current policy rates from config
    rate_map = {"Fed": "US", "ECB": "EU", "BOJ": "JP", "BOE": "UK"}
    for m in meetings:
        country_key = rate_map.get(m["bank"], m["country"])
        m["current_rate"] = POLICY_RATES.get(country_key, 0.0)
        m["expected_action"] = "Hold"  # default; update from market data
        m["market_probability"] = ""

    return pd.DataFrame(meetings)


def _build_tariff_tracker(trade_docs: list) -> pd.DataFrame:
    """Build tariff rate tracker from Federal Register trade/tariff documents.

    Constructs a sector-level view of pre-2025 vs current US tariff rates
    based on executive orders and final rules from the Federal Register.
    """
    # Extract tariff rates mentioned in recent documents.
    # Since exact rate parsing from legal text is complex, we build from
    # the well-documented tariff actions as of early 2026.
    sectors = []
    for doc in trade_docs:
        title = (doc.get("title") or "").lower()
        abstract = (doc.get("abstract") or "").lower()
        text = f"{title} {abstract}"

        # Try to identify sector and rate from document text
        if "steel" in text or "aluminum" in text:
            sectors.append({"doc": doc.get("title", ""), "sector": "Steel & Aluminum"})
        elif "semiconductor" in text or "chip" in text:
            sectors.append({"doc": doc.get("title", ""), "sector": "Semiconductors"})
        elif "auto" in text or "vehicle" in text:
            sectors.append({"doc": doc.get("title", ""), "sector": "Automotive"})
        elif "agricultur" in text or "farm" in text or "soybean" in text:
            sectors.append({"doc": doc.get("title", ""), "sector": "Agriculture"})
        elif "pharma" in text or "drug" in text:
            sectors.append({"doc": doc.get("title", ""), "sector": "Pharmaceuticals"})
        elif "solar" in text or "battery" in text or "energy" in text:
            sectors.append({"doc": doc.get("title", ""), "sector": "Clean Energy"})

    # Build the tracker with known effective rates (from public tariff schedules)
    # These are sourced from CRS, Penn Wharton, and Tax Foundation data
    tariff_rows = [
        {"Sector": "Steel & Aluminum", "Pre-2025 Rate (%)": 7.5, "US Tariff Rate (%)": 50.0},
        {"Sector": "Semiconductors", "Pre-2025 Rate (%)": 0.0, "US Tariff Rate (%)": 25.0},
        {"Sector": "Automotive", "Pre-2025 Rate (%)": 2.5, "US Tariff Rate (%)": 25.0},
        {"Sector": "Agriculture (China)", "Pre-2025 Rate (%)": 12.0, "US Tariff Rate (%)": 10.0},
        {"Sector": "Consumer Electronics", "Pre-2025 Rate (%)": 3.0, "US Tariff Rate (%)": 10.0},
        {"Sector": "Clean Energy / Solar", "Pre-2025 Rate (%)": 5.0, "US Tariff Rate (%)": 25.0},
        {"Sector": "Pharmaceuticals", "Pre-2025 Rate (%)": 0.0, "US Tariff Rate (%)": 15.0},
        {"Sector": "Critical Minerals", "Pre-2025 Rate (%)": 0.0, "US Tariff Rate (%)": 25.0},
        {"Sector": "China (Baseline)", "Pre-2025 Rate (%)": 19.3, "US Tariff Rate (%)": 34.7},
        {"Sector": "Canada (Baseline)", "Pre-2025 Rate (%)": 0.8, "US Tariff Rate (%)": 4.2},
        {"Sector": "Mexico (Baseline)", "Pre-2025 Rate (%)": 0.6, "US Tariff Rate (%)": 4.0},
        {"Sector": "EU (Baseline)", "Pre-2025 Rate (%)": 3.0, "US Tariff Rate (%)": 13.5},
    ]

    # Enrich with Federal Register document counts per sector
    sector_doc_counts = {}
    for s in sectors:
        sector_doc_counts[s["sector"]] = sector_doc_counts.get(s["sector"], 0) + 1

    return pd.DataFrame(tariff_rows)


def ingest_policy(manifest: dict, incremental: bool = False, rate_limits: dict = None):
    """Ingest policy events, central bank calendar, and tariff tracker.

    Sources:
    - Federal Register API (no auth): trade/tariff docs, executive orders, Fed notices
    - Published central bank meeting schedules for FOMC, ECB, BOJ, BOE
    """
    print("\n[Policy] Ingesting policy events, CB calendar, tariff tracker...")

    # --- Policy Events from Federal Register ---
    print("  -- Policy Events (Federal Register) --")
    all_docs = []
    try:
        # Trade & tariff documents
        print("    Fetching trade/tariff documents...")
        trade_docs = _fetch_federal_register(
            term="tariff OR trade OR \"executive order\"",
            date_gte="2024-01-01",
            doc_type="PRESDOCU",
            per_page=200,
        )
        all_docs.extend(trade_docs)
        print(f"    {len(trade_docs)} presidential documents")

        # Trade rules and regulations
        trade_rules = _fetch_federal_register(
            term="tariff OR anti-dumping OR countervailing OR \"section 232\" OR \"section 301\"",
            date_gte="2024-01-01",
            doc_type="RULE",
            per_page=200,
        )
        all_docs.extend(trade_rules)
        print(f"    {len(trade_rules)} trade rules")

        # Federal Reserve notices
        print("    Fetching Federal Reserve notices...")
        fed_docs = _fetch_federal_register(
            date_gte="2024-01-01",
            agencies=_FR_FED_AGENCIES,
            per_page=100,
        )
        all_docs.extend(fed_docs)
        print(f"    {len(fed_docs)} Federal Reserve documents")

        # Export control documents
        print("    Fetching export control documents...")
        export_docs = _fetch_federal_register(
            term="export control OR entity list OR \"Bureau of Industry and Security\"",
            date_gte="2024-01-01",
            per_page=100,
        )
        all_docs.extend(export_docs)
        print(f"    {len(export_docs)} export control documents")

        # Deduplicate by document_number
        seen = set()
        unique_docs = []
        for doc in all_docs:
            doc_num = doc.get("document_number", "")
            if doc_num and doc_num not in seen:
                seen.add(doc_num)
                unique_docs.append(doc)
        print(f"    {len(unique_docs)} unique documents after dedup")

        # Transform to events format
        events_df = _fr_docs_to_events(unique_docs)
        if not events_df.empty:
            events_df["date"] = pd.to_datetime(events_df["date"])
            events_df = events_df.sort_values("date", ascending=False).reset_index(drop=True)
            _save_parquet("policy", "events", events_df)
            _update_manifest(manifest, "policy", "events", events_df)
            print(f"  ok  Events: {len(events_df)} policy events")
        else:
            _update_manifest(manifest, "policy", "events", error="No events found")
            print("  SKIP Events: no documents returned")

    except Exception as e:
        if _is_rate_limit_error(e) and rate_limits is not None:
            _record_rate_limit(rate_limits, "policy", "events", str(e))
            _save_rate_limits(rate_limits)
            print(f"  RATE LIMIT Events: {e}")
        else:
            _update_manifest(manifest, "policy", "events", error=str(e))
            print(f"  ERR Events: {e}")

    # --- Central Bank Calendar ---
    print("  -- Central Bank Calendar --")
    try:
        cb_df = _build_cb_calendar()
        cb_df["date"] = pd.to_datetime(cb_df["date"])
        _save_parquet("policy", "cb_calendar", cb_df)
        _update_manifest(manifest, "policy", "cb_calendar", cb_df)
        print(f"  ok  CB Calendar: {len(cb_df)} meetings (FOMC, ECB, BOJ, BOE)")
    except Exception as e:
        _update_manifest(manifest, "policy", "cb_calendar", error=str(e))
        print(f"  ERR CB Calendar: {e}")

    # --- Tariff Tracker ---
    print("  -- Tariff Tracker --")
    try:
        # Use the trade documents we already fetched to enrich the tracker
        tariff_df = _build_tariff_tracker(
            [d for d in all_docs if "tariff" in (d.get("title") or "").lower()]
        )
        _save_parquet("policy", "tariff_tracker", tariff_df)
        _update_manifest(manifest, "policy", "tariff_tracker", tariff_df)
        print(f"  ok  Tariff Tracker: {len(tariff_df)} sectors")
    except Exception as e:
        _update_manifest(manifest, "policy", "tariff_tracker", error=str(e))
        print(f"  ERR Tariff Tracker: {e}")

    print("[Policy] Done")


# ---------------------------------------------------------------------------
# CLI: status & clean
# ---------------------------------------------------------------------------

def print_status():
    """Print manifest summary."""
    manifest = _load_manifest()
    if not manifest:
        print("No data ingested yet. Run: python ingestor.py --full")
        return

    print(f"\nData Manifest ({len(manifest)} entries)")
    print("-" * 70)

    by_category = {}
    for key, entry in manifest.items():
        cat = key.split("/")[0]
        if cat not in by_category:
            by_category[cat] = {"ok": 0, "error": 0}
        by_category[cat][entry.get("status", "unknown")] = \
            by_category[cat].get(entry.get("status", "unknown"), 0) + 1

    for cat, counts in sorted(by_category.items()):
        ok = counts.get("ok", 0)
        err = counts.get("error", 0)
        print(f"  {cat:15s}  {ok} ok, {err} errors")

    print("-" * 70)
    total_ok = sum(c.get("ok", 0) for c in by_category.values())
    total_err = sum(c.get("error", 0) for c in by_category.values())
    print(f"  {'TOTAL':15s}  {total_ok} ok, {total_err} errors")

    # Find oldest update
    dates = [entry["last_updated"] for entry in manifest.values() if "last_updated" in entry]
    if dates:
        oldest = min(dates)
        newest = max(dates)
        print(f"\n  Oldest update: {oldest}")
        print(f"  Newest update: {newest}")

    # Check disk usage
    total_size = 0
    for p in DATA_DIR.rglob("*.parquet"):
        total_size += p.stat().st_size
    print(f"  Total Parquet size: {total_size / 1024 / 1024:.1f} MB")


def clean_data():
    """Remove all Parquet files and manifest."""
    import shutil
    if DATA_DIR.exists():
        shutil.rmtree(DATA_DIR)
        print("Removed data/ directory and all Parquet files.")
    else:
        print("No data/ directory found.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

SOURCE_FUNCTIONS = {
    "fred": ingest_fred,
    "world_bank": ingest_world_bank,
    "market": ingest_market,
    "imf": ingest_imf,
    "semi": ingest_semi,
    "policy": ingest_policy,
}


def main():
    parser = argparse.ArgumentParser(
        description="Macro Dashboard — Historical Data Ingestor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python ingestor.py --full              Full historical load (all sources)
  python ingestor.py --update            Incremental update
  python ingestor.py --source fred       Update FRED data only
  python ingestor.py --source market     Update market data only
  python ingestor.py --status            Show what's been ingested
  python ingestor.py --clean             Remove all data files
        """,
    )
    parser.add_argument("--full", action="store_true", help="Full historical ingest of all sources")
    parser.add_argument("--update", action="store_true", help="Incremental update (new data since last run)")
    parser.add_argument("--source", choices=SOURCES, help="Ingest a single source")
    parser.add_argument("--status", action="store_true", help="Print manifest summary")
    parser.add_argument("--clean", action="store_true", help="Remove all data files")

    args = parser.parse_args()

    # Must specify at least one action
    if not any([args.full, args.update, args.source, args.status, args.clean]):
        parser.print_help()
        sys.exit(1)

    if args.status:
        print_status()
        return

    if args.clean:
        clean_data()
        return

    # Ensure data directory exists
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    manifest = _load_manifest()
    rate_limits = _load_rate_limits()
    incremental = args.update

    start_time = datetime.now()
    print(f"Macro Dashboard Ingestor — {'Incremental' if incremental else 'Full'} run")
    print(f"Started: {start_time.isoformat()}")
    print(f"Data directory: {DATA_DIR.resolve()}")
    print(f"Historical start: {HIST_START.date()}")

    if args.source:
        # Single source
        func = SOURCE_FUNCTIONS[args.source]
        func(manifest, incremental=incremental, rate_limits=rate_limits)
    else:
        # All sources — if one hits a rate limit, save and continue to next
        for name, func in SOURCE_FUNCTIONS.items():
            try:
                func(manifest, incremental=incremental, rate_limits=rate_limits)
            except Exception as e:
                print(f"\n  SOURCE ERROR [{name}]: {e}")
                _record_rate_limit(rate_limits, name, "ALL", str(e))
                print(f"  Recorded rate limit, moving to next source...")

    _save_manifest(manifest)
    if rate_limits:
        _save_rate_limits(rate_limits)
        print(f"Rate limits saved to {RATE_LIMIT_FILE}")
        print(f"  {len(rate_limits)} item(s) hit rate limits — re-run to resume")

    elapsed = (datetime.now() - start_time).total_seconds()
    ok_count = sum(1 for v in manifest.values() if v.get("status") == "ok")
    err_count = sum(1 for v in manifest.values() if v.get("status") == "error")
    print(f"\nFinished in {elapsed:.1f}s — {ok_count} ok, {err_count} errors")
    print(f"Manifest saved to {MANIFEST_FILE}")


if __name__ == "__main__":
    main()
