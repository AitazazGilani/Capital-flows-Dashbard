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
from datetime import datetime
from pathlib import Path

import pandas as pd

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

def ingest_imf(manifest: dict, incremental: bool = False, rate_limits: dict = None):
    """Ingest IMF BOP and gold reserve data.

    NOTE: Requires a real IMF API client. Currently no live API is integrated.
    Existing Parquet files (if any) are preserved. To add live IMF data,
    implement an API client here.
    """
    print("\n[IMF] Skipping — no live IMF API integration yet.")
    print("  Existing Parquet files in data/imf/ are preserved.")
    print("  To add: implement IMF IFS/BOP API client in this function.")


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

def ingest_policy(manifest: dict, incremental: bool = False, rate_limits: dict = None):
    """Ingest policy events, central bank calendar, tariff tracker.

    NOTE: Requires real API integration (Federal Register, GDELT, etc.).
    Currently no live API is integrated. Existing Parquet files are preserved.
    """
    print("\n[Policy] Skipping — no live policy API integration yet.")
    print("  Existing Parquet files in data/policy/ are preserved.")
    print("  To add: implement Federal Register / GDELT API clients here.")


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
