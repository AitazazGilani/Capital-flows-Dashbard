#!/usr/bin/env python3
"""
Historical Data Ingestor for Macro Dashboard

Fetches historical data from APIs (or generates mock data when keys are
unavailable) and stores as Parquet files for fast dashboard loading.

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
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
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
SOURCES = ["fred", "world_bank", "market", "imf", "semi", "policy"]


# ---------------------------------------------------------------------------
# Mock data helpers (same generators as data_fetcher.py, no Streamlit dep)
# ---------------------------------------------------------------------------

def _date_index(days=1825):
    end = datetime(2026, 2, 6)
    start = end - timedelta(days=days)
    return pd.bdate_range(start=start, end=end)


def _random_walk(start, drift=0.0002, vol=0.01, n=1300, seed=None):
    rng = np.random.RandomState(seed)
    returns = rng.normal(drift, vol, n)
    prices = start * np.exp(np.cumsum(returns))
    return prices


def _mean_reverting(center, vol=0.1, n=1300, seed=None):
    rng = np.random.RandomState(seed)
    series = np.zeros(n)
    series[0] = center
    for i in range(1, n):
        series[i] = series[i - 1] + 0.05 * (center - series[i - 1]) + rng.normal(0, vol)
    return series


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

def ingest_fred(manifest: dict, incremental: bool = False):
    """Ingest all FRED series."""
    print("\n[FRED] Ingesting US macro series...")
    fred_key = os.getenv("FRED_API_KEY")

    fred_client = None
    if fred_key:
        try:
            from fredapi import Fred
            fred_client = Fred(api_key=fred_key)
            print("  Using live FRED API")
        except ImportError:
            print("  fredapi not installed, falling back to mock")
    else:
        print("  No FRED_API_KEY found, using mock data")

    # Mock parameters (same seeds as data_fetcher.py)
    mock_params = {
        "DFF": (5.33, 0.05, 108), "DGS10": (4.2, 0.08, 109),
        "DGS2": (4.5, 0.1, 110), "T10Y2Y": (-0.3, 0.08, 111),
        "DFII10": (1.8, 0.1, 112), "T10YIE": (2.3, 0.05, 113),
        "WALCL": (7800000, 50000, 114), "RRPONTSYD": (500000, 30000, 115),
        "WTREGEN": (750000, 40000, 116), "WM2NS": (20800, 100, 117),
        "BAMLH0A0HYM2": (3.8, 0.3, 118), "BAMLC0A0CM": (1.2, 0.1, 119),
        "NFCI": (-0.3, 0.1, 120), "ICSA": (220000, 8000, 121),
        "CCSA": (1850000, 30000, 122), "UMCSENT": (68, 3, 123),
        "CPIAUCSL": (310, 0.5, 124), "UNRATE": (3.8, 0.1, 125),
        "PSAVERT": (4.5, 0.5, 126), "INDPRO": (103, 0.3, 127),
        "USALOLITONOSTSAM": (99.5, 0.2, 128),
    }

    ok, fail = 0, 0
    for label, series_id in FRED.items():
        try:
            if fred_client:
                # --- Live API ---
                start = "2000-01-01"
                if incremental:
                    existing = _load_existing_parquet("fred", series_id)
                    if existing is not None and len(existing) > 0:
                        start = str(existing.index.max().date())
                data = fred_client.get_series(series_id, observation_start=start)
                df = data.to_frame(name="value")
                if incremental and existing is not None:
                    df = pd.concat([existing, df]).loc[~pd.concat([existing, df]).index.duplicated(keep="last")]
            else:
                # --- Mock data ---
                idx = _date_index(1825)
                n = len(idx)
                center, vol, seed = mock_params.get(series_id, (100, 1, 199))
                values = _mean_reverting(center, vol=vol, n=n, seed=seed)

                # Add trends for specific series
                if series_id == "WALCL":
                    values = values + np.linspace(0, -500000, n)
                elif series_id == "RRPONTSYD":
                    values = np.clip(values + np.linspace(1500000, 0, n), 0, None)
                elif series_id == "CPIAUCSL":
                    values = values + np.linspace(-10, 10, n)

                df = pd.DataFrame({"value": values}, index=idx)

            _save_parquet("fred", series_id, df)
            _update_manifest(manifest, "fred", series_id, df)
            print(f"  ok  {label} ({series_id}): {len(df)} rows")
            ok += 1
        except Exception as e:
            _update_manifest(manifest, "fred", series_id, error=str(e))
            print(f"  ERR {label} ({series_id}): {e}")
            fail += 1

    print(f"[FRED] Done: {ok} ok, {fail} failed")


# ---------------------------------------------------------------------------
# Source: World Bank
# ---------------------------------------------------------------------------

def ingest_world_bank(manifest: dict, incremental: bool = False):
    """Ingest World Bank indicators for all tracked countries."""
    print("\n[World Bank] Ingesting international indicators...")

    wb_available = False
    try:
        import wbgapi as wb
        wb_available = True
        print("  Using live World Bank API")
    except ImportError:
        print("  wbgapi not installed, using mock data")

    wb_codes = [v["wb_code"] for v in COUNTRIES.values()]

    # Mock base values (same as data_fetcher.py)
    base_values = {
        "BN.CAB.XOKA.GD.ZS": {"US": -3.0, "EU": 2.5, "UK": -3.5, "JP": 3.5, "CN": 1.5,
                                "CA": -1.0, "AU": -2.0, "CH": 8.0, "KR": 4.0, "IN": -1.5,
                                "BR": -2.5, "MX": -1.0, "DE": 7.0},
        "NE.RSB.GNFS.CD": {"US": -8e11, "EU": 3e11, "UK": -2e11, "JP": 1.5e11, "CN": 5e11,
                            "CA": -5e10, "AU": -3e10, "CH": 5e10, "KR": 6e10, "IN": -1.5e11,
                            "BR": -3e10, "MX": -1e10, "DE": 2.5e11},
        "BX.KLT.DINV.CD.WD": {"US": 3.5e11, "EU": 2e11, "UK": 1.5e11, "JP": 3e10, "CN": 1.8e11,
                                "CA": 5e10, "AU": 6e10, "CH": 4e10, "KR": 1.5e10, "IN": 5e10,
                                "BR": 7e10, "MX": 3.5e10, "DE": 4e10},
        "BM.KLT.DINV.CD.WD": {"US": 4e11, "EU": 2.5e11, "UK": 1.2e11, "JP": 1.5e11, "CN": 1.2e11,
                                "CA": 8e10, "AU": 3e10, "CH": 1e11, "KR": 5e10, "IN": 2e10,
                                "BR": 1.5e10, "MX": 1e10, "DE": 1e11},
        "FI.RES.TOTL.CD": {"US": 2.4e11, "EU": 3e11, "UK": 1.8e11, "JP": 1.3e12, "CN": 3.2e12,
                            "CA": 1e11, "AU": 6e10, "CH": 9e11, "KR": 4.2e11, "IN": 6e11,
                            "BR": 3.5e11, "MX": 2e11, "DE": 2.5e11},
        "GC.DOD.TOTL.GD.ZS": {"US": 120, "EU": 90, "UK": 100, "JP": 260, "CN": 75,
                                "CA": 105, "AU": 55, "CH": 40, "KR": 50, "IN": 85,
                                "BR": 90, "MX": 55, "DE": 65},
        "GC.BAL.CASH.GD.ZS": {"US": -6.0, "EU": -3.0, "UK": -5.0, "JP": -8.0, "CN": -5.0,
                                "CA": -2.0, "AU": -1.0, "CH": 1.0, "KR": 0.5, "IN": -7.0,
                                "BR": -5.0, "MX": -3.5, "DE": 0.5},
        "NY.GDP.MKTP.CD": {"US": 2.5e13, "EU": 1.4e13, "UK": 3.1e12, "JP": 4.2e12, "CN": 1.8e13,
                            "CA": 2e12, "AU": 1.7e12, "CH": 8e11, "KR": 1.7e12, "IN": 3.5e12,
                            "BR": 2e12, "MX": 1.3e12, "DE": 4.1e12},
        "NY.GDP.MKTP.KD.ZG": {"US": 2.5, "EU": 1.5, "UK": 1.3, "JP": 1.0, "CN": 5.5,
                                "CA": 2.0, "AU": 2.5, "CH": 1.5, "KR": 2.5, "IN": 6.5,
                                "BR": 1.5, "MX": 2.0, "DE": 1.0},
        "FP.CPI.TOTL.ZG": {"US": 3.5, "EU": 2.8, "UK": 4.0, "JP": 3.0, "CN": 0.5,
                            "CA": 3.2, "AU": 4.5, "CH": 1.5, "KR": 3.5, "IN": 5.5,
                            "BR": 5.0, "MX": 5.5, "DE": 3.0},
        "SL.UEM.TOTL.ZS": {"US": 3.8, "EU": 6.5, "UK": 4.0, "JP": 2.6, "CN": 5.2,
                            "CA": 5.5, "AU": 3.8, "CH": 2.0, "KR": 3.0, "IN": 7.5,
                            "BR": 8.0, "MX": 3.5, "DE": 3.2},
    }
    wb_code_to_short = {v["wb_code"]: k for k, v in COUNTRIES.items()}

    ok, fail = 0, 0
    for ind_name, ind_code in WB_INDICATORS.items():
        try:
            if wb_available:
                # --- Live API ---
                raw = wb.data.DataFrame(ind_code, economy=wb_codes, time=range(2000, 2026))
                df = raw.T
                df.index = df.index.astype(int)
            else:
                # --- Mock data ---
                years = list(range(2000, 2026))
                rng = np.random.RandomState(hash(ind_code) % 2**31)
                defaults = base_values.get(ind_code, {})
                data = {}
                for wb_c in wb_codes:
                    short = wb_code_to_short.get(wb_c, wb_c)
                    base = defaults.get(short, 0)
                    scale = abs(base) * 0.05 if base != 0 else 1
                    current = base
                    vals = []
                    for _ in years:
                        current = current + rng.normal(0, scale)
                        vals.append(current)
                    data[wb_c] = vals
                df = pd.DataFrame(data, index=years)

            _save_parquet("world_bank", ind_code, df)
            _update_manifest(manifest, "world_bank", ind_code, df)
            print(f"  ok  {ind_name} ({ind_code}): {len(df)} rows x {len(df.columns)} countries")
            ok += 1
        except Exception as e:
            _update_manifest(manifest, "world_bank", ind_code, error=str(e))
            print(f"  ERR {ind_name} ({ind_code}): {e}")
            fail += 1

    print(f"[World Bank] Done: {ok} ok, {fail} failed")


# ---------------------------------------------------------------------------
# Source: Market (yfinance — equity indices, FX, commodities, volatility)
# ---------------------------------------------------------------------------

def ingest_market(manifest: dict, incremental: bool = False):
    """Ingest market price history — equity indices, FX, commodities, volatility, DXY."""
    print("\n[Market] Ingesting market price history...")

    yf_available = False
    try:
        import yfinance as yf
        yf_available = True
        print("  Using live yfinance API")
    except ImportError:
        print("  yfinance not installed, using mock data")

    # --- Equity Indices ---
    print("  -- Equity Indices --")
    index_seeds = {
        "^GSPC": (4200, 42), "^STOXX50E": (4100, 43), "^FTSE": (7200, 44),
        "^N225": (32000, 45), "000001.SS": (3100, 46), "^GSPTSE": (19000, 47),
        "^AXJO": (7000, 48), "^SSMI": (11000, 49), "^KS11": (2400, 50),
        "^BSESN": (60000, 51), "^BVSP": (115000, 52), "^MXX": (52000, 53),
        "^GDAXI": (15000, 54),
    }
    for code, meta in COUNTRIES.items():
        ticker = meta["index"]
        try:
            if yf_available:
                obj = yf.Ticker(ticker)
                df = obj.history(period="5y")
                if df.empty:
                    raise ValueError("No data returned from yfinance")
            else:
                start_price, seed = index_seeds.get(ticker, (1000, 99))
                idx = _date_index(1825)
                n = len(idx)
                close = _random_walk(start_price, drift=0.0003, vol=0.012, n=n, seed=seed)
                rng = np.random.RandomState(seed)
                df = pd.DataFrame({
                    "Open": close * (1 + rng.normal(0, 0.002, n)),
                    "High": close * (1 + np.abs(rng.normal(0, 0.008, n))),
                    "Low": close * (1 - np.abs(rng.normal(0, 0.008, n))),
                    "Close": close,
                    "Volume": (rng.lognormal(20, 1, n)).astype(int),
                }, index=idx)

            _save_parquet("market", ticker, df)
            _update_manifest(manifest, "market", ticker, df)
            print(f"  ok  {code} ({ticker}): {len(df)} rows")
        except Exception as e:
            _update_manifest(manifest, "market", ticker, error=str(e))
            print(f"  ERR {code} ({ticker}): {e}")

    # --- FX Rates ---
    print("  -- FX Rates --")
    fx_seeds = {
        "EURUSD=X": (1.08, 70), "GBPUSD=X": (1.26, 71), "USDJPY=X": (148, 72),
        "USDCNY=X": (7.20, 73), "USDCAD=X": (1.36, 74), "AUDUSD=X": (0.66, 75),
        "USDCHF=X": (0.88, 76), "USDKRW=X": (1320, 77), "USDINR=X": (83.5, 78),
        "USDBRL=X": (4.95, 79), "USDMXN=X": (17.2, 80),
    }
    for code, meta in COUNTRIES.items():
        pair = meta.get("currency_pair")
        if not pair:
            continue
        try:
            if yf_available:
                obj = yf.Ticker(pair)
                df = obj.history(period="5y")
                if df.empty:
                    raise ValueError("No data returned")
            else:
                start_price, seed = fx_seeds.get(pair, (1.0, 99))
                idx = _date_index(1825)
                n = len(idx)
                close = _random_walk(start_price, drift=0.0001, vol=0.005, n=n, seed=seed)
                df = pd.DataFrame({"Close": close}, index=idx)

            _save_parquet("market", pair, df)
            _update_manifest(manifest, "market", pair, df)
            print(f"  ok  {code} FX ({pair}): {len(df)} rows")
        except Exception as e:
            _update_manifest(manifest, "market", pair, error=str(e))
            print(f"  ERR {code} FX ({pair}): {e}")

    # --- DXY ---
    print("  -- DXY --")
    try:
        if yf_available:
            df = yf.Ticker("DX-Y.NYB").history(period="5y")
            if df.empty:
                raise ValueError("No data returned")
        else:
            idx = _date_index(1825)
            n = len(idx)
            close = _random_walk(104, drift=0.0001, vol=0.004, n=n, seed=100)
            df = pd.DataFrame({"Close": close}, index=idx)
        _save_parquet("market", "DXY", df)
        _update_manifest(manifest, "market", "DXY", df)
        print(f"  ok  DXY: {len(df)} rows")
    except Exception as e:
        _update_manifest(manifest, "market", "DXY", error=str(e))
        print(f"  ERR DXY: {e}")

    # --- Commodities ---
    print("  -- Commodities --")
    comm_seeds = {
        "GC=F": ("Gold", 1950, 0.0003, 0.008, 101),
        "HG=F": ("Copper", 3.80, 0.0002, 0.015, 102),
        "CL=F": ("WTI", 75, 0.0001, 0.02, 103),
        "BZ=F": ("Brent", 80, 0.0001, 0.02, 104),
    }
    for ticker, (name, start_p, drift, vol, seed) in comm_seeds.items():
        try:
            if yf_available:
                df = yf.Ticker(ticker).history(period="5y")
                if df.empty:
                    raise ValueError("No data returned")
            else:
                idx = _date_index(1825)
                n = len(idx)
                close = _random_walk(start_p, drift=drift, vol=vol, n=n, seed=seed)
                df = pd.DataFrame({"Close": close}, index=idx)
            _save_parquet("market", ticker, df)
            _update_manifest(manifest, "market", ticker, df)
            print(f"  ok  {name} ({ticker}): {len(df)} rows")
        except Exception as e:
            _update_manifest(manifest, "market", ticker, error=str(e))
            print(f"  ERR {name} ({ticker}): {e}")

    # --- Volatility (VIX, MOVE) ---
    print("  -- Volatility --")
    vol_seeds = {"^VIX": ("VIX", 18, 1.5, 106), "^MOVE": ("MOVE", 110, 5, 107)}
    for ticker, (name, center, vol_param, seed) in vol_seeds.items():
        try:
            if yf_available:
                df = yf.Ticker(ticker).history(period="5y")
                if df.empty:
                    raise ValueError("No data returned")
            else:
                idx = _date_index(1825)
                n = len(idx)
                low, high = (9, 80) if name == "VIX" else (50, 200)
                close = np.clip(_mean_reverting(center, vol=vol_param, n=n, seed=seed), low, high)
                df = pd.DataFrame({"Close": close}, index=idx)
            _save_parquet("market", ticker, df)
            _update_manifest(manifest, "market", ticker, df)
            print(f"  ok  {name} ({ticker}): {len(df)} rows")
        except Exception as e:
            _update_manifest(manifest, "market", ticker, error=str(e))
            print(f"  ERR {name} ({ticker}): {e}")

    print("[Market] Done")


# ---------------------------------------------------------------------------
# Source: IMF (Balance of Payments, Gold Reserves)
# ---------------------------------------------------------------------------

def ingest_imf(manifest: dict, incremental: bool = False):
    """Ingest IMF BOP and gold reserve data."""
    print("\n[IMF] Ingesting balance of payments and gold reserves...")
    print("  Using mock data (IMF API is flaky; swap when needed)")

    imf_codes = {k: v["imf_code"] for k, v in COUNTRIES.items()}
    bop_base = {"US": -500, "U2": 300, "GB": -100, "JP": 150, "CN": 200,
                "CA": -30, "AU": -50, "CH": 70, "KR": 80, "IN": -40,
                "BR": -50, "MX": -20, "DE": 250}
    gold_base = {"US": 8133, "U2": 10770, "GB": 310, "JP": 846, "CN": 2235,
                 "CA": 0, "AU": 80, "CH": 1040, "KR": 104, "IN": 800,
                 "BR": 130, "MX": 120, "DE": 3355}

    years = list(range(2005, 2026))

    # --- BOP ---
    print("  -- Balance of Payments --")
    bop_data = {}
    for short, imf_c in imf_codes.items():
        base = bop_base.get(imf_c, 0)
        values = _mean_reverting(base, vol=abs(base) * 0.1 + 10,
                                 n=len(years), seed=hash(imf_c) % 2**31)
        bop_data[imf_c] = values
    df_bop = pd.DataFrame(bop_data, index=years)
    _save_parquet("imf", "bop", df_bop)
    _update_manifest(manifest, "imf", "bop", df_bop)
    print(f"  ok  BOP: {len(df_bop)} years x {len(df_bop.columns)} countries")

    # --- Gold Reserves ---
    print("  -- Gold Reserves --")
    gold_data = {}
    for short, imf_c in imf_codes.items():
        base = gold_base.get(imf_c, 100)
        rng = np.random.RandomState(hash(imf_c + "gold") % 2**31)
        values = base + np.cumsum(rng.normal(0, base * 0.005, len(years)))
        gold_data[imf_c] = np.clip(values, 0, None)
    df_gold = pd.DataFrame(gold_data, index=years)
    _save_parquet("imf", "gold_reserves", df_gold)
    _update_manifest(manifest, "imf", "gold_reserves", df_gold)
    print(f"  ok  Gold reserves: {len(df_gold)} years x {len(df_gold.columns)} countries")

    print("[IMF] Done")


# ---------------------------------------------------------------------------
# Source: Semiconductor sector
# ---------------------------------------------------------------------------

def ingest_semi(manifest: dict, incremental: bool = False):
    """Ingest semiconductor stock history, ETFs, revenue and inventory cycles."""
    print("\n[Semi] Ingesting semiconductor sector data...")

    yf_available = False
    try:
        import yfinance as yf
        yf_available = True
        print("  Using live yfinance for stocks/ETFs")
    except ImportError:
        print("  yfinance not installed, using mock data")

    # --- Semi Stocks ---
    print("  -- Semi Stocks --")
    seed_map = {
        "^SOX": (3800, 300), "NVDA": (480, 301), "TSM": (105, 302),
        "ASML": (680, 303), "AMD": (120, 304), "INTC": (35, 305),
        "AVGO": (900, 306), "QCOM": (145, 307), "MU": (80, 308),
        "LRCX": (680, 309), "AMAT": (160, 310),
    }
    for label, ticker in SEMI_TICKERS.items():
        try:
            if yf_available:
                df = yf.Ticker(ticker).history(period="5y")
                if df.empty:
                    raise ValueError("No data returned")
            else:
                start_price, seed = seed_map.get(ticker, (100, 399))
                idx = _date_index(1825)
                n = len(idx)
                close = _random_walk(start_price, drift=0.0005, vol=0.02, n=n, seed=seed)
                df = pd.DataFrame({"Close": close}, index=idx)
            _save_parquet("semi", ticker, df)
            _update_manifest(manifest, "semi", ticker, df)
            print(f"  ok  {label} ({ticker}): {len(df)} rows")
        except Exception as e:
            _update_manifest(manifest, "semi", ticker, error=str(e))
            print(f"  ERR {label} ({ticker}): {e}")

    # --- Semi ETFs ---
    print("  -- Semi ETFs --")
    etf_seeds = {"SMH": (220, 320), "SOXX": (480, 321)}
    for label, ticker in SEMI_ETFS.items():
        try:
            if yf_available:
                df = yf.Ticker(ticker).history(period="5y")
                if df.empty:
                    raise ValueError("No data returned")
            else:
                start_price, seed = etf_seeds.get(ticker, (100, 399))
                idx = _date_index(1825)
                n = len(idx)
                close = _random_walk(start_price, drift=0.0005, vol=0.018, n=n, seed=seed)
                df = pd.DataFrame({"Close": close}, index=idx)
            _save_parquet("semi", ticker, df)
            _update_manifest(manifest, "semi", ticker, df)
            print(f"  ok  {label} ({ticker}): {len(df)} rows")
        except Exception as e:
            _update_manifest(manifest, "semi", ticker, error=str(e))
            print(f"  ERR {label} ({ticker}): {e}")

    # --- Revenue Cycle (quarterly, always mock for now) ---
    print("  -- Revenue Cycle --")
    quarters = pd.date_range(start="2020-01-01", end="2026-01-01", freq="QS")
    n = len(quarters)
    rng = np.random.RandomState(350)
    base = 120
    cycle = base + 30 * np.sin(np.linspace(0, 3 * np.pi, n)) + np.cumsum(rng.normal(1, 3, n))
    df_rev = pd.DataFrame({
        "Global Semi Revenue ($B)": cycle,
        "QoQ Change (%)": np.concatenate([[0], np.diff(cycle) / cycle[:-1] * 100]),
    }, index=quarters)
    _save_parquet("semi", "revenue_cycle", df_rev)
    _update_manifest(manifest, "semi", "revenue_cycle", df_rev)
    print(f"  ok  Revenue cycle: {len(df_rev)} quarters")

    # --- Inventory Cycle (monthly, always mock for now) ---
    print("  -- Inventory Cycle --")
    months = pd.date_range(start="2022-01-01", end="2026-02-01", freq="MS")
    n = len(months)
    btb = np.clip(_mean_reverting(1.0, vol=0.04, n=n, seed=351), 0.7, 1.4)
    inv_days = _mean_reverting(95, vol=4, n=n, seed=352)
    df_inv = pd.DataFrame({
        "Book-to-Bill": btb,
        "Inventory Days": inv_days,
    }, index=months)
    _save_parquet("semi", "inventory_cycle", df_inv)
    _update_manifest(manifest, "semi", "inventory_cycle", df_inv)
    print(f"  ok  Inventory cycle: {len(df_inv)} months")

    print("[Semi] Done")


# ---------------------------------------------------------------------------
# Source: Policy & Geopolitical Events
# ---------------------------------------------------------------------------

def ingest_policy(manifest: dict, incremental: bool = False):
    """Ingest policy events, central bank calendar, tariff tracker."""
    print("\n[Policy] Ingesting policy and geopolitical data...")
    print("  Using curated mock data (swap for Federal Register / GDELT APIs)")

    # --- Policy Events ---
    events = [
        {"date": "2025-01-15", "category": "Trade & Tariffs", "country": "US",
         "event": "US raises Section 301 tariffs on Chinese EVs to 100%, semiconductors to 50%",
         "impact": "Negative", "sectors": "Autos, Semiconductors",
         "detail": "Effective Q2 2025. Targets $18B in imports. China signals retaliation on US agricultural exports."},
        {"date": "2025-02-01", "category": "Central Bank Policy", "country": "US",
         "event": "FOMC holds rates at 5.25-5.50%, signals patience on cuts",
         "impact": "Neutral", "sectors": "Broad Market",
         "detail": "Dot plot shows 2 cuts in 2025 vs market pricing of 4-5. USD strengthens on hawkish hold."},
        {"date": "2025-02-20", "category": "Export Controls & Sanctions", "country": "US",
         "event": "Commerce Dept expands AI chip export controls to include ASML DUV tools",
         "impact": "Negative", "sectors": "Semiconductors, AI",
         "detail": "New rule requires licenses for DUV lithography exports to China. ASML, LRCX, AMAT affected."},
        {"date": "2025-03-10", "category": "Industrial Policy & Subsidies", "country": "CN",
         "event": "China announces $47B 'Big Fund III' for domestic semiconductor capacity",
         "impact": "Mixed", "sectors": "Semiconductors",
         "detail": "Third phase of national IC fund. Focus on mature-node fabs and packaging."},
        {"date": "2025-03-15", "category": "Central Bank Policy", "country": "JP",
         "event": "BOJ raises policy rate to 0.50%, signals further normalization",
         "impact": "Mixed", "sectors": "FX, Bonds",
         "detail": "Yen strengthens 3% on surprise hawkish shift. Carry trade unwind risk rises."},
        {"date": "2025-04-01", "category": "Trade & Tariffs", "country": "US",
         "event": "US imposes 25% tariffs on steel/aluminum from all countries",
         "impact": "Negative", "sectors": "Industrials, Construction",
         "detail": "Universal tariff replaces country-specific exemptions. EU, Japan, Canada announce retaliation."},
        {"date": "2025-04-20", "category": "Industrial Policy & Subsidies", "country": "US",
         "event": "CHIPS Act Phase 2: $8B disbursed to Intel, Samsung US fabs",
         "impact": "Positive", "sectors": "Semiconductors",
         "detail": "Intel Arizona fab gets $5B, Samsung Taylor TX gets $3B. Production expected 2027."},
        {"date": "2025-05-10", "category": "Capital Controls", "country": "CN",
         "event": "PBOC tightens offshore yuan lending to defend CNY",
         "impact": "Mixed", "sectors": "FX, EM",
         "detail": "Squeeze on CNH short positions. Signal of capital outflow pressure."},
        {"date": "2025-06-15", "category": "Central Bank Policy", "country": "EU",
         "event": "ECB cuts deposit rate by 25bp to 4.25%, signals data-dependent path",
         "impact": "Positive", "sectors": "EU Equities, Bonds",
         "detail": "First cut of the cycle. EUR weakens modestly. EU bank stocks rally."},
        {"date": "2025-07-01", "category": "Regulatory Change", "country": "EU",
         "event": "EU Carbon Border Adjustment Mechanism (CBAM) full implementation",
         "impact": "Mixed", "sectors": "Industrials, Energy, Materials",
         "detail": "Carbon tariffs on steel, cement, aluminum, fertilizers, electricity imports."},
        {"date": "2025-08-05", "category": "Geopolitical Event", "country": "CN",
         "event": "China conducts large-scale military exercises near Taiwan Strait",
         "impact": "Negative", "sectors": "Semiconductors, Defense, Shipping",
         "detail": "Week-long exercises. TSM stock drops 8%. Shipping insurance rates triple."},
        {"date": "2025-09-01", "category": "Trade & Tariffs", "country": "CN",
         "event": "China restricts export of gallium, germanium, and antimony",
         "impact": "Negative", "sectors": "Semiconductors, Defense",
         "detail": "Critical minerals export permits required. Affects chip substrates."},
        {"date": "2025-09-20", "category": "Central Bank Policy", "country": "US",
         "event": "Fed cuts rates by 25bp to 5.00-5.25%, first cut of the cycle",
         "impact": "Positive", "sectors": "Broad Market",
         "detail": "Markets rally. Forward guidance suggests gradual easing. DXY drops 1.5%."},
        {"date": "2025-10-15", "category": "Industrial Policy & Subsidies", "country": "US",
         "event": "IRA: $4.5B in new clean energy tax credit allocations",
         "impact": "Positive", "sectors": "Clean Energy, EVs, Utilities",
         "detail": "Focus on battery manufacturing, solar panels, EV charging infrastructure."},
        {"date": "2025-11-01", "category": "Export Controls & Sanctions", "country": "US",
         "event": "US Treasury designates 15 Chinese entities under Russia-related sanctions",
         "impact": "Negative", "sectors": "Banks, Trade Finance",
         "detail": "Targets Chinese banks facilitating Russian commodity trade."},
        {"date": "2025-12-10", "category": "Central Bank Policy", "country": "US",
         "event": "Fed cuts rates by 25bp to 4.75-5.00%, signals 3 more cuts in 2026",
         "impact": "Positive", "sectors": "Broad Market, Real Estate",
         "detail": "Dovish dot plot. 10Y yield drops to 3.9%. REIT sector surges 5%."},
        {"date": "2026-01-05", "category": "Trade & Tariffs", "country": "US",
         "event": "New administration announces 60% tariff proposal on all Chinese goods",
         "impact": "Negative", "sectors": "Consumer, Tech, Industrials",
         "detail": "Phase-in over 2026. Markets sell off 3%. Supply chain diversification accelerates."},
        {"date": "2026-01-20", "category": "Geopolitical Event", "country": "US",
         "event": "US executive orders on energy, trade, and immigration on day one",
         "impact": "Mixed", "sectors": "Energy, Industrials, Agriculture",
         "detail": "Paris Agreement withdrawal, Keystone XL restart, border emergency."},
        {"date": "2026-02-01", "category": "Central Bank Policy", "country": "US",
         "event": "FOMC pauses rate cuts at 4.75-5.00%, cites tariff inflation risks",
         "impact": "Negative", "sectors": "Broad Market",
         "detail": "Hawkish pause. Market reprices terminal rate higher. 2Y yield jumps 15bp."},
        {"date": "2026-02-05", "category": "Export Controls & Sanctions", "country": "US",
         "event": "Commerce Dept proposes 'know your customer' rule for cloud AI compute",
         "impact": "Mixed", "sectors": "Cloud, AI, Semiconductors",
         "detail": "Would require cloud providers to verify end-users of AI training workloads."},
    ]
    df_events = pd.DataFrame(events)
    df_events["date"] = pd.to_datetime(df_events["date"])
    _save_parquet("policy", "events", df_events)
    _update_manifest(manifest, "policy", "events", df_events)
    print(f"  ok  Policy events: {len(df_events)} events")

    # --- Central Bank Calendar ---
    meetings = [
        {"date": "2026-02-05", "bank": "RBA", "country": "AU", "current_rate": 4.35,
         "expected_action": "Hold", "market_probability": "85% hold"},
        {"date": "2026-03-06", "bank": "ECB", "country": "EU", "current_rate": 4.00,
         "expected_action": "Cut 25bp", "market_probability": "70% cut"},
        {"date": "2026-03-14", "bank": "BOJ", "country": "JP", "current_rate": 0.50,
         "expected_action": "Hold", "market_probability": "90% hold"},
        {"date": "2026-03-19", "bank": "FOMC", "country": "US", "current_rate": 4.875,
         "expected_action": "Hold", "market_probability": "80% hold"},
        {"date": "2026-03-20", "bank": "BOE", "country": "UK", "current_rate": 5.00,
         "expected_action": "Hold", "market_probability": "75% hold"},
        {"date": "2026-04-17", "bank": "ECB", "country": "EU", "current_rate": 4.00,
         "expected_action": "Cut 25bp", "market_probability": "60% cut"},
        {"date": "2026-05-07", "bank": "FOMC", "country": "US", "current_rate": 4.875,
         "expected_action": "Cut 25bp", "market_probability": "55% cut"},
        {"date": "2026-05-08", "bank": "BOE", "country": "UK", "current_rate": 5.00,
         "expected_action": "Cut 25bp", "market_probability": "65% cut"},
        {"date": "2026-06-05", "bank": "ECB", "country": "EU", "current_rate": 3.75,
         "expected_action": "Hold", "market_probability": "70% hold"},
        {"date": "2026-06-18", "bank": "FOMC", "country": "US", "current_rate": 4.625,
         "expected_action": "Cut 25bp", "market_probability": "60% cut"},
    ]
    df_cb = pd.DataFrame(meetings)
    df_cb["date"] = pd.to_datetime(df_cb["date"])
    _save_parquet("policy", "cb_calendar", df_cb)
    _update_manifest(manifest, "policy", "cb_calendar", df_cb)
    print(f"  ok  CB calendar: {len(df_cb)} meetings")

    # --- Tariff Tracker ---
    tariff_data = {
        "Target": ["China", "China", "EU", "Japan", "Canada", "Mexico", "S. Korea", "India", "China", "All"],
        "Sector": ["Semiconductors", "EVs & Batteries", "Steel & Aluminum", "Autos", "Steel & Aluminum",
                    "Autos (pending)", "Steel", "Electronics", "Consumer Goods", "Baseline MFN"],
        "US Tariff Rate (%)": [50, 100, 25, 2.5, 25, 25, 25, 3.5, 25, 3.4],
        "Pre-2025 Rate (%)": [25, 27.5, 0, 2.5, 0, 0, 0, 3.5, 7.5, 3.4],
        "Effective Date": ["2025-06-01", "2025-06-01", "2025-04-01", "Unchanged", "2025-04-01",
                          "Proposed", "2025-04-01", "Unchanged", "2025-01-15", "N/A"],
        "Retaliation": ["Yes - Ag", "Yes - Ag", "Yes - Bourbon, Harley", "None", "Yes - Dairy",
                        "TBD", "None", "None", "Yes - LNG", "N/A"],
    }
    df_tariff = pd.DataFrame(tariff_data)
    _save_parquet("policy", "tariff_tracker", df_tariff)
    _update_manifest(manifest, "policy", "tariff_tracker", df_tariff)
    print(f"  ok  Tariff tracker: {len(df_tariff)} entries")

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
    incremental = args.update

    start_time = datetime.now()
    print(f"Macro Dashboard Ingestor — {'Incremental' if incremental else 'Full'} run")
    print(f"Started: {start_time.isoformat()}")
    print(f"Data directory: {DATA_DIR.resolve()}")

    if args.source:
        # Single source
        func = SOURCE_FUNCTIONS[args.source]
        func(manifest, incremental=incremental)
    else:
        # All sources
        for name, func in SOURCE_FUNCTIONS.items():
            func(manifest, incremental=incremental)

    _save_manifest(manifest)

    elapsed = (datetime.now() - start_time).total_seconds()
    ok_count = sum(1 for v in manifest.values() if v.get("status") == "ok")
    err_count = sum(1 for v in manifest.values() if v.get("status") == "error")
    print(f"\nFinished in {elapsed:.1f}s — {ok_count} ok, {err_count} errors")
    print(f"Manifest saved to {MANIFEST_FILE}")


if __name__ == "__main__":
    main()
