"""
Data fetcher module - provides all data for the dashboard.

Data loading priority:
  1. Parquet files in data/ (populated by ingestor.py) — fast, historical
  2. Live API calls (when keys are set) — real-time
  3. In-memory mock generators — fallback when nothing else is available

For historical data: run `python ingestor.py --full` to populate Parquet files.
For real-time quotes: short-period requests (1M, 3M) use live API or mock.
"""

import pandas as pd
import numpy as np
import streamlit as st
from datetime import datetime, timedelta
from pathlib import Path
from config import (
    COUNTRIES, MARKET_TICKERS, FRED, WB_INDICATORS, DATE_RANGES,
    SEMI_TICKERS, SEMI_ETFS, SEMI_COMMODITIES, POLICY_CATEGORIES,
)


# ---------------------------------------------------------------------------
# Parquet loading layer
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).parent / "data"

# Periods considered "long" — prefer Parquet for these
_LONG_PERIODS = {"1y", "3y", "5y", "10y", "max", "1Y", "3Y", "5Y", "10Y", "MAX"}


def _sanitize_filename(name: str) -> str:
    """Make a string safe for use as a filename."""
    return name.replace("^", "").replace("=", "_").replace("/", "_").replace(" ", "_")


def _load_parquet(category: str, name: str) -> pd.DataFrame | None:
    """Try to load a Parquet file from data/<category>/<name>.parquet.
    Returns None if the file doesn't exist."""
    safe_name = _sanitize_filename(name)
    path = DATA_DIR / category / f"{safe_name}.parquet"
    if path.exists():
        try:
            return pd.read_parquet(path)
        except Exception:
            return None
    return None


def _filter_by_period(df: pd.DataFrame, period: str) -> pd.DataFrame:
    """Filter a time-indexed DataFrame by a period string like '5y', '1mo'."""
    period_days = {
        "1mo": 30, "3mo": 90, "6mo": 180, "1y": 365,
        "3y": 1095, "5y": 1825, "10y": 3650,
    }
    days = period_days.get(period.lower())
    if days and hasattr(df.index, "max") and len(df) > 0:
        cutoff = df.index.max() - pd.Timedelta(days=days)
        return df[df.index >= cutoff]
    return df


# ---------------------------------------------------------------------------
# Helper: generate realistic time series
# ---------------------------------------------------------------------------

def _date_index(days=1825):
    """Generate a business day index for the given number of days back."""
    end = datetime(2026, 2, 6)
    start = end - timedelta(days=days)
    return pd.bdate_range(start=start, end=end)


def _random_walk(start, drift=0.0002, vol=0.01, n=1300, seed=None):
    """Generate a geometric random walk (price-like series)."""
    rng = np.random.RandomState(seed)
    returns = rng.normal(drift, vol, n)
    prices = start * np.exp(np.cumsum(returns))
    return prices


def _mean_reverting(center, vol=0.1, n=1300, seed=None):
    """Generate a mean-reverting series (rate-like)."""
    rng = np.random.RandomState(seed)
    series = np.zeros(n)
    series[0] = center
    for i in range(1, n):
        series[i] = series[i - 1] + 0.05 * (center - series[i - 1]) + rng.normal(0, vol)
    return series


# ---------------------------------------------------------------------------
# Market Data (yfinance replacements)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=900)
def get_index_data(ticker: str, period: str = "5y") -> pd.DataFrame:
    """Single index/ticker history. Returns OHLCV df.
    Checks Parquet first for long periods, falls back to mock."""
    # Try Parquet for historical data
    if period in _LONG_PERIODS:
        pq = _load_parquet("market", ticker)
        if pq is not None:
            return _filter_by_period(pq, period)

    # Mock fallback
    seed_map = {
        "^GSPC": (4200, 42), "^STOXX50E": (4100, 43), "^FTSE": (7200, 44),
        "^N225": (32000, 45), "000001.SS": (3100, 46), "^GSPTSE": (19000, 47),
        "^AXJO": (7000, 48), "^SSMI": (11000, 49), "^KS11": (2400, 50),
        "^BSESN": (60000, 51), "^BVSP": (115000, 52), "^MXX": (52000, 53),
        "^GDAXI": (15000, 54),
    }
    start_price, seed = seed_map.get(ticker, (1000, 99))
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
    return df


@st.cache_data(ttl=900)
def get_multiple_tickers(tickers: list, period: str = "5y") -> pd.DataFrame:
    """Multiple tickers, returns df with Close prices as columns."""
    dfs = {}
    for t in tickers:
        data = get_index_data(t, period)
        dfs[t] = data["Close"]
    return pd.DataFrame(dfs)


@st.cache_data(ttl=900)
def get_fx_rates(country_codes: list, period: str = "5y") -> pd.DataFrame:
    """Get FX rates for selected countries. Checks Parquet first."""
    # Try Parquet for historical data
    if period in _LONG_PERIODS:
        fx_data = {}
        all_found = True
        for code in country_codes:
            pair = COUNTRIES.get(code, {}).get("currency_pair")
            if not pair:
                continue
            pq = _load_parquet("market", pair)
            if pq is not None and "Close" in pq.columns:
                fx_data[pair] = pq["Close"]
            else:
                all_found = False
                break
        if all_found and fx_data:
            df = pd.DataFrame(fx_data)
            return _filter_by_period(df, period)

    # Mock fallback
    idx = _date_index(1825)
    n = len(idx)
    fx_data = {}
    fx_seeds = {
        "EURUSD=X": (1.08, 70), "GBPUSD=X": (1.26, 71), "USDJPY=X": (148, 72),
        "USDCNY=X": (7.20, 73), "USDCAD=X": (1.36, 74), "AUDUSD=X": (0.66, 75),
        "USDCHF=X": (0.88, 76), "USDKRW=X": (1320, 77), "USDINR=X": (83.5, 78),
        "USDBRL=X": (4.95, 79), "USDMXN=X": (17.2, 80),
    }
    for code in country_codes:
        pair = COUNTRIES.get(code, {}).get("currency_pair")
        if pair and pair in fx_seeds:
            start_price, seed = fx_seeds[pair]
            rates = _random_walk(start_price, drift=0.0001, vol=0.005, n=n, seed=seed)
            fx_data[pair] = rates[:n]
    if not fx_data:
        return pd.DataFrame(index=idx)
    return pd.DataFrame(fx_data, index=idx[:len(next(iter(fx_data.values())))])


@st.cache_data(ttl=900)
def get_dxy(period: str = "5y") -> pd.DataFrame:
    """DXY index. Checks Parquet first."""
    if period in _LONG_PERIODS:
        pq = _load_parquet("market", "DXY")
        if pq is not None:
            return _filter_by_period(pq, period)

    # Mock fallback
    idx = _date_index(1825)
    n = len(idx)
    close = _random_walk(104, drift=0.0001, vol=0.004, n=n, seed=100)
    return pd.DataFrame({"Close": close}, index=idx)


@st.cache_data(ttl=900)
def get_commodities(period: str = "5y") -> pd.DataFrame:
    """Gold, Copper, WTI, Brent, BDI. Checks Parquet first."""
    if period in _LONG_PERIODS:
        comm_map = {"Gold": "GC=F", "Copper": "HG=F", "WTI": "CL=F", "Brent": "BZ=F"}
        comm_data = {}
        all_found = True
        for name, ticker in comm_map.items():
            pq = _load_parquet("market", ticker)
            if pq is not None and "Close" in pq.columns:
                comm_data[name] = pq["Close"]
            else:
                all_found = False
                break
        if all_found and comm_data:
            return _filter_by_period(pd.DataFrame(comm_data), period)

    # Mock fallback
    idx = _date_index(1825)
    n = len(idx)
    return pd.DataFrame({
        "Gold": _random_walk(1950, drift=0.0003, vol=0.008, n=n, seed=101),
        "Copper": _random_walk(3.80, drift=0.0002, vol=0.015, n=n, seed=102),
        "WTI": _random_walk(75, drift=0.0001, vol=0.02, n=n, seed=103),
        "Brent": _random_walk(80, drift=0.0001, vol=0.02, n=n, seed=104),
        "BDI": _random_walk(1500, drift=0.0, vol=0.03, n=n, seed=105),
    }, index=idx)


@st.cache_data(ttl=900)
def get_volatility(period: str = "5y") -> pd.DataFrame:
    """VIX and MOVE index. Checks Parquet first."""
    if period in _LONG_PERIODS:
        vol_data = {}
        for name, ticker in [("VIX", "^VIX"), ("MOVE", "^MOVE")]:
            pq = _load_parquet("market", ticker)
            if pq is not None and "Close" in pq.columns:
                vol_data[name] = pq["Close"]
        if len(vol_data) == 2:
            return _filter_by_period(pd.DataFrame(vol_data), period)

    # Mock fallback
    idx = _date_index(1825)
    n = len(idx)
    return pd.DataFrame({
        "VIX": np.clip(_mean_reverting(18, vol=1.5, n=n, seed=106), 9, 80),
        "MOVE": np.clip(_mean_reverting(110, vol=5, n=n, seed=107), 50, 200),
    }, index=idx)


# ---------------------------------------------------------------------------
# FRED Data
# ---------------------------------------------------------------------------

@st.cache_data(ttl=21600)
def get_fred_series(series_id: str, start: str = "2000-01-01") -> pd.Series:
    """Single FRED series. Checks Parquet first, falls back to mock."""
    pq = _load_parquet("fred", series_id)
    if pq is not None:
        series = pq.iloc[:, 0]  # First column is the value
        series.name = series_id
        return series

    # Mock fallback
    idx = _date_index(1825)
    n = len(idx)
    mock_params = {
        "DFF": (5.33, 0.05, 108),           # Fed Funds Rate
        "DGS10": (4.2, 0.08, 109),          # 10Y yield
        "DGS2": (4.5, 0.1, 110),            # 2Y yield
        "T10Y2Y": (-0.3, 0.08, 111),        # 2s10s spread
        "DFII10": (1.8, 0.1, 112),          # Real yield
        "T10YIE": (2.3, 0.05, 113),         # Breakeven
        "WALCL": (7800000, 50000, 114),      # Fed balance sheet (millions)
        "RRPONTSYD": (500000, 30000, 115),   # RRP (millions)
        "WTREGEN": (750000, 40000, 116),     # TGA (millions)
        "WM2NS": (20800, 100, 117),          # M2 (billions)
        "BAMLH0A0HYM2": (3.8, 0.3, 118),    # HY OAS
        "BAMLC0A0CM": (1.2, 0.1, 119),      # IG OAS
        "NFCI": (-0.3, 0.1, 120),           # NFCI
        "ICSA": (220000, 8000, 121),         # Initial claims
        "CCSA": (1850000, 30000, 122),       # Continuing claims
        "UMCSENT": (68, 3, 123),             # Consumer sentiment
        "CPIAUCSL": (310, 0.5, 124),         # CPI level
        "UNRATE": (3.8, 0.1, 125),           # Unemployment
        "PSAVERT": (4.5, 0.5, 126),          # Personal savings rate
        "INDPRO": (103, 0.3, 127),           # Industrial production
        "USALOLITONOSTSAM": (99.5, 0.2, 128),  # LEI
    }
    center, vol, seed = mock_params.get(series_id, (100, 1, 199))
    data = _mean_reverting(center, vol=vol, n=n, seed=seed)

    # Add trending behavior for some series
    if series_id == "WALCL":
        # Fed balance sheet had a peak and decline
        trend = np.linspace(0, -500000, n)
        data = data + trend
    elif series_id == "RRPONTSYD":
        # RRP draining over time
        trend = np.linspace(1500000, 0, n)
        data = np.clip(data + trend, 0, None)
    elif series_id == "CPIAUCSL":
        # CPI rising steadily
        trend = np.linspace(-10, 10, n)
        data = data + trend

    return pd.Series(data, index=idx, name=series_id)


@st.cache_data(ttl=21600)
def get_fred_multiple(series_ids: list, start: str = "2000-01-01") -> pd.DataFrame:
    """Multiple FRED series merged into one df."""
    dfs = {}
    for sid in series_ids:
        dfs[sid] = get_fred_series(sid, start)
    return pd.DataFrame(dfs)


@st.cache_data(ttl=21600)
def get_yield_curve_snapshot() -> pd.DataFrame:
    """Current yield curve: maturities as columns, dates as rows."""
    maturities = ["1M", "3M", "6M", "1Y", "2Y", "3Y", "5Y", "7Y", "10Y", "20Y", "30Y"]
    # Current curve (inverted at front end)
    current = [5.35, 5.30, 5.15, 4.80, 4.50, 4.30, 4.20, 4.25, 4.30, 4.55, 4.45]
    # 3 months ago
    three_mo_ago = [5.40, 5.35, 5.25, 5.00, 4.70, 4.50, 4.35, 4.30, 4.35, 4.50, 4.40]
    # 1 year ago
    one_yr_ago = [5.45, 5.40, 5.35, 5.10, 4.85, 4.60, 4.40, 4.35, 4.35, 4.55, 4.50]

    return pd.DataFrame({
        "Current": current,
        "3M Ago": three_mo_ago,
        "1Y Ago": one_yr_ago,
    }, index=maturities)


# ---------------------------------------------------------------------------
# World Bank Data
# ---------------------------------------------------------------------------

@st.cache_data(ttl=604800)
def get_wb_indicator(indicator: str, countries: list, start_year: int = 2000) -> pd.DataFrame:
    """Fetch a World Bank indicator for given countries. Checks Parquet first."""
    pq = _load_parquet("world_bank", indicator)
    if pq is not None:
        # Filter to requested countries and years
        available_cols = [c for c in countries if c in pq.columns]
        if available_cols:
            df = pq[available_cols]
            if start_year and hasattr(df.index, 'min'):
                df = df[df.index >= start_year]
            return df

    # Mock fallback
    years = list(range(start_year, 2026))
    rng = np.random.RandomState(hash(indicator) % 2**31)

    # Realistic base values per indicator
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
    defaults = base_values.get(indicator, {})

    data = {}
    for c in countries:
        short_code = wb_code_to_short.get(c, c)
        base = defaults.get(short_code, 0)
        # Small scale for percentages, larger for dollar amounts
        scale = abs(base) * 0.05 if base != 0 else 1
        vals = []
        current = base
        for _ in years:
            current = current + rng.normal(0, scale)
            vals.append(current)
        data[c] = vals

    return pd.DataFrame(data, index=years)


@st.cache_data(ttl=604800)
def get_wb_multiple_indicators(indicators: dict, countries: list) -> dict:
    """Fetch multiple WB indicators. Returns dict of indicator_name -> df."""
    result = {}
    for name, code in indicators.items():
        result[name] = get_wb_indicator(code, countries)
    return result


# ---------------------------------------------------------------------------
# IMF Data
# ---------------------------------------------------------------------------

@st.cache_data(ttl=2592000)
def get_imf_bop(country_code: str, indicator: str = "BCA_BP6_USD") -> pd.DataFrame:
    """Fetch IMF Balance of Payments data. Checks Parquet first."""
    pq = _load_parquet("imf", "bop")
    if pq is not None and country_code in pq.columns:
        return pd.DataFrame({"Value": pq[country_code]})

    # Mock fallback
    years = list(range(2005, 2026))
    rng = np.random.RandomState(hash(country_code + indicator) % 2**31)
    bop_base = {"US": -500, "U2": 300, "GB": -100, "JP": 150, "CN": 200,
                "CA": -30, "AU": -50, "CH": 70, "KR": 80, "IN": -40,
                "BR": -50, "MX": -20, "DE": 250}
    base = bop_base.get(country_code, 0)
    values = _mean_reverting(base, vol=abs(base) * 0.1 + 10, n=len(years), seed=hash(country_code) % 2**31)
    return pd.DataFrame({"Value": values}, index=years)


@st.cache_data(ttl=2592000)
def get_imf_gold_reserves(country_code: str) -> pd.DataFrame:
    """Fetch gold reserves from IMF IFS. Checks Parquet first."""
    pq = _load_parquet("imf", "gold_reserves")
    if pq is not None and country_code in pq.columns:
        return pd.DataFrame({"Tonnes": pq[country_code]})

    # Mock fallback
    years = list(range(2005, 2026))
    gold_base = {"US": 8133, "U2": 10770, "GB": 310, "JP": 846, "CN": 2235,
                 "CA": 0, "AU": 80, "CH": 1040, "KR": 104, "IN": 800,
                 "BR": 130, "MX": 120, "DE": 3355}
    base = gold_base.get(country_code, 100)
    rng = np.random.RandomState(hash(country_code + "gold") % 2**31)
    values = base + np.cumsum(rng.normal(0, base * 0.005, len(years)))
    values = np.clip(values, 0, None)
    return pd.DataFrame({"Tonnes": values}, index=years)


# ---------------------------------------------------------------------------
# ECB Data
# ---------------------------------------------------------------------------

@st.cache_data(ttl=86400)
def get_ecb_fx(currency: str = "USD") -> pd.DataFrame:
    """ECB reference rate - mock."""
    idx = _date_index(1825)
    n = len(idx)
    rate = _random_walk(1.08, drift=0.0001, vol=0.005, n=n, seed=200)
    return pd.DataFrame({"Rate": rate}, index=idx)


# ---------------------------------------------------------------------------
# BIS Data
# ---------------------------------------------------------------------------

@st.cache_data(ttl=2592000)
def get_bis_reer(country: str) -> pd.DataFrame:
    """Real Effective Exchange Rate from BIS - mock."""
    idx = pd.date_range(start="2015-01-01", end="2026-02-01", freq="MS")
    n = len(idx)
    reer_base = {"US": 110, "EU": 95, "GB": 100, "JP": 75, "CN": 105,
                 "CA": 98, "AU": 95, "CH": 115, "KR": 100, "IN": 98,
                 "BR": 85, "MX": 90, "DE": 100}
    base = reer_base.get(country, 100)
    values = _mean_reverting(base, vol=1.5, n=n, seed=hash(country + "reer") % 2**31)
    return pd.DataFrame({"REER": values}, index=idx)


# ---------------------------------------------------------------------------
# Fed Funds Futures
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def get_fed_funds_futures() -> pd.DataFrame:
    """Build implied rate path from mock ZQ futures contracts."""
    months = pd.date_range(start="2026-03-01", periods=12, freq="MS")
    # Simulating a rate-cutting cycle priced in
    current_rate = 5.33
    implied_rates = [5.25, 5.15, 5.00, 4.85, 4.75, 4.60, 4.50, 4.40, 4.30, 4.25, 4.15, 4.10]
    cuts_priced = [round((current_rate - r) / 0.25, 1) for r in implied_rates]
    return pd.DataFrame({
        "contract_month": months,
        "implied_rate": implied_rates,
        "cuts_priced": cuts_priced,
    })


# ---------------------------------------------------------------------------
# Semiconductor / Strategic Sector Data
# ---------------------------------------------------------------------------

@st.cache_data(ttl=900)
def get_semi_stocks(period: str = "5y") -> pd.DataFrame:
    """Get semiconductor stock prices (Close) for all tracked tickers. Checks Parquet first."""
    if period in _LONG_PERIODS:
        semi_data = {}
        for label, ticker in SEMI_TICKERS.items():
            pq = _load_parquet("semi", ticker)
            if pq is not None and "Close" in pq.columns:
                semi_data[label] = pq["Close"]
        if len(semi_data) == len(SEMI_TICKERS):
            return _filter_by_period(pd.DataFrame(semi_data), period)

    # Mock fallback
    idx = _date_index(1825)
    n = len(idx)
    seed_map = {
        "^SOX": (3800, 300), "NVDA": (480, 301), "TSM": (105, 302),
        "ASML": (680, 303), "AMD": (120, 304), "INTC": (35, 305),
        "AVGO": (900, 306), "QCOM": (145, 307), "MU": (80, 308),
        "LRCX": (680, 309), "AMAT": (160, 310),
    }
    data = {}
    for label, ticker in SEMI_TICKERS.items():
        start_price, seed = seed_map.get(ticker, (100, 399))
        # Semis are high-vol, high-drift
        data[label] = _random_walk(start_price, drift=0.0005, vol=0.02, n=n, seed=seed)
    return pd.DataFrame(data, index=idx)


@st.cache_data(ttl=900)
def get_semi_etfs(period: str = "5y") -> pd.DataFrame:
    """Get semiconductor ETF prices. Checks Parquet first."""
    if period in _LONG_PERIODS:
        etf_data = {}
        for label, ticker in SEMI_ETFS.items():
            pq = _load_parquet("semi", ticker)
            if pq is not None and "Close" in pq.columns:
                etf_data[label] = pq["Close"]
        if len(etf_data) == len(SEMI_ETFS):
            return _filter_by_period(pd.DataFrame(etf_data), period)

    # Mock fallback
    idx = _date_index(1825)
    n = len(idx)
    data = {
        "SMH (VanEck Semi ETF)": _random_walk(220, drift=0.0005, vol=0.018, n=n, seed=320),
        "SOXX (iShares Semi ETF)": _random_walk(480, drift=0.0005, vol=0.018, n=n, seed=321),
    }
    return pd.DataFrame(data, index=idx)


@st.cache_data(ttl=900)
def get_semi_vs_market(period: str = "5y") -> pd.DataFrame:
    """SOX index vs S&P 500 for relative performance. Checks Parquet first."""
    if period in _LONG_PERIODS:
        sox_pq = _load_parquet("semi", "^SOX")
        spx_pq = _load_parquet("market", "^GSPC")
        if sox_pq is not None and spx_pq is not None:
            sox_close = sox_pq["Close"] if "Close" in sox_pq.columns else sox_pq.iloc[:, 0]
            spx_close = spx_pq["Close"] if "Close" in spx_pq.columns else spx_pq.iloc[:, 0]
            df = pd.DataFrame({"SOX (Semis)": sox_close, "S&P 500": spx_close}).dropna()
            return _filter_by_period(df, period)

    # Mock fallback
    idx = _date_index(1825)
    n = len(idx)
    return pd.DataFrame({
        "SOX (Semis)": _random_walk(3800, drift=0.0005, vol=0.02, n=n, seed=300),
        "S&P 500": _random_walk(4200, drift=0.0003, vol=0.012, n=n, seed=42),
    }, index=idx)


@st.cache_data(ttl=86400)
def get_semi_revenue_cycle() -> pd.DataFrame:
    """Global semiconductor revenue cycle data (quarterly, $B). Checks Parquet first."""
    pq = _load_parquet("semi", "revenue_cycle")
    if pq is not None:
        return pq

    # Mock fallback
    quarters = pd.date_range(start="2020-01-01", end="2026-01-01", freq="QS")
    n = len(quarters)
    rng = np.random.RandomState(350)
    # Simulate the boom-bust-recovery cycle
    base = 120  # ~$120B/quarter baseline
    cycle = base + 30 * np.sin(np.linspace(0, 3 * np.pi, n)) + np.cumsum(rng.normal(1, 3, n))
    return pd.DataFrame({
        "Global Semi Revenue ($B)": cycle,
        "QoQ Change (%)": np.concatenate([[0], np.diff(cycle) / cycle[:-1] * 100]),
    }, index=quarters)


@st.cache_data(ttl=86400)
def get_semi_inventory_cycle() -> pd.DataFrame:
    """Semiconductor inventory/book-to-bill data. Checks Parquet first."""
    pq = _load_parquet("semi", "inventory_cycle")
    if pq is not None:
        return pq

    # Mock fallback
    months = pd.date_range(start="2022-01-01", end="2026-02-01", freq="MS")
    n = len(months)
    rng = np.random.RandomState(351)
    # Book-to-bill oscillates around 1.0 (>1 = expanding, <1 = contracting)
    btb = _mean_reverting(1.0, vol=0.04, n=n, seed=351)
    btb = np.clip(btb, 0.7, 1.4)
    # Inventory days — higher = glut
    inv_days = _mean_reverting(95, vol=4, n=n, seed=352)
    return pd.DataFrame({
        "Book-to-Bill": btb,
        "Inventory Days": inv_days,
    }, index=months)


# ---------------------------------------------------------------------------
# Policy & Geopolitical Events
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def get_policy_events() -> pd.DataFrame:
    """Curated macro-relevant policy events. Checks Parquet first."""
    pq = _load_parquet("policy", "events")
    if pq is not None:
        pq["date"] = pd.to_datetime(pq["date"])
        return pq.sort_values("date", ascending=False).reset_index(drop=True)

    # Mock fallback
    events = [
        # 2025-2026 realistic policy events
        {"date": "2025-01-15", "category": "Trade & Tariffs",
         "country": "US", "event": "US raises Section 301 tariffs on Chinese EVs to 100%, semiconductors to 50%",
         "impact": "Negative", "sectors": "Autos, Semiconductors",
         "detail": "Effective Q2 2025. Targets $18B in imports. China signals retaliation on US agricultural exports."},
        {"date": "2025-02-01", "category": "Central Bank Policy",
         "country": "US", "event": "FOMC holds rates at 5.25-5.50%, signals patience on cuts",
         "impact": "Neutral", "sectors": "Broad Market",
         "detail": "Dot plot shows 2 cuts in 2025 vs market pricing of 4-5. USD strengthens on hawkish hold."},
        {"date": "2025-02-20", "category": "Export Controls & Sanctions",
         "country": "US", "event": "Commerce Dept expands AI chip export controls to include ASML DUV tools",
         "impact": "Negative", "sectors": "Semiconductors, AI",
         "detail": "New rule requires licenses for DUV lithography exports to China. ASML, LRCX, AMAT affected."},
        {"date": "2025-03-10", "category": "Industrial Policy & Subsidies",
         "country": "CN", "event": "China announces $47B 'Big Fund III' for domestic semiconductor capacity",
         "impact": "Mixed", "sectors": "Semiconductors",
         "detail": "Third phase of national IC fund. Focus on mature-node fabs and packaging. Negative for global ASPs."},
        {"date": "2025-03-15", "category": "Central Bank Policy",
         "country": "JP", "event": "BOJ raises policy rate to 0.50%, signals further normalization",
         "impact": "Mixed", "sectors": "FX, Bonds",
         "detail": "Yen strengthens 3% on surprise hawkish shift. Carry trade unwind risk rises."},
        {"date": "2025-04-01", "category": "Trade & Tariffs",
         "country": "US", "event": "US imposes 25% tariffs on steel/aluminum from all countries",
         "impact": "Negative", "sectors": "Industrials, Construction",
         "detail": "Universal tariff replaces country-specific exemptions. EU, Japan, Canada announce retaliatory measures."},
        {"date": "2025-04-20", "category": "Industrial Policy & Subsidies",
         "country": "US", "event": "CHIPS Act Phase 2: $8B disbursed to Intel, Samsung US fabs",
         "impact": "Positive", "sectors": "Semiconductors",
         "detail": "Intel Arizona fab gets $5B, Samsung Taylor TX gets $3B. Production expected 2027."},
        {"date": "2025-05-10", "category": "Capital Controls",
         "country": "CN", "event": "PBOC tightens offshore yuan lending to defend CNY",
         "impact": "Mixed", "sectors": "FX, EM",
         "detail": "Squeeze on CNH short positions. Signal of capital outflow pressure."},
        {"date": "2025-06-15", "category": "Central Bank Policy",
         "country": "EU", "event": "ECB cuts deposit rate by 25bp to 4.25%, signals data-dependent path",
         "impact": "Positive", "sectors": "EU Equities, Bonds",
         "detail": "First cut of the cycle. EUR weakens modestly. EU bank stocks rally."},
        {"date": "2025-07-01", "category": "Regulatory Change",
         "country": "EU", "event": "EU Carbon Border Adjustment Mechanism (CBAM) full implementation",
         "impact": "Mixed", "sectors": "Industrials, Energy, Materials",
         "detail": "Carbon tariffs on steel, cement, aluminum, fertilizers, electricity imports. Increases costs for non-EU exporters."},
        {"date": "2025-08-05", "category": "Geopolitical Event",
         "country": "CN", "event": "China conducts large-scale military exercises near Taiwan Strait",
         "impact": "Negative", "sectors": "Semiconductors, Defense, Shipping",
         "detail": "Week-long exercises. TSM stock drops 8%. Shipping insurance rates for Taiwan Strait triple."},
        {"date": "2025-09-01", "category": "Trade & Tariffs",
         "country": "CN", "event": "China restricts export of gallium, germanium, and antimony",
         "impact": "Negative", "sectors": "Semiconductors, Defense",
         "detail": "Critical minerals export permits required. Affects chip substrates and military applications."},
        {"date": "2025-09-20", "category": "Central Bank Policy",
         "country": "US", "event": "Fed cuts rates by 25bp to 5.00-5.25%, first cut of the cycle",
         "impact": "Positive", "sectors": "Broad Market",
         "detail": "Markets rally. Forward guidance suggests gradual easing. DXY drops 1.5%."},
        {"date": "2025-10-15", "category": "Industrial Policy & Subsidies",
         "country": "US", "event": "Inflation Reduction Act: $4.5B in new clean energy tax credit allocations",
         "impact": "Positive", "sectors": "Clean Energy, EVs, Utilities",
         "detail": "Focus on battery manufacturing, solar panel production, EV charging infrastructure."},
        {"date": "2025-11-01", "category": "Export Controls & Sanctions",
         "country": "US", "event": "US Treasury designates 15 Chinese entities under Russia-related sanctions",
         "impact": "Negative", "sectors": "Banks, Trade Finance",
         "detail": "Targets Chinese banks facilitating Russian commodity trade. USDCNY weakens."},
        {"date": "2025-12-10", "category": "Central Bank Policy",
         "country": "US", "event": "Fed cuts rates by 25bp to 4.75-5.00%, signals 3 more cuts in 2026",
         "impact": "Positive", "sectors": "Broad Market, Real Estate",
         "detail": "Dovish dot plot. 10Y yield drops to 3.9%. REIT sector surges 5%."},
        {"date": "2026-01-05", "category": "Trade & Tariffs",
         "country": "US", "event": "New administration announces 60% tariff proposal on all Chinese goods",
         "impact": "Negative", "sectors": "Consumer, Tech, Industrials",
         "detail": "Phase-in over 2026. Markets sell off 3%. Supply chain diversification accelerates."},
        {"date": "2026-01-20", "category": "Geopolitical Event",
         "country": "US", "event": "US executive orders on energy, trade, and immigration on day one",
         "impact": "Mixed", "sectors": "Energy, Industrials, Agriculture",
         "detail": "Paris Agreement withdrawal, Keystone XL restart, border emergency. Oil stocks rally, clean energy sells off."},
        {"date": "2026-02-01", "category": "Central Bank Policy",
         "country": "US", "event": "FOMC pauses rate cuts at 4.75-5.00%, cites tariff inflation risks",
         "impact": "Negative", "sectors": "Broad Market",
         "detail": "Hawkish pause. Market reprices terminal rate higher. 2Y yield jumps 15bp."},
        {"date": "2026-02-05", "category": "Export Controls & Sanctions",
         "country": "US", "event": "Commerce Dept proposes 'know your customer' rule for cloud AI compute",
         "impact": "Mixed", "sectors": "Cloud, AI, Semiconductors",
         "detail": "Would require cloud providers to verify end-users of AI training workloads. Targets China access to US AI infrastructure."},
    ]
    df = pd.DataFrame(events)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date", ascending=False).reset_index(drop=True)


@st.cache_data(ttl=86400)
def get_central_bank_calendar() -> pd.DataFrame:
    """Upcoming central bank meeting dates and expectations. Checks Parquet first."""
    pq = _load_parquet("policy", "cb_calendar")
    if pq is not None:
        pq["date"] = pd.to_datetime(pq["date"])
        return pq.sort_values("date").reset_index(drop=True)

    # Mock fallback
    meetings = [
        {"date": "2026-02-05", "bank": "RBA", "country": "AU", "current_rate": 4.35, "expected_action": "Hold", "market_probability": "85% hold"},
        {"date": "2026-03-06", "bank": "ECB", "country": "EU", "current_rate": 4.00, "expected_action": "Cut 25bp", "market_probability": "70% cut"},
        {"date": "2026-03-14", "bank": "BOJ", "country": "JP", "current_rate": 0.50, "expected_action": "Hold", "market_probability": "90% hold"},
        {"date": "2026-03-19", "bank": "FOMC", "country": "US", "current_rate": 4.875, "expected_action": "Hold", "market_probability": "80% hold"},
        {"date": "2026-03-20", "bank": "BOE", "country": "UK", "current_rate": 5.00, "expected_action": "Hold", "market_probability": "75% hold"},
        {"date": "2026-04-17", "bank": "ECB", "country": "EU", "current_rate": 4.00, "expected_action": "Cut 25bp", "market_probability": "60% cut"},
        {"date": "2026-05-07", "bank": "FOMC", "country": "US", "current_rate": 4.875, "expected_action": "Cut 25bp", "market_probability": "55% cut"},
        {"date": "2026-05-08", "bank": "BOE", "country": "UK", "current_rate": 5.00, "expected_action": "Cut 25bp", "market_probability": "65% cut"},
        {"date": "2026-06-05", "bank": "ECB", "country": "EU", "current_rate": 3.75, "expected_action": "Hold", "market_probability": "70% hold"},
        {"date": "2026-06-18", "bank": "FOMC", "country": "US", "current_rate": 4.625, "expected_action": "Cut 25bp", "market_probability": "60% cut"},
    ]
    df = pd.DataFrame(meetings)
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


@st.cache_data(ttl=86400)
def get_tariff_tracker() -> pd.DataFrame:
    """Track current effective tariff rates. Checks Parquet first."""
    pq = _load_parquet("policy", "tariff_tracker")
    if pq is not None:
        return pq

    # Mock fallback
    data = {
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
    return pd.DataFrame(data)
