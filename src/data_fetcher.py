"""
Data fetcher module - provides all data for the dashboard.

Data loading priority:
  1. Parquet files in data/ (populated by ingestor.py) - fast, historical
  2. Live API calls (when keys are set) - real-time

For historical data: run `python ingestor.py --full` to populate Parquet files.
"""

import pandas as pd
import streamlit as st
from pathlib import Path
from src.config import COUNTRIES, SEMI_TICKERS, SEMI_ETFS


# ---------------------------------------------------------------------------
# Parquet loading layer
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).parent.parent / "data"

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
# Market Data
# ---------------------------------------------------------------------------

@st.cache_data(ttl=900)
def get_index_data(ticker: str, period: str = "5y") -> pd.DataFrame:
    """Single index/ticker history. Returns OHLCV df from Parquet."""
    pq = _load_parquet("market", ticker)
    if pq is not None:
        return _filter_by_period(pq, period)
    return pd.DataFrame()


@st.cache_data(ttl=900)
def get_multiple_tickers(tickers: list, period: str = "5y") -> pd.DataFrame:
    """Multiple tickers, returns df with Close prices as columns."""
    dfs = {}
    for t in tickers:
        data = get_index_data(t, period)
        if not data.empty and "Close" in data.columns:
            dfs[t] = data["Close"]
    return pd.DataFrame(dfs)


@st.cache_data(ttl=900)
def get_fx_rates(country_codes: list, period: str = "5y") -> pd.DataFrame:
    """Get FX rates for selected countries from Parquet."""
    fx_data = {}
    for code in country_codes:
        pair = COUNTRIES.get(code, {}).get("currency_pair")
        if not pair:
            continue
        pq = _load_parquet("market", pair)
        if pq is not None and "Close" in pq.columns:
            fx_data[pair] = pq["Close"]
    if not fx_data:
        return pd.DataFrame()
    df = pd.DataFrame(fx_data)
    return _filter_by_period(df, period)


@st.cache_data(ttl=900)
def get_dxy(period: str = "5y") -> pd.DataFrame:
    """DXY index from Parquet."""
    pq = _load_parquet("market", "DXY")
    if pq is not None:
        return _filter_by_period(pq, period)
    return pd.DataFrame()


@st.cache_data(ttl=900)
def get_commodities(period: str = "5y") -> pd.DataFrame:
    """Gold, Copper, WTI, Brent, BDI from Parquet."""
    comm_map = {"Gold": "GC=F", "Copper": "HG=F", "WTI": "CL=F", "Brent": "BZ=F", "BDI": "^BDI"}
    comm_data = {}
    for name, ticker in comm_map.items():
        pq = _load_parquet("market", ticker)
        if pq is not None and "Close" in pq.columns:
            comm_data[name] = pq["Close"]
    if not comm_data:
        return pd.DataFrame()
    return _filter_by_period(pd.DataFrame(comm_data), period)


@st.cache_data(ttl=900)
def get_volatility(period: str = "5y") -> pd.DataFrame:
    """VIX and MOVE index from Parquet."""
    vol_data = {}
    for name, ticker in [("VIX", "^VIX"), ("MOVE", "^MOVE")]:
        pq = _load_parquet("market", ticker)
        if pq is not None and "Close" in pq.columns:
            vol_data[name] = pq["Close"]
    if not vol_data:
        return pd.DataFrame()
    return _filter_by_period(pd.DataFrame(vol_data), period)


# ---------------------------------------------------------------------------
# FRED Data
# ---------------------------------------------------------------------------

@st.cache_data(ttl=21600)
def get_fred_series(series_id: str, start: str = "2000-01-01") -> pd.Series:
    """Single FRED series from Parquet."""
    pq = _load_parquet("fred", series_id)
    if pq is not None:
        series = pq.iloc[:, 0]
        series.name = series_id
        # Forward-fill NaN gaps (standard for financial time series —
        # e.g. Fed Funds Rate on a holiday equals the last trading day's rate)
        series = series.ffill()
        return series
    return pd.Series(dtype=float, name=series_id)


@st.cache_data(ttl=21600)
def get_fred_multiple(series_ids: list, start: str = "2000-01-01") -> pd.DataFrame:
    """Multiple FRED series merged into one df."""
    dfs = {}
    for sid in series_ids:
        s = get_fred_series(sid, start)
        if not s.empty:
            dfs[sid] = s
    return pd.DataFrame(dfs)


@st.cache_data(ttl=21600)
def get_yield_curve_snapshot() -> pd.DataFrame:
    """Current yield curve from Parquet. Returns empty DataFrame if not available."""
    pq = _load_parquet("fred", "yield_curve_snapshot")
    if pq is not None:
        return pq
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# World Bank Data
# ---------------------------------------------------------------------------

@st.cache_data(ttl=604800)
def get_wb_indicator(indicator: str, countries: list, start_year: int = 2000) -> pd.DataFrame:
    """Fetch a World Bank indicator from Parquet."""
    pq = _load_parquet("world_bank", indicator)
    if pq is not None:
        available_cols = [c for c in countries if c in pq.columns]
        if available_cols:
            df = pq[available_cols]
            if start_year and hasattr(df.index, 'min'):
                df = df[df.index >= start_year]
            return df
    return pd.DataFrame()


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
    """Fetch IMF Balance of Payments data from Parquet."""
    pq = _load_parquet("imf", "bop")
    if pq is not None and country_code in pq.columns:
        return pd.DataFrame({"Value": pq[country_code]})
    return pd.DataFrame()


@st.cache_data(ttl=2592000)
def get_imf_gold_reserves(country_code: str) -> pd.DataFrame:
    """Fetch gold reserves from Parquet."""
    pq = _load_parquet("imf", "gold_reserves")
    if pq is not None and country_code in pq.columns:
        return pd.DataFrame({"Tonnes": pq[country_code]})
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# ECB Data
# ---------------------------------------------------------------------------

@st.cache_data(ttl=86400)
def get_ecb_fx(currency: str = "USD") -> pd.DataFrame:
    """ECB reference rate from Parquet."""
    pq = _load_parquet("ecb", f"fx_{currency}")
    if pq is not None:
        return pq
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# BIS Data
# ---------------------------------------------------------------------------

@st.cache_data(ttl=2592000)
def get_bis_reer(country: str) -> pd.DataFrame:
    """Real Effective Exchange Rate from BIS from Parquet."""
    pq = _load_parquet("bis", f"reer_{country}")
    if pq is not None:
        return pq
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Fed Funds Futures
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def get_fed_funds_futures() -> pd.DataFrame:
    """Fed funds futures implied rate path from Parquet."""
    pq = _load_parquet("fred", "fed_funds_futures")
    if pq is not None:
        return pq
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Semiconductor / Strategic Sector Data
# ---------------------------------------------------------------------------

@st.cache_data(ttl=900)
def get_semi_stocks(period: str = "5y") -> pd.DataFrame:
    """Get semiconductor stock prices from Parquet."""
    semi_data = {}
    for label, ticker in SEMI_TICKERS.items():
        pq = _load_parquet("semi", ticker)
        if pq is not None and "Close" in pq.columns:
            semi_data[label] = pq["Close"]
    if not semi_data:
        return pd.DataFrame()
    return _filter_by_period(pd.DataFrame(semi_data), period)


@st.cache_data(ttl=900)
def get_semi_etfs(period: str = "5y") -> pd.DataFrame:
    """Get semiconductor ETF prices from Parquet."""
    etf_data = {}
    for label, ticker in SEMI_ETFS.items():
        pq = _load_parquet("semi", ticker)
        if pq is not None and "Close" in pq.columns:
            etf_data[label] = pq["Close"]
    if not etf_data:
        return pd.DataFrame()
    return _filter_by_period(pd.DataFrame(etf_data), period)


@st.cache_data(ttl=900)
def get_semi_vs_market(period: str = "5y") -> pd.DataFrame:
    """SOX index vs S&P 500 for relative performance from Parquet."""
    sox_pq = _load_parquet("semi", "^SOX")
    spx_pq = _load_parquet("market", "^GSPC")
    if sox_pq is not None and spx_pq is not None:
        sox_close = sox_pq["Close"] if "Close" in sox_pq.columns else sox_pq.iloc[:, 0]
        spx_close = spx_pq["Close"] if "Close" in spx_pq.columns else spx_pq.iloc[:, 0]
        df = pd.DataFrame({"SOX (Semis)": sox_close, "S&P 500": spx_close}).dropna()
        return _filter_by_period(df, period)
    return pd.DataFrame()


@st.cache_data(ttl=86400)
def get_semi_revenue_cycle() -> pd.DataFrame:
    """Global semiconductor revenue cycle data from Parquet."""
    pq = _load_parquet("semi", "revenue_cycle")
    if pq is not None:
        return pq
    return pd.DataFrame()


@st.cache_data(ttl=86400)
def get_semi_inventory_cycle() -> pd.DataFrame:
    """Semiconductor inventory/book-to-bill data from Parquet."""
    pq = _load_parquet("semi", "inventory_cycle")
    if pq is not None:
        return pq
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Policy & Geopolitical Events
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def get_policy_events() -> pd.DataFrame:
    """Curated macro-relevant policy events from Parquet."""
    pq = _load_parquet("policy", "events")
    if pq is not None:
        pq["date"] = pd.to_datetime(pq["date"])
        return pq.sort_values("date", ascending=False).reset_index(drop=True)
    return pd.DataFrame()


@st.cache_data(ttl=86400)
def get_central_bank_calendar() -> pd.DataFrame:
    """Upcoming central bank meeting dates from Parquet."""
    pq = _load_parquet("policy", "cb_calendar")
    if pq is not None:
        pq["date"] = pd.to_datetime(pq["date"])
        return pq.sort_values("date").reset_index(drop=True)
    return pd.DataFrame()


@st.cache_data(ttl=86400)
def get_tariff_tracker() -> pd.DataFrame:
    """Track current effective tariff rates from Parquet."""
    pq = _load_parquet("policy", "tariff_tracker")
    if pq is not None:
        return pq
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# BIS REER Data
# ---------------------------------------------------------------------------

@st.cache_data(ttl=2592000)
def get_bis_reer(country: str) -> pd.DataFrame:
    """Real Effective Exchange Rate from BIS from Parquet."""
    pq = _load_parquet("bis", f"reer_{country}")
    if pq is not None:
        return pq
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# CFTC Commitments of Traders
# ---------------------------------------------------------------------------

@st.cache_data(ttl=86400)
def get_cot_data(category: str = "fx") -> pd.DataFrame:
    """CFTC COT positioning data from Parquet.
    category: 'fx', 'rates', or 'commodities'"""
    pq = _load_parquet("cftc", f"cot_{category}")
    if pq is not None:
        pq["date"] = pd.to_datetime(pq["date"])
        return pq.sort_values(["contract", "date"]).reset_index(drop=True)
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Geopolitical / Policy Uncertainty Indices
# ---------------------------------------------------------------------------

@st.cache_data(ttl=86400)
def get_epu_index() -> pd.Series:
    """Economic Policy Uncertainty Index from FRED Parquet."""
    pq = _load_parquet("fred", "USEPUINDXD")
    if pq is not None:
        series = pq.iloc[:, 0]
        series.name = "EPU"
        return series.ffill()
    return pd.Series(dtype=float, name="EPU")


@st.cache_data(ttl=86400)
def get_gpr_index() -> pd.DataFrame:
    """Caldara-Iacoviello Geopolitical Risk Index from Parquet."""
    pq = _load_parquet("policy", "gpr_index")
    if pq is not None:
        return pq
    return pd.DataFrame()
