"""
Computed/derived indicators for the macro dashboard.
"""

import pandas as pd
import numpy as np


def compute_net_liquidity(fed_bs: pd.Series, tga: pd.Series, rrp: pd.Series) -> pd.Series:
    """Net Liquidity = Fed Balance Sheet - TGA - RRP. Align dates, forward fill, subtract."""
    df = pd.DataFrame({"fed_bs": fed_bs, "tga": tga, "rrp": rrp}).ffill()
    net_liq = df["fed_bs"] - df["tga"] - df["rrp"]
    net_liq.name = "Net Liquidity"
    return net_liq


def compute_implied_rate_path(futures_df: pd.DataFrame, current_rate: float) -> pd.DataFrame:
    """From futures prices, compute implied rate per meeting and number of cuts/hikes priced in."""
    df = futures_df.copy()
    df["change_from_current"] = df["implied_rate"] - current_rate
    df["cuts_25bp"] = (df["change_from_current"] / -0.25).round(1)
    return df


def compute_copper_gold_ratio(copper: pd.Series, gold: pd.Series) -> pd.Series:
    """Copper/Gold ratio. Multiply copper by 1000 to get similar scale (copper is per lb, gold per oz)."""
    # Copper is ~$3-5/lb, gold is ~$1900-2100/oz
    # Ratio = copper * 1000 / gold to get a unitless ratio
    ratio = (copper * 1000) / gold
    ratio.name = "Copper/Gold Ratio"
    return ratio


def compute_flow_signals(ca_data: pd.DataFrame, reserves_data: pd.DataFrame,
                         fdi_data: pd.DataFrame, fx_data: pd.DataFrame) -> pd.DataFrame:
    """Per-country composite signal based on:
    - Current account trend (improving/deteriorating)
    - Reserve changes (accumulating/depleting)
    - FDI trend
    - FX momentum
    Returns df with country rows and signal columns."""
    countries = ca_data.columns.tolist()
    signals = []

    for country in countries:
        row = {"Country": country}

        # Current account trend (last 3 years)
        if country in ca_data.columns and len(ca_data[country].dropna()) >= 3:
            ca_vals = ca_data[country].dropna().values
            ca_trend = ca_vals[-1] - ca_vals[-3] if len(ca_vals) >= 3 else 0
            row["CA Trend"] = "Improving" if ca_trend > 0.5 else ("Deteriorating" if ca_trend < -0.5 else "Stable")
        else:
            row["CA Trend"] = "N/A"

        # Reserve changes
        if country in reserves_data.columns and len(reserves_data[country].dropna()) >= 2:
            res_vals = reserves_data[country].dropna().values
            res_chg = (res_vals[-1] - res_vals[-2]) / abs(res_vals[-2]) * 100 if res_vals[-2] != 0 else 0
            row["Reserve Trend"] = "Accumulating" if res_chg > 2 else ("Depleting" if res_chg < -2 else "Stable")
        else:
            row["Reserve Trend"] = "N/A"

        # FDI trend
        if country in fdi_data.columns and len(fdi_data[country].dropna()) >= 2:
            fdi_vals = fdi_data[country].dropna().values
            fdi_net = fdi_vals[-1]
            row["FDI Net"] = "Positive" if fdi_net > 0 else "Negative"
        else:
            row["FDI Net"] = "N/A"

        # FX momentum (use last available column from fx_data matching country)
        fx_cols = [c for c in fx_data.columns if country in c] if not fx_data.empty else []
        if fx_cols:
            fx_series = fx_data[fx_cols[0]].dropna()
            if len(fx_series) >= 60:
                pct_chg = (fx_series.iloc[-1] / fx_series.iloc[-60] - 1) * 100
                row["FX Momentum"] = "Strengthening" if pct_chg < -1 else ("Weakening" if pct_chg > 1 else "Stable")
            else:
                row["FX Momentum"] = "N/A"
        else:
            row["FX Momentum"] = "N/A"

        # Composite signal
        score = 0
        if row["CA Trend"] == "Improving":
            score += 1
        elif row["CA Trend"] == "Deteriorating":
            score -= 1
        if row["Reserve Trend"] == "Accumulating":
            score += 1
        elif row["Reserve Trend"] == "Depleting":
            score -= 1
        if row["FDI Net"] == "Positive":
            score += 1
        elif row["FDI Net"] == "Negative":
            score -= 1
        if row["FX Momentum"] == "Strengthening":
            score += 1
        elif row["FX Momentum"] == "Weakening":
            score -= 1

        if score >= 2:
            row["Signal"] = "Inflow"
        elif score <= -2:
            row["Signal"] = "Outflow"
        else:
            row["Signal"] = "Neutral"

        signals.append(row)

    return pd.DataFrame(signals).set_index("Country")


def compute_risk_scores(debt_gdp: pd.DataFrame, ca_gdp: pd.DataFrame,
                        reserves: pd.DataFrame, budget: pd.DataFrame) -> pd.DataFrame:
    """Simple percentile-based risk score per country.
    Higher debt/GDP = worse, bigger deficit = worse, lower reserves = worse.
    Return df with countries and composite score 0-100 (higher = riskier)."""
    countries = set(debt_gdp.columns) & set(ca_gdp.columns)
    rows = []

    for country in sorted(countries):
        row = {"Country": country}

        # Get latest values
        debt_val = debt_gdp[country].dropna().iloc[-1] if country in debt_gdp.columns and not debt_gdp[country].dropna().empty else np.nan
        ca_val = ca_gdp[country].dropna().iloc[-1] if country in ca_gdp.columns and not ca_gdp[country].dropna().empty else np.nan
        res_val = reserves[country].dropna().iloc[-1] if country in reserves.columns and not reserves[country].dropna().empty else np.nan
        budget_val = budget[country].dropna().iloc[-1] if country in budget.columns and not budget[country].dropna().empty else np.nan

        row["Debt/GDP"] = round(debt_val, 1) if not np.isnan(debt_val) else None
        row["CA/GDP"] = round(ca_val, 1) if not np.isnan(ca_val) else None
        row["Reserves ($B)"] = round(res_val / 1e9, 1) if not np.isnan(res_val) else None
        row["Budget Bal (% GDP)"] = round(budget_val, 1) if not np.isnan(budget_val) else None

        # Score components (0-25 each, higher = riskier)
        debt_score = min(25, max(0, (debt_val - 30) / 10)) if not np.isnan(debt_val) else 12.5
        ca_score = min(25, max(0, (-ca_val + 5) * 2.5)) if not np.isnan(ca_val) else 12.5
        # Lower reserves = riskier (invert, normalize roughly)
        res_score = min(25, max(0, 25 - (res_val / 1e11))) if not np.isnan(res_val) else 12.5
        budget_score = min(25, max(0, (-budget_val + 2) * 3)) if not np.isnan(budget_val) else 12.5

        row["Risk Score"] = round(debt_score + ca_score + res_score + budget_score, 0)
        rows.append(row)

    df = pd.DataFrame(rows).set_index("Country")
    return df
