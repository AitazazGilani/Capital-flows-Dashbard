"""
Computed/derived indicators for the macro dashboard.
"""

import pandas as pd
import numpy as np
from config import COUNTRIES, POLICY_RATES, COUNTRY_PE_ESTIMATES


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


# ---------------------------------------------------------------------------
# Cross-Asset / Relative Value Processors
# ---------------------------------------------------------------------------

def compute_rate_differentials(countries: list) -> pd.DataFrame:
    """Compute interest rate differentials vs US for carry trade analysis.
    Positive = country rate > US rate (attracts capital via carry).
    Returns df with countries as index."""
    us_rate = POLICY_RATES.get("US", 5.33)
    rows = []
    for c in countries:
        if c == "US":
            continue
        rate = POLICY_RATES.get(c, np.nan)
        if not np.isnan(rate):
            diff = rate - us_rate
            carry = "Positive Carry" if diff > 0.5 else ("Negative Carry" if diff < -0.5 else "Neutral")
            rows.append({
                "Country": c,
                "Name": COUNTRIES[c]["name"],
                "Policy Rate (%)": rate,
                "US Rate (%)": us_rate,
                "Differential (%)": round(diff, 2),
                "Carry Signal": carry,
            })
    return pd.DataFrame(rows).set_index("Country")


def compute_equity_risk_premium(countries: list, real_yields: dict = None) -> pd.DataFrame:
    """Equity risk premium = earnings yield - real yield.
    Higher ERP = equities are cheap relative to bonds.
    If real_yields not provided, estimate from policy rate - inflation."""
    rows = []
    for c in countries:
        pe = COUNTRY_PE_ESTIMATES.get(c)
        if pe is None:
            continue
        earnings_yield = (1 / pe) * 100
        # Approximate real yield: policy rate minus ~2.5% inflation assumption
        # (In production, use actual real yield data from FRED/central banks)
        policy_rate = POLICY_RATES.get(c, 3.0)
        inflation_est = {"US": 3.2, "EU": 2.5, "UK": 3.8, "JP": 2.8, "CN": 0.3,
                         "CA": 3.0, "AU": 4.0, "CH": 1.4, "KR": 3.2, "IN": 5.0,
                         "BR": 4.5, "MX": 4.8, "DE": 2.5}.get(c, 2.5)
        real_yield = policy_rate - inflation_est
        erp = earnings_yield - real_yield

        rows.append({
            "Country": c,
            "Name": COUNTRIES[c]["name"],
            "P/E": pe,
            "Earnings Yield (%)": round(earnings_yield, 2),
            "Est. Real Yield (%)": round(real_yield, 2),
            "ERP (%)": round(erp, 2),
            "Valuation": "Cheap" if erp > 4 else ("Rich" if erp < 1 else "Fair"),
        })
    return pd.DataFrame(rows).set_index("Country")


def compute_relative_value_matrix(countries: list, erp_df: pd.DataFrame,
                                   carry_df: pd.DataFrame,
                                   flow_signals: pd.DataFrame) -> pd.DataFrame:
    """Synthesize ERP, carry, and flow signals into a single relative value score.
    Score from -3 (avoid) to +3 (attractive).
    """
    rows = []
    for c in countries:
        if c == "US":
            # US is the benchmark
            rows.append({"Country": c, "Name": COUNTRIES[c]["name"],
                         "ERP Score": 0, "Carry Score": 0, "Flow Score": 0,
                         "Total Score": 0, "Signal": "Benchmark"})
            continue

        score = 0

        # ERP component
        erp_score = 0
        if c in erp_df.index:
            erp = erp_df.loc[c, "ERP (%)"]
            if erp > 4:
                erp_score = 1
            elif erp < 1:
                erp_score = -1
            score += erp_score

        # Carry component
        carry_score = 0
        if c in carry_df.index:
            diff = carry_df.loc[c, "Differential (%)"]
            if diff > 1:
                carry_score = 1
            elif diff < -1:
                carry_score = -1
            score += carry_score

        # Flow component
        flow_score = 0
        if c in flow_signals.index:
            signal = flow_signals.loc[c, "Signal"] if "Signal" in flow_signals.columns else "Neutral"
            if signal == "Inflow":
                flow_score = 1
            elif signal == "Outflow":
                flow_score = -1
            score += flow_score

        if score >= 2:
            signal = "Attractive"
        elif score <= -2:
            signal = "Avoid"
        else:
            signal = "Neutral"

        rows.append({
            "Country": c,
            "Name": COUNTRIES[c]["name"],
            "ERP Score": erp_score,
            "Carry Score": carry_score,
            "Flow Score": flow_score,
            "Total Score": score,
            "Signal": signal,
        })

    return pd.DataFrame(rows).set_index("Country")


def compute_cross_asset_momentum(equity_df: pd.DataFrame, fx_df: pd.DataFrame,
                                  commodity_df: pd.DataFrame) -> pd.DataFrame:
    """Compute momentum signals across asset classes.
    Uses 1-month and 3-month returns to identify trend direction."""
    signals = {}

    for label, df, default_cols in [
        ("Equities", equity_df, None),
        ("FX", fx_df, None),
        ("Commodities", commodity_df, None),
    ]:
        if df is None or df.empty:
            continue
        for col in df.columns:
            series = df[col].dropna()
            if len(series) < 63:
                continue
            ret_1m = (series.iloc[-1] / series.iloc[-22] - 1) * 100
            ret_3m = (series.iloc[-1] / series.iloc[-63] - 1) * 100

            if ret_1m > 2 and ret_3m > 5:
                trend = "Strong Up"
            elif ret_1m > 0 and ret_3m > 0:
                trend = "Up"
            elif ret_1m < -2 and ret_3m < -5:
                trend = "Strong Down"
            elif ret_1m < 0 and ret_3m < 0:
                trend = "Down"
            else:
                trend = "Mixed"

            signals[col] = {
                "Asset Class": label,
                "1M Return (%)": round(ret_1m, 2),
                "3M Return (%)": round(ret_3m, 2),
                "Trend": trend,
            }

    return pd.DataFrame(signals).T


def compute_macro_catalyst_score(policy_events: pd.DataFrame,
                                  lookback_days: int = 90) -> pd.DataFrame:
    """Score the net policy environment as bullish/bearish based on recent events.
    Counts positive vs negative impact events in the lookback window."""
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=lookback_days)
    recent = policy_events[policy_events["date"] >= cutoff].copy()

    if recent.empty:
        return pd.DataFrame()

    # Score by country
    country_scores = {}
    for _, row in recent.iterrows():
        c = row["country"]
        impact = row["impact"]
        if c not in country_scores:
            country_scores[c] = {"positive": 0, "negative": 0, "neutral": 0}
        country_scores[c][impact.lower()] = country_scores[c].get(impact.lower(), 0) + 1

    rows = []
    for c, counts in country_scores.items():
        net = counts.get("positive", 0) - counts.get("negative", 0)
        total = sum(counts.values())
        rows.append({
            "Country": c,
            "Positive Events": counts.get("positive", 0),
            "Negative Events": counts.get("negative", 0),
            "Mixed/Neutral": counts.get("neutral", 0) + counts.get("mixed", 0),
            "Net Score": net,
            "Policy Bias": "Bullish" if net > 0 else ("Bearish" if net < 0 else "Neutral"),
        })

    return pd.DataFrame(rows).set_index("Country")


def compute_semi_relative_strength(semi_series: pd.Series, market_series: pd.Series) -> pd.Series:
    """Relative strength = semi / market, rebased to 100.
    Rising = semis outperforming. Falling = semis underperforming."""
    ratio = semi_series / market_series
    rebased = ratio / ratio.dropna().iloc[0] * 100
    rebased.name = "Semi vs Market (Relative Strength)"
    return rebased
