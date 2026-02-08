# -- Countries --
COUNTRIES = {
    "US": {"name": "United States", "wb_code": "USA", "imf_code": "US", "index": "^GSPC", "currency_pair": None},
    "EU": {"name": "Eurozone", "wb_code": "EMU", "imf_code": "U2", "index": "^STOXX50E", "currency_pair": "EURUSD=X"},
    "UK": {"name": "United Kingdom", "wb_code": "GBR", "imf_code": "GB", "index": "^FTSE", "currency_pair": "GBPUSD=X"},
    "JP": {"name": "Japan", "wb_code": "JPN", "imf_code": "JP", "index": "^N225", "currency_pair": "USDJPY=X"},
    "CN": {"name": "China", "wb_code": "CHN", "imf_code": "CN", "index": "000001.SS", "currency_pair": "USDCNY=X"},
    "CA": {"name": "Canada", "wb_code": "CAN", "imf_code": "CA", "index": "^GSPTSE", "currency_pair": "USDCAD=X"},
    "AU": {"name": "Australia", "wb_code": "AUS", "imf_code": "AU", "index": "^AXJO", "currency_pair": "AUDUSD=X"},
    "CH": {"name": "Switzerland", "wb_code": "CHE", "imf_code": "CH", "index": "^SSMI", "currency_pair": "USDCHF=X"},
    "KR": {"name": "South Korea", "wb_code": "KOR", "imf_code": "KR", "index": "^KS11", "currency_pair": "USDKRW=X"},
    "IN": {"name": "India", "wb_code": "IND", "imf_code": "IN", "index": "^BSESN", "currency_pair": "USDINR=X"},
    "BR": {"name": "Brazil", "wb_code": "BRA", "imf_code": "BR", "index": "^BVSP", "currency_pair": "USDBRL=X"},
    "MX": {"name": "Mexico", "wb_code": "MEX", "imf_code": "MX", "index": "^MXX", "currency_pair": "USDMXN=X"},
    "DE": {"name": "Germany", "wb_code": "DEU", "imf_code": "DE", "index": "^GDAXI", "currency_pair": None},
}

DEFAULT_COUNTRIES = ["US", "EU", "UK", "JP", "CN"]

# -- FRED Series IDs --
FRED = {
    # Rates
    "fed_funds": "DFF",
    "us_10y": "DGS10",
    "us_2y": "DGS2",
    "us_2s10s": "T10Y2Y",
    "real_yield_10y": "DFII10",
    "breakeven_10y": "T10YIE",
    # Liquidity
    "fed_balance_sheet": "WALCL",
    "rrp": "RRPONTSYD",
    "tga": "WTREGEN",
    "m2": "WM2NS",
    # Credit
    "hy_oas": "BAMLH0A0HYM2",
    "ig_oas": "BAMLC0A0CM",
    "nfci": "NFCI",
    # Economy
    "initial_claims": "ICSA",
    "continuing_claims": "CCSA",
    "consumer_sentiment": "UMCSENT",
    "cpi": "CPIAUCSL",
    "unemployment": "UNRATE",
    "personal_savings": "PSAVERT",
    "industrial_production": "INDPRO",
    "lei": "USALOLITONOSTSAM",
}

# -- Market Tickers (yfinance) --
MARKET_TICKERS = {
    "DXY": "DX-Y.NYB",
    "VIX": "^VIX",
    "MOVE": "^MOVE",
    "BDI": "^BDI",
    "Gold": "GC=F",
    "Copper": "HG=F",
    "WTI": "CL=F",
    "Brent": "BZ=F",
}

# -- Fed Funds Futures (yfinance) --
FF_FUTURES_BASE = "ZQ"

# -- World Bank Indicators --
WB_INDICATORS = {
    "current_account_pct_gdp": "BN.CAB.XOKA.GD.ZS",
    "trade_balance": "NE.RSB.GNFS.CD",
    "fdi_inflows": "BX.KLT.DINV.CD.WD",
    "fdi_outflows": "BM.KLT.DINV.CD.WD",
    "reserves_excl_gold": "FI.RES.TOTL.CD",
    "external_debt": "DT.DOD.DECT.CD",
    "debt_to_gdp": "GC.DOD.TOTL.GD.ZS",
    "budget_balance_pct_gdp": "GC.BAL.CASH.GD.ZS",
    "gdp_current_usd": "NY.GDP.MKTP.CD",
    "gdp_growth": "NY.GDP.MKTP.KD.ZG",
    "inflation_cpi": "FP.CPI.TOTL.ZG",
    "unemployment": "SL.UEM.TOTL.ZS",
}

# -- Date range options --
DATE_RANGES = {
    "1M": 30, "3M": 90, "6M": 180, "1Y": 365,
    "3Y": 1095, "5Y": 1825, "10Y": 3650, "MAX": None,
}

# Map sidebar date range to yfinance period strings
YF_PERIOD_MAP = {
    "1M": "1mo", "3M": "3mo", "6M": "6mo", "1Y": "1y",
    "3Y": "3y", "5Y": "5y", "10Y": "10y", "MAX": "max",
}
