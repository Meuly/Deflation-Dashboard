import pandas as pd


def _ma(series: pd.Series, n: int) -> float:
    m = series.tail(n).mean()
    # If mean is a Series (shouldn't be, but can happen), take first value
    if isinstance(m, pd.Series):
        m = m.iloc[0]
    return float(m)


def ryg_trend_ma(series: pd.Series, fast: int = 5, slow: int = 20, flat_band: float = 0.05):
    if series is None or len(series.dropna()) < slow:
        return "YELLOW", {"reason": "insufficient_data"}

    s = series.dropna()
    fast_ma = _ma(s, fast)
    slow_ma = _ma(s, slow)

    if fast_ma > slow_ma * (1.0 + flat_band):
        return "RED", {"fast_ma": fast_ma, "slow_ma": slow_ma}
    if fast_ma < slow_ma * (1.0 - flat_band):
        return "GREEN", {"fast_ma": fast_ma, "slow_ma": slow_ma}
    return "YELLOW", {"fast_ma": fast_ma, "slow_ma": slow_ma}


def credit_stress_us_can(us_hy_oas: pd.Series, ca_hy_etf: pd.Series):
    us_status, us_meta = ryg_trend_ma(us_hy_oas, flat_band=0.03)
    ca_status_raw, ca_meta = ryg_trend_ma(ca_hy_etf, flat_band=0.02)

    # Invert Canada ETF logic: falling price = higher credit stress
    if ca_status_raw == "RED":
        ca_status = "GREEN"
    elif ca_status_raw == "GREEN":
        ca_status = "RED"
    else:
        ca_status = "YELLOW"

    if us_status == "RED" or ca_status == "RED":
        combined = "RED"
    elif us_status == "GREEN" and ca_status == "GREEN":
        combined = "GREEN"
    else:
        combined = "YELLOW"

    return {
        "combined": combined,
        "us_status": us_status,
        "ca_status": ca_status,
        "us_meta": us_meta,
        "ca_meta": ca_meta,
    }

def real_yields_us_can(us_real_10y: pd.Series, ca_10y_nominal: pd.Series):
    """
    US: real yields (higher = tighter conditions, deflationary pressure)
    Canada: proxy using nominal 10y yield direction (higher = tighter).
    Combined:
      - RED if either is RED (tightening)
      - GREEN if both are GREEN (easing)
      - else YELLOW
    """
    us_status, us_meta = ryg_trend_ma(us_real_10y, flat_band=0.02)
    ca_status, ca_meta = ryg_trend_ma(ca_10y_nominal, flat_band=0.02)

    if us_status == "RED" or ca_status == "RED":
        combined = "RED"
    elif us_status == "GREEN" and ca_status == "GREEN":
        combined = "GREEN"
    else:
        combined = "YELLOW"

    return {
        "combined": combined,
        "us_status": us_status,
        "ca_status": ca_status,
        "us_meta": us_meta,
        "ca_meta": ca_meta,
        "note": "Canada series is a proxy (10Y nominal yield trend) until a clean real-yield series is wired."
    }

def _ratio_trend(a: pd.Series, b: pd.Series, lookback: int = 10):
    """
    Returns +1 if ratio up over lookback, -1 if down, 0 if insufficient.
    """
    if a is None or b is None:
        return 0, {"reason": "missing_data"}
    a = a.dropna()
    b = b.dropna()
    if len(a) < lookback + 1 or len(b) < lookback + 1:
        return 0, {"reason": "insufficient_data"}

    # Align by date
    df = pd.concat([a, b], axis=1, join="inner").dropna()
    if len(df) < lookback + 1:
        return 0, {"reason": "insufficient_aligned_data"}

    ratio = df.iloc[:, 0] / df.iloc[:, 1]
    start = float(ratio.iloc[-(lookback + 1)])
    end = float(ratio.iloc[-1])

    if end > start * 1.01:   # up > +1%
        return 1, {"start": start, "end": end}
    if end < start * 0.99:   # down < -1%
        return -1, {"start": start, "end": end}
    return 0, {"start": start, "end": end}


def high_beta_leadership(btc: pd.Series, spy: pd.Series, qqq: pd.Series, dia: pd.Series, iwm: pd.Series):
    """
    Measures whether high-beta is leading using 3 relative ratios.
    Score:
      - GREEN: >=2 ratios up
      - RED:   >=2 ratios down
      - YELLOW: otherwise
    """
    signals = {}
    score = 0

    s, meta = _ratio_trend(btc, spy, lookback=10)
    signals["BTC/SPY"] = {"signal": s, "meta": meta}
    score += s

    s, meta = _ratio_trend(qqq, dia, lookback=10)
    signals["QQQ/DIA"] = {"signal": s, "meta": meta}
    score += s

    s, meta = _ratio_trend(iwm, spy, lookback=10)
    signals["IWM/SPY"] = {"signal": s, "meta": meta}
    score += s

    # Convert signals to status
    ups = sum(1 for k in signals if signals[k]["signal"] == 1)
    downs = sum(1 for k in signals if signals[k]["signal"] == -1)

    if ups >= 2:
        combined = "GREEN"
    elif downs >= 2:
        combined = "RED"
    else:
        combined = "YELLOW"

    return {
        "combined": combined,
        "details": signals,
        "ups": ups,
        "downs": downs,
        "note": "High-beta leadership based on 10D relative strength of BTC/SPY, QQQ/DIA, IWM/SPY."
    }

def asset_correlations(
    xic: pd.Series,  # Canada equity proxy
    spy: pd.Series,  # US equity
    hyg: pd.Series,  # credit proxy
    xre: pd.Series,  # Canada REITs
    vnq: pd.Series,  # US REITs
    btc: pd.Series,  # bitcoin
    lookback: int = 10,
):
    """
    Computes average pairwise correlation of daily returns across assets.
    High correlation = forced selling / risk-off.
    """
    series_map = {
        "XIC.TO": xic,
        "SPY": spy,
        "HYG": hyg,
        "XRE.TO": xre,
        "VNQ": vnq,
        "BTC-USD": btc,
    }

    # Clean + align
    df = pd.DataFrame({k: v for k, v in series_map.items() if v is not None})
    df = df.dropna(how="all")

    # Need enough data
    if df.shape[0] < lookback + 2 or df.shape[1] < 3:
        return {"combined": "YELLOW", "reason": "insufficient_data"}

    # Use returns for correlation
    rets = df.pct_change().dropna()
    rets = rets.tail(lookback)

    if rets.shape[0] < lookback or rets.shape[1] < 3:
        return {"combined": "YELLOW", "reason": "insufficient_aligned_data"}

    corr = rets.corr()

    # Average pairwise correlation (upper triangle, excluding diagonal)
    vals = []
    cols = list(corr.columns)
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            v = corr.iloc[i, j]
            if pd.notna(v):
                vals.append(float(v))

    if not vals:
        return {"combined": "YELLOW", "reason": "no_corr_values"}

    avg_corr = sum(vals) / len(vals)

    # Thresholds (tunable)
    if avg_corr >= 0.75:
        combined = "RED"
    elif avg_corr <= 0.55:
        combined = "GREEN"
    else:
        combined = "YELLOW"

    return {
        "combined": combined,
        "avg_corr": avg_corr,
        "assets_used": cols,
        "lookback_days": lookback,
        "note": "Higher correlation implies forced selling / risk-off; lower implies dispersion / healthier market."
    }
