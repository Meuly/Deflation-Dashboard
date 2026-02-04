import pandas as pd


def _ma(series: pd.Series, n: int) -> float:
    return float(series.tail(n).mean())


def _pct_change(series: pd.Series, n: int) -> float:
    # percent change over n trading days
    if len(series) < n + 1:
        return float("nan")
    start = float(series.iloc[-(n + 1)])
    end = float(series.iloc[-1])
    if start == 0:
        return float("nan")
    return (end / start) - 1.0


def ryg_trend_ma(series: pd.Series, fast: int = 5, slow: int = 20, flat_band: float = 0.05):
    """
    Generic R/Y/G based on fast MA vs slow MA.
    - RED: fast > slow*(1+flat_band)
    - GREEN: fast < slow*(1-flat_band)
    - YELLOW: in between or insufficient data
    """
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
    """
    US: HY OAS (higher = worse) via MA trend.
    Canada: HY ETF price (lower = worse) via MA trend, inverted.
    Combined:
      - RED if either side is RED
      - GREEN if both are GREEN
      - else YELLOW
    """
    us_status, us_meta = ryg_trend_ma(us_hy_oas, flat_band=0.03)  # tighter band
    ca_status_raw, ca_meta = ryg_trend_ma(ca_hy_etf, flat_band=0.02)

    # Invert Canada ETF logic: price trend down = stress up
    if ca_status_raw == "RED":
        ca_status = "GREEN"
    elif ca_status_raw == "GREEN":
        ca_status = "RED"
    else:
        ca_status = "YELLOW"

    # Combine conservatively
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
