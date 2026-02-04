import pandas as pd


def _ma(series: pd.Series, n: int) -> float:
    return float(series.tail(n).mean())


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
