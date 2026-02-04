import io
import requests
import pandas as pd
import yfinance as yf


def fred_series_csv(series_id: str) -> pd.Series:
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()

    df = pd.read_csv(io.StringIO(r.text))
    df.columns = ["date", "value"]
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna()

    return df.set_index("date")["value"].sort_index()


def yahoo_adj_close(ticker: str, period: str = "6mo") -> pd.Series:
    """
    Pulls Adj Close from Yahoo Finance via yfinance and returns a clean 1-D numeric Series.
    Handles cases where yfinance returns multi-index columns.
    """
    df = yf.download(ticker, period=period, interval="1d", progress=False, auto_adjust=False, group_by="column")
    if df is None or df.empty:
        raise RuntimeError(f"No data returned for {ticker}")

    # If columns are MultiIndex (common), extract the field properly
    if isinstance(df.columns, pd.MultiIndex):
        # expected form: (field, ticker) or (field, something)
        if ("Adj Close", ticker) in df.columns:
            s = df[("Adj Close", ticker)]
        elif ("Adj Close",) in df.columns:
            s = df[("Adj Close",)]
        else:
            # fallback: take first column that contains 'Adj Close'
            candidates = [c for c in df.columns if isinstance(c, tuple) and "Adj Close" in c]
            if not candidates:
                raise RuntimeError(f"Adj Close missing for {ticker}")
            s = df[candidates[0]]
    else:
        if "Adj Close" not in df.columns:
            raise RuntimeError(f"Adj Close missing for {ticker}")
        s = df["Adj Close"]

    s = pd.to_numeric(s, errors="coerce").dropna().sort_index()

    # If still a DataFrame somehow, squeeze to Series
    if hasattr(s, "squeeze"):
        s = s.squeeze()

    if not isinstance(s, pd.Series) or s.empty:
        raise RuntimeError(f"Adj Close extraction failed for {ticker}")

    return s
