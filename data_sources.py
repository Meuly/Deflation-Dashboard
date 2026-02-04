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
    df = yf.download(ticker, period=period, interval="1d", progress=False, auto_adjust=False)
    if df is None or df.empty:
        raise RuntimeError(f"No data returned for {ticker}")
    if "Adj Close" not in df.columns:
        raise RuntimeError(f"Adj Close missing for {ticker}")
    return df["Adj Close"].dropna().sort_index()
