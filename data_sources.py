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

def boc_series_csv(series_url: str) -> pd.Series:
    """
    Pulls a BoC Valet CSV URL and returns a pandas Series indexed by date.
    Handles Valet CSV headers and series-coded value columns.
    """
    r = requests.get(series_url, timeout=20)
    r.raise_for_status()

    text = r.text.splitlines()

    # Find the header row that starts the actual data table
    header_idx = None
    for i, line in enumerate(text):
        # Valet uses quoted CSV; header usually starts with "date"
        if line.strip().lower().startswith('"date"') or line.strip().lower().startswith("date"):
            header_idx = i
            break

    if header_idx is None:
        raise RuntimeError("Could not find data header row in BoC CSV response")

    data_text = "\n".join(text[header_idx:])

    df = pd.read_csv(io.StringIO(data_text))
    # First column should be date
    date_col = df.columns[0]
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # Value column: usually the second column (series code)
    if len(df.columns) < 2:
        raise RuntimeError("BoC CSV data does not contain a value column")
    value_col = df.columns[1]
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")

    df = df.dropna(subset=[date_col, value_col])

    return df.set_index(date_col)[value_col].sort_index()
