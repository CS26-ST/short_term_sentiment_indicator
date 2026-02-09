import os
import time
import argparse
from datetime import datetime, timedelta

import pandas as pd

# yfinance is simple and works fine for NSE daily closes
import yfinance as yf


def normalize_symbol(sym: str) -> str:
    """
    Convert symbols to Yahoo format.
    If symbol already ends with .NS / .BO, keep it.
    Otherwise append .NS (NSE).
    """
    s = str(sym).strip().upper()
    if not s:
        return s
    if s.endswith(".NS") or s.endswith(".BO"):
        return s
    return s + ".NS"


def read_symbols_csv(path: str) -> list[str]:
    df = pd.read_csv(path)
    # try common column names; otherwise use first column
    for col in ["symbol", "Symbol", "SYMBOL", "ticker", "Ticker", "TICKER"]:
        if col in df.columns:
            syms = df[col].dropna().astype(str).tolist()
            return [s.strip() for s in syms if s.strip()]
    # fallback: first column
    syms = df.iloc[:, 0].dropna().astype(str).tolist()
    return [s.strip() for s in syms if s.strip()]


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]


def fetch_close_wide(symbols: list[str], start: str, end: str, batch_size: int = 80, pause_sec: float = 1.0) -> pd.DataFrame:
    """
    Fetch Adj Close (preferred) else Close from Yahoo Finance.
    Returns a wide dataframe with Date index and one column per original (non-suffixed) symbol.
    """
    out_parts = []

    # Map original symbol -> yahoo symbol
    original = [s.strip().upper() for s in symbols]
    yahoo_syms = [normalize_symbol(s) for s in original]

    # Keep mapping for column rename back to original (without .NS)
    rename_map = {normalize_symbol(s): s for s in original}

    for batch in chunks(yahoo_syms, batch_size):
        tickers_str = " ".join(batch)
        try:
            df = yf.download(
                tickers=tickers_str,
                start=start,
                end=end,
                interval="1d",
                group_by="column",
                auto_adjust=False,
                threads=True,
                progress=False
            )
        except Exception as e:
            print(f"Error downloading batch: {batch[:5]}... ({len(batch)} tickers): {e}")
            time.sleep(max(2.0, pause_sec))
            continue

        # yfinance returns:
        # - MultiIndex columns when multiple tickers
        # - Single-level columns when one ticker
        closes = None

        if isinstance(df.columns, pd.MultiIndex):
            # Prefer Adj Close, fallback to Close
            if ("Adj Close" in df.columns.levels[0]):
                closes = df["Adj Close"].copy()
            else:
                closes = df["Close"].copy()
            # rename columns to original symbol (without suffix)
            closes = closes.rename(columns=rename_map)
        else:
            # Single ticker case
            col = "Adj Close" if "Adj Close" in df.columns else "Close"
            closes = df[[col]].copy()
            # Name it with the original (first in batch)
            one = batch[0]
            closes = closes.rename(columns={col: rename_map.get(one, one)})

        out_parts.append(closes)

        time.sleep(pause_sec)

    if not out_parts:
        return pd.DataFrame()

    merged = pd.concat(out_parts, axis=1)

    # Remove duplicate columns if any (keep first)
    merged = merged.loc[:, ~merged.columns.duplicated()]

    # Sort index and ensure Date index name
    merged.index = pd.to_datetime(merged.index)
    merged = merged.sort_index()
    merged.index.name = "Date"

    return merged


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols_csv", default="data/nifty500/nifty500_symbols.csv", help="Symbols CSV path")
    ap.add_argument("--out_csv", default="data/nifty500/close.csv", help="Output wide close CSV")
    ap.add_argument("--years", type=int, default=2, help="How many years of history")
    ap.add_argument("--batch_size", type=int, default=80, help="Yahoo batch size")
    ap.add_argument("--pause_sec", type=float, default=1.0, help="Pause between batches")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.out_csv), exist_ok=True)

    symbols = read_symbols_csv(args.symbols_csv)
    if not symbols:
        raise SystemExit(f"No symbols found in {args.symbols_csv}")

    end_dt = datetime.utcnow().date() + timedelta(days=1)  # include latest
    start_dt = datetime.utcnow().date() - timedelta(days=int(args.years * 365.25))

    start = start_dt.isoformat()
    end = end_dt.isoformat()

    print(f"Fetching {len(symbols)} symbols from {start} to {end} ...")
    close = fetch_close_wide(
        symbols=symbols,
        start=start,
        end=end,
        batch_size=args.batch_size,
        pause_sec=args.pause_sec
    )

    if close.empty:
        raise SystemExit("No data downloaded. Check symbols format / Yahoo availability / rate limits.")

    close.to_csv(args.out_csv)
    print(f"Saved close prices to: {args.out_csv}")
    print("Sample:")
    print(close.tail(3).to_string())


if __name__ == "__main__":
    main()
