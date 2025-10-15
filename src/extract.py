import time
import pandas as pd
import requests
import yfinance as yf
from pathlib import Path
from typing import List, Tuple
from .utils import load_cfg, ensure_dir

def _stooq_download_one(ticker: str) -> pd.Series:
    """
    Pobiera dzienne notowania z Stooq (CSV) dla spółek z USA: <ticker>.us.
    Zwraca Series z indeksem Daty i kolumną Close.
    """
    url = f"https://stooq.com/q/d/l/?s={ticker.lower()}.us&i=d"
    df = pd.read_csv(url)

    df["Date"] = pd.to_datetime(df["Date"])
    s = df.set_index("Date")["Close"].rename(ticker).dropna().sort_index()
    return s

def fetch_prices(tickers: List[str], period: str, interval: str) -> Tuple[pd.DataFrame, list]:
    """
    Próbuje pobrać dane z Yahoo; jeśli się nie uda – spada do Stooq.
    Zwraca (prices_df, failed).
    """
    sess = requests.Session()
    sess.headers["User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 Safari/537.36"

    frames = []
    failed = []

    big = None
    for attempt in range(2):
        try:
            big = yf.download(
                " ".join(tickers),
                period=period,
                interval=interval,
                auto_adjust=True,
                group_by="ticker",
                threads=False,
                progress=False,
                session=sess,
                timeout=30,
            )
            break
        except Exception:
            time.sleep(2 ** attempt)

    def _append_series_from_yf(multidf, t):
        try:
            s = multidf[(t, "Close")].rename(t).dropna()
            if not s.empty:
                frames.append(s)
                return True
        except Exception:
            pass
        return False

    if isinstance(big, pd.DataFrame) and isinstance(big.columns, pd.MultiIndex):
        for t in tickers:
            ok = _append_series_from_yf(big, t)
            if not ok:
                # fallback: per-ticker Yahoo
                ok_yf = False
                for attempt in range(2):
                    try:
                        df = yf.download(t, period=period, interval=interval,
                                         auto_adjust=True, progress=False,
                                         session=sess, threads=False, timeout=30)
                        s = df["Close"].rename(t).dropna()
                        if not s.empty:
                            frames.append(s)
                            ok_yf = True
                            break
                    except Exception:
                        time.sleep(1.5 * (attempt + 1))
                if not ok_yf:
                    # fallback: Stooq
                    try:
                        s = _stooq_download_one(t)
                        if not s.empty:
                            frames.append(s)
                        else:
                            failed.append(t)
                    except Exception:
                        failed.append(t)
    else:
        for t in tickers:
            ok = False
            # 1) Yahoo single
            for attempt in range(2):
                try:
                    df = yf.download(t, period=period, interval=interval,
                                     auto_adjust=True, progress=False,
                                     session=sess, threads=False, timeout=30)
                    s = df["Close"].rename(t).dropna()
                    if not s.empty:
                        frames.append(s)
                        ok = True
                        break
                except Exception:
                    time.sleep(1.5 * (attempt + 1))
            # 2) Stooq fallback
            if not ok:
                try:
                    s = _stooq_download_one(t)
                    if not s.empty:
                        frames.append(s)
                        ok = True
                except Exception:
                    pass
            if not ok:
                failed.append(t)

    prices = pd.concat(frames, axis=1) if frames else pd.DataFrame()
    prices.index = pd.to_datetime(prices.index)
    prices = prices.sort_index()
    return prices, failed

def fetch_ecb_fx() -> pd.DataFrame:
    """Pobiera dzienne kursy ECB i normalizuje nazwy kolumn na lowercase."""
    url = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.csv"
    try:
        fx = pd.read_csv(url)
        fx["Date"] = pd.to_datetime(fx["Date"])
        from .utils import load_cfg
        symbols = load_cfg()["sources"]["fx"]["symbols"]
        fx = fx[["Date"] + symbols].melt(
            id_vars="Date", var_name="Currency", value_name="Rate_vs_EUR"
        ).dropna().sort_values(["Currency", "Date"])
        fx = fx.rename(columns={
            "Date": "date",
            "Currency": "currency",
            "Rate_vs_EUR": "rate_vs_eur"
        })
        return fx
    except Exception as e:
        raise RuntimeError(f"FX fetch failed: {e}")

def extract_all() -> dict:
    cfg = load_cfg()
    proc_dir = Path(cfg["paths"]["processed_dir"])
    ensure_dir(proc_dir)

    tickers  = cfg["sources"]["stocks"]["tickers"]
    period   = cfg["sources"]["stocks"]["period"]
    interval = cfg["sources"]["stocks"]["interval"]

    print("Extract…")
    stocks, failed = fetch_prices(tickers, period, interval)
    if failed:
        print("Failed downloads:", failed)
    if stocks.empty:
        raise RuntimeError("Brak danych akcji po ekstrakcji (stocks.empty == True).")

    fx = fetch_ecb_fx()
    stocks.to_parquet(proc_dir / "stocks_raw.parquet")
    fx.to_parquet(proc_dir / "fx_raw.parquet")
    print("Extracted and saved raw data.")
    return {"stocks": stocks, "fx": fx}