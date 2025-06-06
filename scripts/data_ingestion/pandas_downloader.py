
import os
import requests
import yfinance as yf
import logging
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date, timedelta
from tqdm import tqdm
import time

# 📌 CONFIGURACIÓN GLOBAL
logging.basicConfig(level=logging.INFO)
DEFAULT_START_DATE = os.getenv("HISTORICAL_START_DATE", "2015-01-01")
DEFAULT_END_DATE = os.getenv("HISTORICAL_END_DATE", date.today().isoformat())
DEFAULT_OUTPUT_DIR = os.getenv("HISTORICAL_DATA_DIR", "data/historical")
SNAPSHOT_PATH = os.getenv("SNAPSHOT_OUTPUT", "data/processed/precios_diarios.csv")

os.makedirs(DEFAULT_OUTPUT_DIR, exist_ok=True)
os.makedirs(os.path.dirname(SNAPSHOT_PATH), exist_ok=True)

# 🔎 OBTENER TICKERS
def get_sp500_tickers():
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    html = requests.get(url).text
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", {"id": "constituents"})
    tickers = [
        row.find_all("td")[0].text.strip().replace(".", "-")
        for row in table.find_all("tr")[1:]
    ]
    return tickers

def get_ibex35_tickers():
    url = "https://es.wikipedia.org/wiki/IBEX_35"
    html = requests.get(url).text
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", {"class": "wikitable"})
    tickers = []
    for row in table.find_all("tr")[1:]:
        try:
            name = row.find_all("td")[1].text.strip()
            ticker = name.split()[0].replace(",", "") + ".MC"
            tickers.append(ticker)
        except Exception:
            continue
    return tickers

# 🔄 Verifica si hay datos nuevos que añadir
def get_last_saved_date(ticker, output_dir=DEFAULT_OUTPUT_DIR):
    path = os.path.join(output_dir, f"{ticker.replace('.', '_')}.parquet")
    if not os.path.exists(path):
        return None
    try:
        df = pd.read_parquet(path)
        return pd.to_datetime(df["Date"]).max().date()
    except Exception:
        return None

# 📥 DESCARGA DATOS HISTÓRICOS Y SNAPSHOT INCREMENTAL
def download_ticker_incremental(ticker, start=DEFAULT_START_DATE, end=DEFAULT_END_DATE, output_dir=DEFAULT_OUTPUT_DIR):
    last_date = get_last_saved_date(ticker, output_dir)
    actual_start = (last_date + timedelta(days=1)).isoformat() if last_date else start
    actual_end = date.today().isoformat()

    df_new = yf.download(ticker, start=actual_start, end=actual_end, auto_adjust=False, progress=False)
    if df_new.empty:
        logging.info(f"🟡 {ticker} ya está actualizado.")
        return None

    df_new.reset_index(inplace=True)
    df_new["Ticker"] = ticker
    path = os.path.join(output_dir, f"{ticker.replace('.', '_')}.parquet")

    if last_date:
        try:
            df_old = pd.read_parquet(path)
            df_combined = pd.concat([df_old, df_new]).drop_duplicates(subset=["Date"]).sort_values("Date")
        except Exception:
            df_combined = df_new
    else:
        df_combined = df_new

    df_combined.to_parquet(path, index=False)

    # Último día para snapshot
    last_row = df_combined.sort_values(by="Date").iloc[-1]
    snapshot = {
        "ticker": ticker,
        "date": pd.to_datetime(last_row["Date"]).strftime("%Y-%m-%d") if not isinstance(last_row["Date"], str) else last_row["Date"],
        "open": last_row["Open"],
        "close": last_row["Close"],
        "high": last_row["High"],
        "low": last_row["Low"],
        "volume": last_row["Volume"],
        "adjusted_close": last_row.get("Adj Close", None)
    }
    return snapshot

# 🚀 DESCARGA MASIVA INCREMENTAL + SNAPSHOT
def bulk_download(ticker_list, start=DEFAULT_START_DATE, end=DEFAULT_END_DATE, output_dir=DEFAULT_OUTPUT_DIR):
    snapshots = []
    for ticker in tqdm(ticker_list, desc="📥 Descargando nuevos datos"):
        snapshot = download_ticker_incremental(ticker, start, end, output_dir)
        if snapshot:
            snapshots.append(snapshot)
        time.sleep(0.3)

    if snapshots:
        df_snapshot = pd.DataFrame(snapshots)
        df_snapshot.sort_values(by="ticker", inplace=True)
        df_snapshot.to_csv(SNAPSHOT_PATH, index=False)
        logging.info(f"📊 Snapshot diario guardado en {SNAPSHOT_PATH}")

# 🎯 MAIN
if __name__ == "__main__":
    logging.info("🧠 Iniciando descarga incremental de históricos + snapshot diario")
    sp500 = get_sp500_tickers()
    ibex35 = get_ibex35_tickers()
    tickers = sorted(set(sp500 + ibex35))
    bulk_download(tickers)
