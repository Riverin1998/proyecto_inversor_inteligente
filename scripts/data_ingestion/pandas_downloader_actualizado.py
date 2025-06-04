
import os
import requests
import yfinance as yf
import logging
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date
from tqdm import tqdm
import time

logging.basicConfig(level=logging.INFO)

# 📅 Configuración de fechas y rutas
DEFAULT_START_DATE = os.getenv("HISTORICAL_START_DATE", "2015-01-01")
DEFAULT_END_DATE = os.getenv("HISTORICAL_END_DATE", date.today().isoformat())
DEFAULT_OUTPUT_DIR = os.getenv(
    "HISTORICAL_DATA_DIR",
    "data/historical"
)
os.makedirs(DEFAULT_OUTPUT_DIR, exist_ok=True)

# 🔎 Obtiene tickers del S&P 500 desde Wikipedia
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

# 🔎 Obtiene tickers del IBEX 35 desde Wikipedia
def get_ibex35_tickers():
    url = "https://es.wikipedia.org/wiki/IBEX_35"
    html = requests.get(url).text
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", {"class": "wikitable"})
    tickers = []
    for row in table.find_all("tr")[1:]:
        try:
            name = row.find_all("td")[1].text.strip()
            yahoo_suffix = ".MC"
            ticker = name.split()[0].replace(",", "") + yahoo_suffix
            tickers.append(ticker)
        except Exception:
            continue
    return tickers

# ↓ Descarga y guarda los datos de un ticker como archivo Parquet
def download_ticker(ticker, start=DEFAULT_START_DATE, end=DEFAULT_END_DATE, output_dir=DEFAULT_OUTPUT_DIR):
    try:
        df = yf.download(ticker, start=start, end=end, auto_adjust=False, progress=False)
        if df.empty:
            logging.warning(f"⚠️ No hay datos para {ticker}")
            return
        df.reset_index(inplace=True)
        output_path = os.path.join(output_dir, f"{ticker.replace('.', '_')}.parquet")
        df.to_parquet(output_path, index=False)
        logging.info(f"✅ Guardado {ticker}")
    except Exception as e:
        logging.error(f"❌ Error al procesar {ticker}: {e}", exc_info=True)

# 📦 Descarga masiva con barra de progreso
def bulk_download(ticker_list, start=DEFAULT_START_DATE, end=DEFAULT_END_DATE, output_dir=DEFAULT_OUTPUT_DIR):
    for ticker in tqdm(ticker_list, desc="📥 Descargando tickers"):
        download_ticker(ticker, start, end, output_dir)
        time.sleep(0.3)  # Evitar bloqueos por exceso de peticiones

# 🚀 Punto de entrada
if __name__ == "__main__":
    logging.info("📊 Iniciando descarga automática de históricos del S&P 500 + IBEX 35")
    sp500 = get_sp500_tickers()
    ibex35 = get_ibex35_tickers()
    tickers = list(set(sp500 + ibex35))
    bulk_download(tickers)
