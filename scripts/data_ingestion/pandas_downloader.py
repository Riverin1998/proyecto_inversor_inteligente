# scripts/data_ingestion/pandas_downloader.py

import os
import requests
import yfinance as yf
import logging
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date

logging.basicConfig(level=logging.INFO)

# 📅 Configuración de fechas y rutas
DEFAULT_START_DATE = os.getenv("HISTORICAL_START_DATE", "2015-01-01")
DEFAULT_END_DATE = os.getenv("HISTORICAL_END_DATE", date.today().isoformat())
DEFAULT_OUTPUT_DIR = os.getenv(
    "HISTORICAL_DATA_DIR",
    "/Users/alvaroriverofernandezdelarrea/Desktop/PROYECTOS/proyecto_inversor_inteligente/data/historical"
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

# ↓ Descarga y guarda los datos de un ticker como archivo Parquet
def download_ticker(ticker, start=DEFAULT_START_DATE, end=DEFAULT_END_DATE, output_dir=DEFAULT_OUTPUT_DIR):
    try:
        logging.info(f"⬇️ Descargando: {ticker}")

        df = yf.download(ticker, start=start, end=end, auto_adjust=False, progress=False)

        if df.empty:
            logging.warning(f"⚠️ No hay datos para {ticker}")
            return

        df.reset_index(inplace=True)

        # 💾 Guardar como Parquet
        output_path = os.path.join(output_dir, f"{ticker.replace('.', '_')}.parquet")
        df.to_parquet(output_path, index=False)
        logging.info(f"✅ Guardado {ticker} en {output_path}")

    except Exception as e:
        logging.error(f"❌ Error al procesar {ticker}: {e}", exc_info=True)

# 📦 Procesa muchos tickers en serie
def bulk_download(ticker_list, start=DEFAULT_START_DATE, end=DEFAULT_END_DATE, output_dir=DEFAULT_OUTPUT_DIR):
    for ticker in ticker_list:
        download_ticker(ticker, start, end, output_dir)

# 🚀 Punto de entrada
if __name__ == "__main__":
    tickers = get_sp500_tickers()
    bulk_download(tickers[:500])  # Puedes cambiar el [:5] por [:500] para todo el S&P500
