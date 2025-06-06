# scripts/fundamentals_downloader.py

import sys
import os
import json
import requests
import logging
from datetime import datetime
from dotenv import load_dotenv
from data_ingestion.pandas_downloader import get_sp500_tickers, get_ibex35_tickers

# 📌 CONFIGURACIÓN GLOBAL
load_dotenv(dotenv_path="config.env")
API_KEY = os.getenv("FINANCIAL_API_KEY")
BASE_URL = "https://financialmodelingprep.com/api/v3"
OUTPUT_DIR = "data/fundamentals"

logging.basicConfig(level=logging.INFO)

if not API_KEY:
    raise ValueError("❌ No se encontró la API key. Añade FINANCIAL_API_KEY en config.env")

# 🔗 CONSTRUCCIÓN DE URL
def build_url(endpoint: str) -> str:
    separator = '&' if '?' in endpoint else '?'
    return f"{BASE_URL}{endpoint}{separator}apikey={API_KEY}"

# 🧼 VALIDACIÓN DE ARCHIVOS
def is_json_empty_or_invalid(path):
    if not os.path.exists(path):
        return True
    try:
        with open(path, "r") as f:
            content = json.load(f)
            return not content or all(not v for v in content.values())
    except Exception:
        return True

def get_last_financial_date(json_path):
    try:
        with open(json_path, "r") as f:
            data = json.load(f)
        income = data.get("income_statement", [])
        if not income:
            return None
        dates = sorted([x.get("date") for x in income if x.get("date")], reverse=True)
        return dates[0] if dates else None
    except Exception as e:
        logging.warning(f"⚠️ Error leyendo {json_path}: {e}")
        return None

def needs_update_since_2015(json_path):
    last_date = get_last_financial_date(json_path)
    if not last_date:
        return True
    last_year = int(last_date.split("-")[0])
    return last_year < datetime.now().year

def clean_invalid_json_files(directory: str):
    logging.info(f"🧹 Limpiando archivos JSON inválidos en: {directory}")
    removed = 0
    os.makedirs(directory, exist_ok=True)
    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            path = os.path.join(directory, filename)
            if is_json_empty_or_invalid(path):
                try:
                    os.remove(path)
                    logging.info(f"🗑️ Eliminado: {filename}")
                    removed += 1
                except Exception as e:
                    logging.warning(f"⚠️ No se pudo eliminar {filename}: {e}")
    logging.info(f"✅ Limpieza completada. Eliminados: {removed}")

# 📥 DESCARGA DE DATOS FUNDAMENTALES
def download_fundamentals(ticker: str, output_dir: str = OUTPUT_DIR) -> bool:
    endpoints = {
        "profile": f"/profile/{ticker}",
        "ratios": f"/ratios/{ticker}",
        "income_statement": f"/income-statement/{ticker}?limit=120",
        "balance_sheet": f"/balance-sheet-statement/{ticker}?limit=120",
    }

    os.makedirs(output_dir, exist_ok=True)
    data = {}

    for name, endpoint in endpoints.items():
        url = build_url(endpoint)
        logging.info(f"📡 {ticker} - {name}")
        response = requests.get(url)

        if response.status_code == 429:
            logging.error("❌ Error 429 (Too Many Requests). Espera o limita tus peticiones.")
            return False
        elif response.status_code == 200:
            data[name] = response.json()
        else:
            logging.warning(f"❌ {ticker} - Error {response.status_code} en {name}")

    output_path = os.path.join(output_dir, f"{ticker.upper()}.json")
    if not data or all(not v for v in data.values()):
        logging.warning(f"⚠️ {ticker} - Datos vacíos. No se guardará.")
        return True

    with open(output_path, "w") as f:
        json.dump(data, f, indent=4)
    logging.info(f"✅ {ticker} - Guardado en: {output_path}")
    return True

# 🚀 DESCARGA MASIVA
def bulk_download_fundamentals(tickers, output_dir=OUTPUT_DIR):
    clean_invalid_json_files(output_dir)
    for i, ticker in enumerate(tickers, 1):
        output_path = os.path.join(output_dir, f"{ticker.upper()}.json")
        if os.path.exists(output_path) and not needs_update_since_2015(output_path):
            logging.info(f"⏭️ {i}/{len(tickers)} - Ya actualizado: {ticker}")
            continue
        logging.info(f"🔄 {i}/{len(tickers)} - Descargando: {ticker}")
        success = download_fundamentals(ticker, output_dir)
        if not success:
            logging.error("⛔ Proceso detenido por error crítico.")
            break

# 🎯 MAIN
if __name__ == "__main__":
    if len(sys.argv) == 2:
        ticker = sys.argv[1].upper()
        download_fundamentals(ticker)
    else:
        logging.info("ℹ️ No se pasó ticker, descargando para el S&P500 + IBEX35...")
        tickers = sorted(set(get_sp500_tickers() + get_ibex35_tickers()))
        bulk_download_fundamentals(tickers)
