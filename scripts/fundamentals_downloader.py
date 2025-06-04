# scripts/fundamentals_downloader.py

import sys
import os
import json
import requests
from datetime import datetime
from dotenv import load_dotenv
from data_ingestion.pandas_downloader import get_sp500_tickers

load_dotenv(dotenv_path="config.env")

API_KEY = os.getenv("FINANCIAL_API_KEY")
BASE_URL = "https://financialmodelingprep.com/api/v3"

if not API_KEY:
    raise ValueError("❌ No se encontró la API key en las variables de entorno. Añade FINANCIAL_API_KEY en config.env")

def build_url(endpoint: str) -> str:
    separator = '&' if '?' in endpoint else '?'
    return f"{BASE_URL}{endpoint}{separator}apikey={API_KEY}"

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

        dates = [x.get("date") for x in income if x.get("date")]
        dates = sorted(dates, reverse=True)
        return dates[0] if dates else None
    except Exception as e:
        print(f"⚠️ Error leyendo fecha de {json_path}: {e}")
        return None

def needs_update_since_2015(json_path):
    last_date = get_last_financial_date(json_path)
    if not last_date:
        return True

    last_year = int(last_date.split("-")[0])
    current_year = datetime.now().year
    return last_year < current_year

def clean_invalid_json_files(directory: str):
    print(f"🧹 Comprobando archivos JSON inválidos en: {directory}")
    removed = 0

    if not os.path.exists(directory):
        print("⚠️ Carpeta no encontrada. Saltando limpieza.")
        return

    for filename in os.listdir(directory):
        if filename.endswith(".json"):
            path = os.path.join(directory, filename)
            if is_json_empty_or_invalid(path):
                try:
                    os.remove(path)
                    print(f"🗑️ Eliminado archivo inválido: {filename}")
                    removed += 1
                except Exception as e:
                    print(f"⚠️ No se pudo eliminar {filename}: {e}")
    print(f"✅ Limpieza completada. Archivos eliminados: {removed}")

def download_fundamentals(ticker: str, output_dir: str = "data/fundamentals/") -> bool:
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
        print(f"📡 Descargando {name} para {ticker} desde {url}")
        response = requests.get(url)

        if response.status_code == 429:
            print("❌ Error 429 (Too Many Requests). Deteniendo proceso para evitar bloqueo.")
            return False

        if response.status_code == 200:
            data[name] = response.json()
        else:
            print(f"❌ Error {response.status_code} descargando {name} para {ticker}")

    output_path = os.path.join(output_dir, f"{ticker.upper()}.json")

    if not data or all(not v for v in data.values()):
        print(f"⚠️ Datos vacíos para {ticker}. No se guardará el archivo.")
        return True

    with open(output_path, "w") as f:
        json.dump(data, f, indent=4)
    print(f"✅ Datos guardados en: {output_path}")
    return True

def bulk_download_fundamentals(tickers, output_dir="data/fundamentals/"):
    clean_invalid_json_files(output_dir)

    for i, ticker in enumerate(tickers, 1):
        output_path = os.path.join(output_dir, f"{ticker.upper()}.json")

        if os.path.exists(output_path) and not needs_update_since_2015(output_path):
            print(f"⏭️ Ya actualizado: {ticker}")
            continue

        print(f"\n🔄 Actualizando {i}/{len(tickers)}: {ticker}")
        success = download_fundamentals(ticker, output_dir)
        if not success:
            print("⛔ Proceso detenido por error crítico.")
            break

if __name__ == "__main__":
    if len(sys.argv) == 2:
        ticker = sys.argv[1].upper()
        download_fundamentals(ticker)
    else:
        print("ℹ️ No se pasó ticker, descargando para el S&P 500 completo...")
        tickers = get_sp500_tickers()
        bulk_download_fundamentals(tickers[:500])





