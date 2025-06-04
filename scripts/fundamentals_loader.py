# scripts/fundamentals_loader.py

import os
import json
import pandas as pd
from datetime import datetime

DATA_DIR = "data/fundamentals/"

def load_json_data(filepath):
    with open(filepath, 'r') as f:
        return json.load(f)

def process_fundamentals(filepath):
    data = load_json_data(filepath)
    ticker = os.path.basename(filepath).replace(".json", "")

    income = {pd.to_datetime(x["date"]).year: x for x in data.get("income_statement", [])}
    balance = {pd.to_datetime(x["date"]).year: x for x in data.get("balance_sheet", [])}
    ratios = data.get("ratios", [])

    rows = []

    for year_data in ratios:
        try:
            date = year_data.get("date")
            year = pd.to_datetime(date).year

            # Buscar EPS desde income_statement
            eps = float(income.get(year, {}).get("eps", 0))

            # Calcular BVPS desde balance_sheet
            equity = float(balance.get(year, {}).get("totalStockholdersEquity", 0))
            shares = float(income.get(year, {}).get("weightedAverageShsOut", 0))
            bvps = equity / shares if equity > 0 and shares > 0 else 0

            pe = float(year_data.get("priceEarningsRatio", 0))
            pb = float(year_data.get("priceToBookRatio", 0))
            roe = float(year_data.get("returnOnEquity", 0))
            de_ratio = float(year_data.get("debtEquityRatio", 0))
            current_ratio = float(year_data.get("currentRatio", 0))
            dividend_yield = float(year_data.get("dividendYield", 0))
            net_margin = float(year_data.get("netProfitMargin", 0))

            graham_number = (22.5 * eps * bvps) ** 0.5 if eps > 0 and bvps > 0 else None

            rows.append({
                "ticker": ticker,
                "year": year,
                "EPS": eps,
                "BVPS": bvps,
                "P/E": pe,
                "P/B": pb,
                "ROE": roe,
                "Debt/Equity": de_ratio,
                "Current Ratio": current_ratio,
                "Dividend Yield": dividend_yield,
                "Net Margin": net_margin,
                "Graham Number": graham_number,
            })
        except Exception as e:
            print(f"⚠️ Error en {ticker} año {year_data.get('date')}: {e}")

    return pd.DataFrame(rows)


def load_all_fundamentals(data_dir=DATA_DIR):
    all_data = []

    for file in os.listdir(data_dir):
        if file.endswith(".json"):
            filepath = os.path.join(data_dir, file)
            df = process_fundamentals(filepath)
            if not df.empty:
                all_data.append(df)

    combined = pd.concat(all_data, ignore_index=True)
    combined.sort_values(by=["ticker", "year"], inplace=True)

    # Calcular EPS Growth en porcentaje
    combined["EPS Growth (%)"] = combined.groupby("ticker")["EPS"].pct_change() * 100
    combined["EPS Growth (%)"] = combined["EPS Growth (%)"].round(2)

    # (Opcional) Para calcular EPS Growth en valor absoluto, descomenta esta línea:
    # combined["EPS Growth (abs)"] = combined.groupby("ticker")["EPS"].diff().round(2)

    return combined

if __name__ == "__main__":
    df = load_all_fundamentals()
    output_path = "data/processed/fundamentals_evolucion.csv"
    df.to_csv(output_path, index=False)
    print(f"✅ Guardado CSV con evolución fundamental: {output_path}")
