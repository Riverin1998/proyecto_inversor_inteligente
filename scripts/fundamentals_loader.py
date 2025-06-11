# scripts/fundamentals_loader.py

import os
import json
import pandas as pd
from datetime import datetime

DATA_DIR = "data/fundamentals/"
PRICE_DIR = "data/historical/"

def load_json_data(filepath):
    with open(filepath, 'r') as f:
        return json.load(f)

def load_price_after_filing(ticker, filing_date_str, price_dir=PRICE_DIR, days=7):
    filepath = os.path.join(price_dir, f"{ticker.replace('.', '_')}.parquet")
    if not os.path.exists(filepath) or not filing_date_str:
        return None
    try:
        df = pd.read_parquet(filepath)
        df["Date"] = pd.to_datetime(df["Date"])
        filing_date = pd.to_datetime(filing_date_str)
        window = df[(df["Date"] >= filing_date) & (df["Date"] <= filing_date + pd.Timedelta(days=days))]
        if window.empty:
            return None
        return window["Close"].mean()
    except Exception as e:
        print(f"⚠️ {ticker} error al cargar precio post-filing: {e}")
        return None

def process_fundamentals(filepath):
    data = load_json_data(filepath)
    ticker = os.path.basename(filepath).replace(".json", "")

    income = {pd.to_datetime(x["date"]).year: x for x in data.get("income_statement", [])}
    balance = {pd.to_datetime(x["date"]).year: x for x in data.get("balance_sheet", [])}
    ratios_raw = data.get("ratios", [])
    profile = data.get("profile", [{}])[0]

    if not isinstance(ratios_raw, list):
        print(f"⚠️ {ticker}: 'ratios' no es una lista. Saltando...")
        return pd.DataFrame()

    ratios = {}
    for x in ratios_raw:
        try:
            year = pd.to_datetime(x["date"]).year
            ratios[year] = x
        except Exception as e:
            print(f"⚠️ {ticker}: Error procesando fecha en ratios: {e}")

    rows = []

    for year in sorted(ratios):
        try:
            r = ratios[year]
            i = income.get(year, {})
            b = balance.get(year, {})

            eps = float(i.get("eps", 0))
            equity = float(b.get("totalStockholdersEquity", 0))
            shares = float(i.get("weightedAverageShsOut", 0))
            bvps = equity / shares if equity > 0 and shares > 0 else 0

            pe = float(r.get("priceEarningsRatio", 0))
            pb = float(r.get("priceToBookRatio", 0))
            roe = float(r.get("returnOnEquity", 0))
            de_ratio = float(r.get("debtEquityRatio", 0))
            current_ratio = float(r.get("currentRatio", 0))
            dividend_yield = float(r.get("dividendYield", 0))
            net_margin = float(r.get("netProfitMargin", 0))
            payout_ratio = float(r.get("payoutRatio", 0))
            asset_turnover = float(r.get("assetTurnover", 0))
            price_to_sales = float(r.get("priceSalesRatio", 0))
            free_cash_flow_per_share = float(r.get("freeCashFlowPerShare", 0))
            operating_cash_flow_per_share = float(r.get("operatingCashFlowPerShare", 0))
            graham_number = (22.5 * eps * bvps) ** 0.5 if eps > 0 and bvps > 0 else None

            filing_date = i.get("fillingDate")
            closing_price = load_price_after_filing(ticker, filing_date)
            margen_graham = ((graham_number - closing_price) / closing_price) * 100 if graham_number and closing_price else None
            
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
                "Payout Ratio": payout_ratio,
                "Net Margin": net_margin,
                "Asset Turnover": asset_turnover,
                "Price/Sales": price_to_sales,
                "Free CF/Share": free_cash_flow_per_share,
                "Operating CF/Share": operating_cash_flow_per_share,
                "Graham Number": graham_number,
                "Filing Date": filing_date,
                "Precio Post-Filing": closing_price,
                "Margen Graham (%)": round(margen_graham, 2) if margen_graham else None
            })
        except Exception as e:
            print(f"⚠️ Error en {ticker} año {year}: {e}")

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
