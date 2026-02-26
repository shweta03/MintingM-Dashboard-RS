import os
import json
import requests
import gspread
import yfinance as yf
from datetime import datetime

# --- CONFIGURATION ---
POWER_BI_URL = "https://api.powerbi.com/beta/d1f14348-f1b5-4a09-ac99-7ebf213cbc81/datasets/4fb24510-2bf7-4d8a-95a7-9b2ff139a217/rows?experience=power-bi&key=s1ZY%2FrevbSplGQmqM5Cit18Er9lYpdVrQJzk0kYXuiLwQn4RBL4KNs%2FqnSFrnnqAOy2uvAfMChiBwyu6DO8i%2FQ%3D%3D"
def run_live_update():
    try:
        credentials_json = os.environ.get("GOOGLE_SHEETS_CREDENTIALS")
        creds_dict = json.loads(credentials_json)
        gc = gspread.service_account_from_dict(creds_dict)
        
        sheet = gc.open("MintingMRS").sheet1
        top_20_stocks = sheet.get_all_records()
    except Exception as e:
        print(f"Sheet Error: {e}")
        return

    power_bi_payload = []
    current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")

    for stock in top_20_stocks:
        symbol = str(stock['Stock Name'])
        yf_symbol = symbol + ".NS"
        
        try:
            ticker = yf.Ticker(yf_symbol)
            # Fetching fast_info for speed during live hours
            live_price = round(ticker.fast_info['last_price'], 2)
        except:
            live_price = float(stock.get('CMP', 0))

        # Pushing the full 16-column set to maintain dashboard integrity
        row_data = {
            "Stock Name": symbol,
            "CMP": float(live_price),
            "MintingM Score": float(stock.get("MintingM Score", 0)),
            "RS (1-100)": float(stock.get("RS (1-100)", 0)),
            "SMA 200": float(stock.get("SMA 200", 0)),
            "1 Day Return (%)": float(stock.get("1 Day Return (%)", 0)),
            "1 Week Return (%)": float(stock.get("1 Week Return (%)", 0)),
            "1M Return (%)": float(stock.get("1M Return (%)", 0)),
            "3M Return (%)": float(stock.get("3M Return (%)", 0)),
            "6M Return (%)": float(stock.get("6M Return (%)", 0)),
            "9M Return (%)": float(stock.get("9M Return (%)", 0)),
            "12M Return (%)": float(stock.get("12M Return (%)", 0)),
            "Qtr Profit Var %": float(stock.get("Qtr Profit Var %", 0)),
            "QoQ profits %": float(stock.get("QoQ profits %", 0)),
            "QoQ sales %": float(stock.get("QoQ sales %", 0)),
            "OPM": float(stock.get("OPM", 0)),
            "Last updated": current_time
        }
        power_bi_payload.append(row_data)

    headers = {"Content-Type": "application/json"}
    response = requests.post(POWER_BI_URL, data=json.dumps(power_bi_payload), headers=headers)
    
    if response.status_code == 200:
        print(f"SUCCESS: Pushed {len(power_bi_payload)} rows at {current_time}")
    else:
        print(f"FAILED: {response.status_code} - {response.text}")

if __name__ == "__main__":
    run_live_update()
