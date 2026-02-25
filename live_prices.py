import os
import json
import requests
import gspread
import yfinance as yf
from datetime import datetime

# --- CONFIGURATION ---
# Use the exact same Power BI URL you've been using
POWER_BI_URL = "https://api.powerbi.com/beta/d1f14348-f1b5-4a09-ac99-7ebf213cbc81/datasets/c51db8a4-7f48-4226-88b6-42d137cc1513/rows?experience=power-bi&key=xw5CfBTvK900ZlzwRs1Si1q6UzgjK4Z1rfNT0IciurBhg0mqz6DXC79stXhkmQ%2FBMTyy90OCM%2BVSRpVoVoVLPQ%3D%3D"

def run_live_update():
    try:
        credentials_json = os.environ.get("GOOGLE_SHEETS_CREDENTIALS")
        creds_dict = json.loads(credentials_json)
        gc = gspread.service_account_from_dict(creds_dict)
        
        # Open your Google Sheet
        sheet = gc.open("MintingMRS").sheet1
        top_20_stocks = sheet.get_all_records()
    except Exception as e:
        print(f"Error: {e}")
        return

    power_bi_payload = []
    # ISO format for Power BI DateTime columns
    current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")

    for stock in top_20_stocks:
        symbol = str(stock['Stock Name'])
        yf_symbol = symbol + ".NS"
        
        try:
            ticker = yf.Ticker(yf_symbol)
            live_price = round(ticker.fast_info['last_price'], 2)
        except:
            live_price = float(stock['CMP'])

        # MATCHING YOUR DATASET CONFIGURATION EXACTLY
        row_data = {
            "Stock Name": symbol,
            "CMP": float(live_price),
            "MintingM Score": float(stock.get("MintingM Score", 0)),
            "1 Day Return (%)": float(stock.get("1 Day Return (%)", 0)),
            "1 Week Return (%)": float(stock.get("1 Week Return (%)", 0)),
            "1 Month Return (%)": float(stock.get("1 Month Return (%)", 0)),
            "3M Return (%)": float(stock.get("3M Return (%)", 0)),
            "6M Return (%)": float(stock.get("6M Return (%)", 0)),
            "9M Return (%)": float(stock.get("9M Return (%)", 0)),
            "12M Return (%)": float(stock.get("12M Return (%)", 0)),
            "Last Updated": current_time  # CORRECTED: Capital 'U'
        }
        power_bi_payload.append(row_data)

    headers = {"Content-Type": "application/json"}
    response = requests.post(POWER_BI_URL, data=json.dumps(power_bi_payload), headers=headers)
    
    if response.status_code == 200:
        print("SUCCESS: Live data updated.")
    else:
        print(f"FAILED: {response.status_code} - {response.text}")

if __name__ == "__main__":
    run_live_update()
