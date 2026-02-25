import os
import json
import requests
import gspread
import yfinance as yf
from datetime import datetime

print("Waking up to fetch live prices via Yahoo Finance...")

# --- 1. CONFIGURATION ---
# Your exact Power BI Push URL
POWER_BI_URL = "https://api.powerbi.com/beta/d1f14348-f1b5-4a09-ac99-7ebf213cbc81/datasets/c51db8a4-7f48-4226-88b6-42d137cc1513/rows?experience=power-bi&key=xw5CfBTvK900ZlzwRs1Si1q6UzgjK4Z1rfNT0IciurBhg0mqz6DXC79stXhkmQ%2FBMTyy90OCM%2BVSRpVoVoVLPQ%3D%3D"

# --- 2. PULL THE TOP 20 FROM GOOGLE SHEETS ---
credentials_json = os.environ.get("GOOGLE_SHEETS_CREDENTIALS")
creds_dict = json.loads(credentials_json)
gc = gspread.service_account_from_dict(creds_dict)

# ---> CHANGE THIS TO YOUR EXACT GOOGLE SHEET NAME <---
sheet = gc.open("MintingMRS").sheet1
top_20_stocks = sheet.get_all_records()

# --- 3. FETCH LIVE PRICES & PREPARE PAYLOAD ---
power_bi_payload = []
current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")

for stock in top_20_stocks:
    raw_symbol = stock['Stock Name']
    yf_symbol = raw_symbol + ".NS"
    
    try:
        # Fetch the absolute latest price available on Yahoo Finance
        ticker_data = yf.Ticker(yf_symbol)
        live_price = round(ticker_data.fast_info['last_price'], 2)
        print(f"Fetched {raw_symbol}: ₹{live_price}")
    except Exception as e:
        print(f"Could not fetch {raw_symbol}, using morning CMP. Error: {e}")
        live_price = float(stock['CMP']) 

    # Package the exact columns Power BI is expecting
    row_data = {
        "Stock Name": str(raw_symbol),
        "CMP": float(live_price),
        "RS (1-100)": float(stock.get("RS (1-100)", 0)),
        "MintingM Score": float(stock.get("MintingM Score", 0)),
        "SMA 200": float(stock.get("SMA 200", 0)),
        "1 Day Return (%)": float(stock.get("1 Day Return (%)", 0)),
        "1 Week Return (%)": float(stock.get("1 Week Return (%)", 0)),
        "1 Month Return (%)": float(stock.get("1 Month Return (%)", 0)),
        "3M Return (%)": float(stock.get("3M Return (%)", 0)),
        "6M Return (%)": float(stock.get("6M Return (%)", 0)),
        "9M Return (%)": float(stock.get("9M Return (%)", 0)),
        "12M Return (%)": float(stock.get("12M Return (%)", 0)),
        "Last Updated": current_time
    }
    power_bi_payload.append(row_data)

# --- 4. PUSH TO POWER BI ---
headers = {"Content-Type": "application/json"}
response = requests.post(POWER_BI_URL, data=json.dumps(power_bi_payload), headers=headers)

if response.status_code == 200:
    print("\nSUCCESS! Sent the live data array to the Power BI dashboard.")
else:
    print(f"\nFAILED to send data. Error: {response.status_code} - {response.text}")
