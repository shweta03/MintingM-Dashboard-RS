import os
import json
import requests
import gspread
import yfinance as yf
from datetime import datetime

# --- CONFIGURATION ---
# Your NEW "No-History" Push URL
POWER_BI_URL = "https://api.powerbi.com/beta/d1f14348-f1b5-4a09-ac99-7ebf213cbc81/datasets/39ee90f3-2ca5-48bc-b4e7-57f7933321d0/rows?experience=power-bi&key=J9iGDYJhkuPBjccUu4cCjXNN8FEMRqGph4t5BYkSiu4dccVxHNDycxqx1W3dc5mjMyDVVjmwpAk2v74c7jarlA%3D%3D"

def run_live_update():
    try:
        credentials_json = os.environ.get("GOOGLE_SHEETS_CREDENTIALS")
        creds_dict = json.loads(credentials_json)
        gc = gspread.service_account_from_dict(creds_dict)
        
        # Ensure this matches your actual Google Sheet name
        sheet = gc.open("Your Google Sheet Name Here").sheet1
        top_20_stocks = sheet.get_all_records()
    except Exception as e:
        print(f"Error: {e}")
        return

    power_bi_payload = []
    # Power BI requires ISO format for DateTime
    current_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")

    for stock in top_20_stocks:
        symbol = str(stock['Stock Name'])
        yf_symbol = symbol + ".NS"
        
        try:
            ticker = yf.Ticker(yf_symbol)
            live_price = round(ticker.fast_info['last_price'], 2)
        except:
            live_price = float(stock['CMP'])

        # MATCHING YOUR EXACT REQUESTED STRUCTURE
        row_data = {
            "Stock Name": symbol,
            "CMP": float(live_price),
            "MintingM Score": float(stock.get("MintingM Score", 0)),
            "1 Day Return (%)": float(stock.get("1 Day Return (%)", 0)),
            "1 Week Return (%)": float(stock.get("1 Week Return (%)", 0)),
            "1 M Return (%)": float(stock.get("1 M Return (%)", 0)),
            "3 M Return (%)": float(stock.get("3 M Return (%)", 0)),
            "6 M Return (%)": float(stock.get("6 M Return (%)", 0)),
            "9 M Return (%)": float(stock.get("9 M Return (%)", 0)),
            "12 M Return (%)": float(stock.get("12 M Return (%)", 0)),
            "SMA 200": float(stock.get("SMA 200", 0)),
            "Last updated": current_time
        }
        power_bi_payload.append(row_data)

    headers = {"Content-Type": "application/json"}
    response = requests.post(POWER_BI_URL, data=json.dumps(power_bi_payload), headers=headers)
    
    if response.status_code == 200:
        print("SUCCESS: Live data pushed to your Clean Slate dashboard!")
    else:
        print(f"FAILED: {response.status_code} - {response.text}")

if __name__ == "__main__":
    run_live_update()
