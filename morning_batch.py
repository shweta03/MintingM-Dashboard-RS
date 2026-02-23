import yfinance as yf
import pandas as pd
import gspread
import json
import os
from datetime import datetime, timedelta

print("Starting Morning Data Fetch (18 Months for Nifty 750)...")

# 1. Read the official Nifty 750 stocks from your uploaded CSV
try:
    df_tickers = pd.read_csv("ind_nifty750list.csv")
    # This grabs the 'Symbol' column and adds '.NS'
    tickers = [str(symbol).strip() + ".NS" for symbol in df_tickers['Symbol'].tolist()]
    print(f"Successfully loaded {len(tickers)} stocks from CSV.")
except Exception as e:
    print("Error reading CSV file:", e)
    # We force the script to stop here if it can't find your CSV!
    raise SystemExit("Pipeline stopped: Could not find ind_nifty750list.csv. Did you upload it?")

# 2. Calculate exactly 18 months ago (roughly 540 calendar days)
end_date = datetime.today()
start_date = end_date - timedelta(days=540)

print(f"Fetching data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}...")

# Download the data 
data = yf.download(tickers, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'), group_by='ticker')

results = []

for ticker in tickers:
    try:
        # Extract just the 'Close' prices for this specific stock
        df_close = data[ticker]['Close'].dropna()
        
        # We need at least 252 days of history to do the math. If it's a new IPO, skip it safely.
        if len(df_close) < 252:
            continue
            
        current_price = float(df_close.iloc[-1])
        sma_200 = float(df_close.iloc[-200:].mean())
        
        # Get the historical anchor prices (63, 126, 189, 252 trading days ago)
        price_3m = float(df_close.iloc[-63])
        price_6m = float(df_close.iloc[-126])
        price_9m = float(df_close.iloc[-189])
        price_12m = float(df_close.iloc[-252])
        
        results.append({
            "Ticker": ticker.replace('.NS', ''), # Removes the .NS so your sheet looks clean
            "Last_Close": current_price,
            "Anchor_3M": price_3m,
            "Anchor_6M": price_6m,
            "Anchor_9M": price_9m,
            "Anchor_12M": price_12m,
            "SMA_200": sma_200
        })
    except Exception as e:
        pass

# 3. Save it to a DataFrame
final_df = pd.DataFrame(results)
print(f"\nSuccessfully calculated RS Anchors for {len(final_df)} stocks!")

# 4. Connect to Google Sheets and Push Data
print("Connecting to Google Sheets...")
credentials_json = os.environ.get("GOOGLE_SHEETS_CREDENTIALS")
creds_dict = json.loads(credentials_json)

gc = gspread.service_account_from_dict(creds_dict)

# ---> CHANGE THIS TO YOUR EXACT GOOGLE SHEET NAME <---
sheet = gc.open("MintingMRS").sheet1

# Format the data and push it
data_to_upload = [final_df.columns.values.tolist()] + final_df.values.tolist()
sheet.clear()
sheet.update(values=data_to_upload, range_name="A1")

print("Successfully pushed the massive 750-stock list to Google Sheets!")
