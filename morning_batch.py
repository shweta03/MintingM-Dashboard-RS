import yfinance as yf
import pandas as pd
import gspread
import json
import os

print("Starting Morning Data Fetch (2 Years)...")

# 1. Your list of stocks (Using 5 placeholders to test before expanding to all 500 Nifty stocks)
tickers = ["HINDCOPPER.NS", "RELIANCE.NS", "TCS.NS", "INFY.NS", "HDFCBANK.NS"]

# 2. Download exactly 2 years of history
data = yf.download(tickers, period="2y")

results = []

for ticker in tickers:
    try:
        # Extract just the 'Close' prices for this specific stock and drop missing/holiday rows
        df_close = data['Close'][ticker].dropna()
        
        current_price = float(df_close.iloc[-1])
        
        # Calculate exactly 200 trading days for SMA
        sma_200 = float(df_close.iloc[-200:].mean())
        
        # Get the historical anchor prices (63, 126, 189, 252 trading days ago)
        price_3m = float(df_close.iloc[-63])
        price_6m = float(df_close.iloc[-126])
        price_9m = float(df_close.iloc[-189])
        price_12m = float(df_close.iloc[-252])
        
        results.append({
            "Ticker": ticker,
            "Last_Close": current_price,
            "Anchor_3M": price_3m,
            "Anchor_6M": price_6m,
            "Anchor_9M": price_9m,
            "Anchor_12M": price_12m,
            "SMA_200": sma_200
        })
        print(f"Successfully calculated anchors for {ticker}")
    except Exception as e:
        print(f"Not enough data for {ticker}. Skipping. Error: {e}")

# 3. Save it to a DataFrame
final_df = pd.DataFrame(results)
print("\nFinal Math Calculation:")
print(final_df)

# 4. Connect to Google Sheets and Push Data
print("\nConnecting to Google Sheets...")
credentials_json = os.environ.get("GOOGLE_SHEETS_CREDENTIALS")
creds_dict = json.loads(credentials_json)

gc = gspread.service_account_from_dict(creds_dict)

# ---> CHANGE THIS TO YOUR EXACT GOOGLE SHEET NAME <---
sheet = gc.open("MintingMRS").sheet1

# Format the data and push it
data_to_upload = [final_df.columns.values.tolist()] + final_df.values.tolist()
sheet.clear()
sheet.update(values=data_to_upload, range_name="A1")

print("Successfully pushed Morning RS Anchors to Google Sheets!")
