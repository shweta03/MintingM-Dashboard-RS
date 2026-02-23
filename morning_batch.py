import yfinance as yf
import pandas as pd
import gspread
import json
import os
from datetime import datetime, timedelta

print("Starting Morning Data Fetch for Nifty 750...")

# 1. Read the official Nifty 750 stocks
try:
    df_tickers = pd.read_csv("ind_nifty750list.csv")
    tickers = [str(symbol).strip() + ".NS" for symbol in df_tickers['Symbol'].tolist()]
except Exception as e:
    raise SystemExit(f"CRITICAL ERROR: Could not find ind_nifty750list.csv! Error details: {e}")

# 2. Calculate exactly 18 months ago
end_date = datetime.today()
start_date = end_date - timedelta(days=540)

print("Downloading stock data... This will take 1-2 minutes...")
data = yf.download(tickers, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'), group_by='ticker')

results = []

for ticker in tickers:
    try:
        df_close = data[ticker]['Close'].dropna()
        
        # Require 252 days of history to do the math safely
        if len(df_close) < 252:
            continue
            
        current_price = float(df_close.iloc[-1])
        sma_200 = float(df_close.iloc[-200:].mean())
        
        # Get historical prices (1M=21 days, 3M=63, 6M=126, 9M=189, 12M=252)
        price_1m = float(df_close.iloc[-21])
        price_3m = float(df_close.iloc[-63])
        price_6m = float(df_close.iloc[-126])
        price_9m = float(df_close.iloc[-189])
        price_12m = float(df_close.iloc[-252])
        
        # Calculate Rolling Returns (%)
        ret_1m = ((current_price / price_1m) - 1) * 100
        ret_3m = ((current_price / price_3m) - 1) * 100
        ret_6m = ((current_price / price_6m) - 1) * 100
        ret_9m = ((current_price / price_9m) - 1) * 100
        ret_12m = ((current_price / price_12m) - 1) * 100
        
        # --- THE QUANTITATIVE MATH (METHOD 2) ---
        # Apply the 40/20/20/20 Weighting Formula (Excluding 1M as requested)
        rs_raw = (ret_3m * 0.40) + (ret_6m * 0.20) + (ret_9m * 0.20) + (ret_12m * 0.20)
        
        # Distance from 200 SMA (%) for Trend Rank
        sma_dist = ((current_price / sma_200) - 1) * 100
        
        results.append({
            "Stock Name": ticker.replace('.NS', ''),
            "Price": round(current_price, 2),
            "1M Return (%)": round(ret_1m, 2),
            "3M Return (%)": round(ret_3m, 2),
            "6M Return (%)": round(ret_6m, 2),
            "9M Return (%)": round(ret_9m, 2),
            "12M Return (%)": round(ret_12m, 2),
            "SMA 200": round(sma_200, 2),
            "RS_Raw": rs_raw,
            "SMA_Dist": sma_dist
        })
    except Exception as e:
        pass

# 3. Save to a DataFrame
final_df = pd.DataFrame(results)

# --- NORMALIZE, RANK, AND AVERAGE ---
final_df['RS_Rank'] = final_df['RS_Raw'].rank(pct=True) * 100
final_df['SMA_Rank'] = final_df['SMA_Dist'].rank(pct=True) * 100

# Calculate the final MintingM Score
final_df['MintingM Score'] = ((final_df['RS_Rank'] + final_df['SMA_Rank']) / 2).round(2)

# Sort from highest score to lowest
final_df = final_df.sort_values(by="MintingM Score", ascending=False)

# Slice exactly the Top 20 stocks
final_df = final_df.head(20)

# --- CLEAN UP COLUMNS FOR THE SPREADSHEET ---
# We only keep the exact columns you asked for, in the exact order you asked for them!
columns_to_keep = [
    "Stock Name", "Price", "1M Return (%)", "3M Return (%)", 
    "6M Return (%)", "9M Return (%)", "12M Return (%)", 
    "SMA 200", "MintingM Score"
]
final_sheet_df = final_df[columns_to_keep]

print("Successfully formatted the Top 20 dashboard!")

# 4. Connect to Google Sheets and Push Data
credentials_json = os.environ.get("GOOGLE_SHEETS_CREDENTIALS")
creds_dict = json.loads(credentials_json)
gc = gspread.service_account_from_dict(creds_dict)

# ---> CHANGE THIS TO YOUR EXACT GOOGLE SHEET NAME <---
sheet = gc.open("MintingMRS").sheet1

# Push the clean data to the sheet
data_to_upload = [final_sheet_df.columns.values.tolist()] + final_sheet_df.values.tolist()
sheet.clear()
sheet.update(values=data_to_upload, range_name="A1")

print("SUCCESS! Pushed the beautiful Top 20 dashboard to Google Sheets.")
