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
        
        price_3m = float(df_close.iloc[-63])
        price_6m = float(df_close.iloc[-126])
        price_9m = float(df_close.iloc[-189])
        price_12m = float(df_close.iloc[-252])
        
        # --- THE QUANTITATIVE MATH (METHOD 2) ---
        # Step A: Calculate Rolling Returns
        ret_3m = ((current_price / price_3m) - 1) * 100
        ret_6m = ((current_price / price_6m) - 1) * 100
        ret_9m = ((current_price / price_9m) - 1) * 100
        ret_12m = ((current_price / price_12m) - 1) * 100
        
        # Step B: Apply the 40/20/20/20 Weighting Formula
        rs_raw = (ret_3m * 0.40) + (ret_6m * 0.20) + (ret_9m * 0.20) + (ret_12m * 0.20)
        
        # Step C: Distance from 200 SMA (%)
        sma_dist = ((current_price / sma_200) - 1) * 100
        
        results.append({
            "Ticker": ticker.replace('.NS', ''),
            "Last_Close": round(current_price, 2),
            "SMA_200": round(sma_200, 2),
            "SMA_Dist_%": round(sma_dist, 2),
            "RS_Raw": round(rs_raw, 2)
        })
    except Exception as e:
        pass

# 3. Save to a DataFrame
final_df = pd.DataFrame(results)

# --- NORMALIZE, RANK, AND AVERAGE ---
# Grade the RS Formula on a curve from 0 to 100
final_df['RS_Rank'] = final_df['RS_Raw'].rank(pct=True) * 100

# Grade the SMA Distance on a curve from 0 to 100
final_df['SMA_Rank'] = final_df['SMA_Dist_%'].rank(pct=True) * 100

# Calculate the final composite score
final_df['MintingM score'] = (final_df['RS_Rank'] + final_df['SMA_Rank']) / 2

# Clean up the decimals for a crisp, professional sheet
final_df['RS_Rank'] = final_df['RS_Rank'].round(2)
final_df['SMA_Rank'] = final_df['SMA_Rank'].round(2)
final_df['MintingM score'] = final_df['MintingM score'].round(2)

# --- THE MAGIC FILTER ---
# Sort by the final score (Highest to lowest)
final_df = final_df.sort_values(by="MintingM score", ascending=False)

# Slice the list down to exactly the Top 20 stocks!
final_df = final_df.head(20)

print(f"Successfully filtered down to the Top {len(final_df)} outperforming stocks!")

# 4. Connect to Google Sheets and Push Data
credentials_json = os.environ.get("GOOGLE_SHEETS_CREDENTIALS")
creds_dict = json.loads(credentials_json)
gc = gspread.service_account_from_dict(creds_dict)

# ---> CHANGE THIS TO YOUR EXACT GOOGLE SHEET NAME <---
sheet = gc.open("Your Google Sheet Name Here").sheet1

# Push the exact Top 20 to the sheet
data_to_upload = [final_df.columns.values.tolist()] + final_df.values.tolist()
sheet.clear()
sheet.update(values=data_to_upload, range_name="A1")

print("SUCCESS! Pushed the Top 20 MintingM scores to Google Sheets.")
