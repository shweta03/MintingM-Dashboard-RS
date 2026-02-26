import yfinance as yf
import pandas as pd
import gspread
import json
import os
from datetime import datetime, timedelta

# --- 1. SETUP ---
try:
    df_tickers = pd.read_csv("ind_nifty750list.csv")
    tickers = [str(symbol).strip() + ".NS" for symbol in df_tickers['Symbol'].tolist()]
except Exception as e:
    raise SystemExit(f"CRITICAL ERROR: {e}")

is_friday = True # Temporary: Forces the script to fetch QoQ/OPM data today
# We download 540 days of data to ensure we have enough for 12M returns and SMA 200
data = yf.download(tickers, start=(datetime.today() - timedelta(days=540)).strftime('%Y-%m-%d'), group_by='ticker')

results = []

for ticker in tickers:
    try:
        df_close = data[ticker]['Close'].dropna()
        if len(df_close) < 252: continue
            
        current_price = float(df_close.iloc[-1])
        sma_200 = float(df_close.iloc[-200:].mean())
        
        # --- DAILY RETURNS CALCULATION ---
        # These will be updated every morning (Mon-Fri)
        ret_1d = ((current_price / float(df_close.iloc[-2])) - 1) * 100
        ret_1w = ((current_price / float(df_close.iloc[-6])) - 1) * 100
        ret_1m = ((current_price / float(df_close.iloc[-21])) - 1) * 100
        ret_3m = ((current_price / float(df_close.iloc[-63])) - 1) * 100
        ret_6m = ((current_price / float(df_close.iloc[-126])) - 1) * 100
        ret_9m = ((current_price / float(df_close.iloc[-189])) - 1) * 100
        ret_12m = ((current_price / float(df_close.iloc[-252])) - 1) * 100
        
        # RS Formula: 40%(3M) + 20%(6M) + 20%(9M) + 20%(12M)
        rs_raw = (ret_3m * 0.40) + (ret_6m * 0.20) + (ret_9m * 0.20) + (ret_12m * 0.20)
        sma_dist = ((current_price / sma_200) - 1) * 100

        # --- WEEKLY FINANCIALS (FRIDAY ONLY) ---
        financial_data = {"Qtr Profit Var %": 0, "QoQ profits %": 0, "QoQ sales %": 0, "OPM": 0}
        if is_friday:
            try:
                stock_obj = yf.Ticker(ticker)
                q_fin = stock_obj.quarterly_financials
                if not q_fin.empty:
                    financial_data["OPM"] = round((q_fin.loc['Operating Income'].iloc[0] / q_fin.loc['Total Revenue'].iloc[0]) * 100, 2)
                    financial_data["QoQ profits %"] = round(((q_fin.loc['Net Income'].iloc[0] / q_fin.loc['Net Income'].iloc[1]) - 1) * 100, 2)
                    financial_data["QoQ sales %"] = round(((q_fin.loc['Total Revenue'].iloc[0] / q_fin.loc['Total Revenue'].iloc[1]) - 1) * 100, 2)
            except: pass

        results.append({
            "Stock Name": ticker.replace('.NS', ''),
            "CMP": round(current_price, 2),
            "SMA 200": round(sma_200, 2),
            "1 Day Return (%)": round(ret_1d, 2),
            "1 Week Return (%)": round(ret_1w, 2),
            "1 Month Return (%)": round(ret_1m, 2),
            "3M Return (%)": round(ret_3m, 2),
            "6M Return (%)": round(ret_6m, 2),
            "9M Return (%)": round(ret_9m, 2),
            "12M Return (%)": round(ret_12m, 2),
            "RS_Raw": rs_raw,
            "SMA_Dist": sma_dist,
            **financial_data
        })
    except: pass

# --- 2. RANKING & DATA PRESERVATION ---
final_df = pd.DataFrame(results)
final_df['RS (1-100)'] = (final_df['RS_Raw'].rank(pct=True) * 100).round(2)
final_df['SMA_Rank'] = (final_df['SMA_Dist'].rank(pct=True) * 100).round(2)
final_df['MintingM Score'] = ((final_df['RS (1-100)'] + final_df['SMA_Rank']) / 2).round(2)
final_df = final_df.sort_values(by="MintingM Score", ascending=False).head(20)

# Connect to Google Sheets
gc = gspread.service_account_from_dict(json.loads(os.environ.get("GOOGLE_SHEETS_CREDENTIALS")))
sheet = gc.open("MintingMRS").sheet1

# Mon-Thu: Preserve the OPM/QoQ columns from the previous Friday's run
if not is_friday:
    try:
        existing = pd.DataFrame(sheet.get_all_records())
        if not existing.empty:
            for col in ["Qtr Profit Var %", "QoQ profits %", "QoQ sales %", "OPM"]:
                final_df[col] = final_df['Stock Name'].map(existing.set_index('Stock Name')[col]).fillna(0)
    except: pass

# --- 3. FINAL UPLOAD (EXACT COLUMN ORDER) ---
cols = ["Stock Name", "CMP", "MintingM Score", "RS (1-100)", "SMA 200", 
        "1 Day Return (%)", "1 Week Return (%)", "1 Month Return (%)", 
        "3M Return (%)", "6M Return (%)", "9M Return (%)", "12M Return (%)",
        "Qtr Profit Var %", "QoQ profits %", "QoQ sales %", "OPM"]

data_to_upload = [final_df[cols].columns.values.tolist()] + final_df[cols].values.tolist()
sheet.clear()
sheet.update(values=data_to_upload, range_name="A1")
