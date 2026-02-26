import yfinance as yf
import pandas as pd
import gspread
import json
import os
import numpy as np
from datetime import datetime, timedelta

# --- 1. CONFIGURATION & SETUP ---
try:
    df_tickers = pd.read_csv("ind_nifty750list.csv")
    tickers = [str(symbol).strip() + ".NS" for symbol in df_tickers['Symbol'].tolist()]
except Exception as e:
    raise SystemExit(f"CRITICAL ERROR: {e}")

# FORCED FOR NOW: Set to True so you can see all columns update immediately.
# REVERT TO: datetime.today().weekday() == 4 after this run.
is_friday = True 

def safe_div(n, d):
    """Prevents Infinity and NaN errors for JSON compliance."""
    if d == 0 or pd.isna(n) or pd.isna(d):
        return 0
    res = n / d
    if np.isinf(res) or np.isnan(res):
        return 0
    return res

print(f"Starting Morning Data Fetch. Force Financial Fetch: {is_friday}")

# Download ~12 months of data (370 days covers 252 trading days + buffer)
end_date = datetime.today()
start_date = end_date - timedelta(days=370)
data = yf.download(tickers, start=start_date.strftime('%Y-%m-%d'), group_by='ticker')

results = []

for ticker in tickers:
    try:
        df_close = data[ticker]['Close'].dropna()
        
        # SAFETY: Lowered threshold to 240 to account for holiday gaps
        if len(df_close) < 240: 
            continue
            
        current_price = float(df_close.iloc[-1])
        sma_200 = float(df_close.iloc[-200:].mean())
        
        # Dynamic Indexing to prevent "Index Out of Bounds" errors
        # Uses the closest available day if the target index is missing
        def get_prev_price(df, days_back):
            idx = -min(len(df), days_back)
            return float(df.iloc[idx])

        p_1d = get_prev_price(df_close, 2)
        p_1w = get_prev_price(df_close, 6)
        p_1m = get_prev_price(df_close, 21)
        p_3m = get_prev_price(df_close, 63)
        p_6m = get_prev_price(df_close, 126)
        p_9m = get_prev_price(df_close, 189)
        p_12m = get_prev_price(df_close, 252)
        
        # Daily Return Calculations
        ret_1d = ((current_price / p_1d) - 1) * 100
        ret_1w = ((current_price / p_1w) - 1) * 100
        ret_1m = ((current_price / p_1m) - 1) * 100
        ret_3m = ((current_price / p_3m) - 1) * 100
        ret_6m = ((current_price / p_6m) - 1) * 100
        ret_9m = ((current_price / p_9m) - 1) * 100
        ret_12m = ((current_price / p_12m) - 1) * 100
        
        # RS Formula: 40%(3M) + 20%(6M) + 20%(9M) + 20%(12M)
        rs_raw = (ret_3m * 0.40) + (ret_6m * 0.20) + (ret_9m * 0.20) + (ret_12m * 0.20)
        sma_dist = ((current_price / sma_200) - 1) * 100

        # --- FINANCIAL FETCH ---
        financial_data = {"Qtr Profit Var %": 0, "QoQ profits %": 0, "QoQ sales %": 0, "OPM": 0}
        
        if is_friday:
            try:
                stock_obj = yf.Ticker(ticker)
                q_fin = stock_obj.quarterly_financials
                if not q_fin.empty and q_fin.shape[1] >= 2:
                    financial_data["OPM"] = round(safe_div(q_fin.loc['Operating Income'].iloc[0], q_fin.loc['Total Revenue'].iloc[0]) * 100, 2)
                    financial_data["QoQ profits %"] = round((safe_div(q_fin.loc['Net Income'].iloc[0], q_fin.loc['Net Income'].iloc[1]) - 1) * 100, 2)
                    financial_data["QoQ sales %"] = round((safe_div(q_fin.loc['Total Revenue'].iloc[0], q_fin.loc['Total Revenue'].iloc[1]) - 1) * 100, 2)
                    financial_data["Qtr Profit Var %"] = financial_data["QoQ profits %"]
            except: pass

        results.append({
            "Stock Name": ticker.replace('.NS', ''),
            "CMP": round(current_price, 2),
            "SMA 200": round(sma_200, 2),
            "1 Day Return (%)": round(ret_1d, 2),
            "1 Week Return (%)": round(ret_1w, 2),
            "1M Return (%)": round(ret_1m, 2), 
            "3M Return (%)": round(ret_3m, 2),
            "6M Return (%)": round(ret_6m, 2),
            "9M Return (%)": round(ret_9m, 2),
            "12M Return (%)": round(ret_12m, 2),
            "RS_Raw": rs_raw,
            "SMA_Dist": sma_dist,
            **financial_data
        })
    except Exception as e:
        print(f"Skipping {ticker} due to error: {e}")

# --- 2. FINAL PROCESSING & UPLOAD ---
if not results:
    print("CRITICAL: No stock results found. Check YFinance download status.")
else:
    final_df = pd.DataFrame(results)
    
    # Ranks & MintingM Score
    final_df['RS (1-100)'] = (final_df['RS_Raw'].rank(pct=True) * 100).round(2)
    final_df['SMA_Rank'] = (final_df['SMA_Dist'].rank(pct=True) * 100).round(2)
    final_df['MintingM Score'] = ((final_df['RS (1-100)'] + final_df['SMA_Rank']) / 2).round(2)
    
    # Sort and Keep Top 20
    final_df = final_df.sort_values(by="MintingM Score", ascending=False).head(20)

    # Google Sheets Connection
    gc = gspread.service_account_from_dict(json.loads(os.environ.get("GOOGLE_SHEETS_CREDENTIALS")))
    sheet = gc.open("MintingMRS").sheet1

    # Mon-Thu: Carry forward previous weekly data
    if not is_friday:
        try:
            existing = pd.DataFrame(sheet.get_all_records())
            if not existing.empty:
                for col in ["Qtr Profit Var %", "QoQ profits %", "QoQ sales %", "OPM"]:
                    final_df[col] = final_df['Stock Name'].map(existing.set_index('Stock Name')[col]).fillna(0)
        except: pass

    # Column Order for Dashboard
    cols = ["Stock Name", "CMP", "MintingM Score", "RS (1-100)", "SMA 200", 
            "1 Day Return (%)", "1 Week Return (%)", "1M Return (%)", 
            "3M Return (%)", "6M Return (%)", "9M Return (%)", "12M Return (%)",
            "Qtr Profit Var %", "QoQ profits %", "QoQ sales %", "OPM"]

    data_to_upload = [final_df[cols].columns.values.tolist()] + final_df[cols].values.tolist()
    sheet.clear()
    sheet.update(values=data_to_upload, range_name="A1")
    print("SUCCESS: Forced refresh of all 16 columns completed.")
