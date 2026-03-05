import yfinance as yf
import pandas as pd
import pandas_ta as ta
import numpy as np
from datetime import datetime, timedelta
import math
import os

print("Starting Morning Master (750 Universe Scan) with your specific Strategy Logic...")

# 1. Load Universe
try:
    df_tickers = pd.read_csv("ind_nifty750list.csv")
    tickers = [str(symbol).strip() + ".NS" for symbol in df_tickers['Symbol'].tolist()]
except FileNotFoundError:
    print("Error: ind_nifty750list.csv not found!")
    exit(1)

# Download 1.5 years of data
start_date = datetime.today() - timedelta(days=400)
data = yf.download(tickers, start=start_date.strftime('%Y-%m-%d'), group_by='ticker', threads=True)

daily_rf_rate = 0.05 / 252
results = []

for ticker in tickers:
    try:
        # Basic data cleaning
        df = data[ticker].dropna(subset=['High', 'Low', 'Close']).copy()
        if len(df) < 252:
            continue
            
        # --- TA Indicators ---
        df['SMA_50'] = ta.sma(df['Close'], length=50)
        df['SMA_200'] = ta.sma(df['Close'], length=200)
        df['EMA_9'] = ta.ema(df['Close'], length=9)
        df['RSI_14'] = ta.rsi(df['Close'], length=14)
        
        try:
            # SuperTrend (15, 2.75)
            st = ta.supertrend(df['High'], df['Low'], df['Close'], length=15, multiplier=2.75)
            st_col = [c for c in st.columns if c.startswith('SUPERT_')][0]
            df['SuperTrend'] = st[st_col]
        except:
            df['SuperTrend'] = np.nan
            
        current_price = float(df['Close'].iloc[-1])
        high_now = float(df['High'].iloc[-1])
        low_now = float(df['Low'].iloc[-1])
        sma_200 = float(df['SMA_200'].iloc[-1])
        sma_50 = float(df['SMA_50'].iloc[-1])
        ema_9 = float(df['EMA_9'].iloc[-1])
        supertrend_val = float(df['SuperTrend'].iloc[-1])
        rsi_14 = float(df['RSI_14'].iloc[-1])

        # --- Performance Returns ---
        def get_ret(days): 
            try:
                return ((current_price / float(df['Close'].iloc[-min(len(df), days)])) - 1) * 100
            except:
                return 0

        ret_1d, ret_1w, ret_1m = get_ret(2), get_ret(6), get_ret(21)
        ret_3m, ret_6m, ret_9m, ret_12m = get_ret(63), get_ret(126), get_ret(189), get_ret(252)
        
        # --- Weighted Sharpe (For Sorting) ---
        df['Daily_Ret'] = df['Close'].pct_change()
        weighted_mean = df['Daily_Ret'].tail(63).ewm(span=63).mean().iloc[-1]
        stable_std = df['Daily_Ret'].tail(252).std()
        sharpe = ((weighted_mean - daily_rf_rate) / stable_std) * 10 if stable_std > 0.005 else 0
            
        # RS and SMA Distance
        rs_raw = (ret_3m * 0.40) + (ret_6m * 0.20) + (ret_9m * 0.20) + (ret_12m * 0.20)
        sma_dist = ((current_price / sma_200) - 1) * 100
        
        # --- YOUR SPECIFIC SIGNAL LOGIC ---
        
        # Circuit Day Check (high == low)
        is_circuit_day = (high_now == low_now)
        
        # Sell Condition: close < 200sma OR Close < Super Trend (15,2.75)
        sell_triggered = (current_price < sma_200) or (current_price < supertrend_val)
        
        # Buy Condition:
        # rsi > 55 AND Return 1d > -5% AND (3m > 20 OR 6m > 30 OR 1m > 10) AND
        # DMA 50 > DMA 200 AND RS > 80 AND Price > 200sma AND Price > Super Trend AND
        # Price is within 5% of EMA_9
        buy_triggered = (
            not is_circuit_day and
            (rsi_14 > 55) and 
            (ret_1d > -5.0) and 
            ((ret_3m > 20.0) or (ret_6m > 30.0) or (ret_1m > 10.0)) and
            (sma_50 > sma_200) and 
            (rs_raw > 80) and 
            (current_price > sma_200) and 
            (current_price > supertrend_val) and
            (abs((current_price / ema_9) - 1) <= 0.05)
        )

        if is_circuit_day:
            signal = "HOLD (CIRCUIT)"
        elif sell_triggered:
            signal = "SELL"
        elif buy_triggered:
            signal = "BUY"
        else:
            signal = "HOLD"

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
            "Sharpe": round(sharpe, 2), 
            "Signal": signal
        })
    except Exception:
        continue

# 2. Process Results and Ranking
if not results:
    exit(1)

df_final = pd.DataFrame(results)

# Sorting by RS_Raw in Descending order as per your logic
df_final['RS (1-100)'] = (df_final['RS_Raw'].rank(pct=True) * 100).round(2)
df_final['SMA_Rank'] = (df_final['SMA_Dist'].rank(pct=True) * 100).round(2)
df_final['MintingM Score'] = ((df_final['RS (1-100)'] + df_final['SMA_Rank']) / 2).round(2)

# Get the Top 20 for the CSV
top_20 = df_final.sort_values(by="RS_Raw", ascending=False).head(20).copy()

# 3. Merge Quarterly Data
qtr_cols = ['Qtr Profit Var %', 'QoQ profits %', 'QoQ sales %', 'OPM']
try:
    yesterday_df = pd.read_csv("live_cmp.csv")
    top_20 = pd.merge(top_20, yesterday_df[['Stock Name'] + qtr_cols], on='Stock Name', how='left')
    top_20[qtr_cols] = top_20[qtr_cols].fillna(0)
except Exception:
    for col in qtr_cols: top_20[col] = 0

top_20['Last updated'] = datetime.now().strftime("%Y-%m-%d %H:%M")
final_cols = ["Stock Name", "CMP", "MintingM Score", "RS (1-100)", "SMA 200", "1 Day Return (%)", 
              "1 Week Return (%)", "1M Return (%)", "3M Return (%)", "6M Return (%)", 
              "9M Return (%)", "12M Return (%)", "Sharpe", "Signal"] + qtr_cols + ["Last updated"]

top_20[final_cols].to_csv("live_cmp.csv", index=False)
print(f"Morning Master Complete. {len(top_20)} stocks saved to live_cmp.csv")

# Final Filter: Pick exactly 6 positions based on your logic
buys_only = df_final[df_final['Signal'] == "BUY"].sort_values(by="RS_Raw", ascending=False).head(6)
if not buys_only.empty:
    print("\n--- YOUR TOP 6 BUY POSITIONS ---")
    print(buys_only[["Stock Name", "CMP", "RS_Raw", "Signal"]])
