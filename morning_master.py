import yfinance as yf
import pandas as pd
import pandas_ta as ta
import numpy as np
from datetime import datetime, timedelta
import math
import os

print("Starting Morning Master (750 Universe Scan) with Logic Verification...")

# 1. Load Universe
try:
    df_tickers = pd.read_csv("ind_nifty750list.csv")
    tickers = [str(symbol).strip() + ".NS" for symbol in df_tickers['Symbol'].tolist()]
except FileNotFoundError:
    print("Error: ind_nifty750list.csv not found!")
    exit(1)

# Download data
start_date = datetime.today() - timedelta(days=400)
data = yf.download(tickers, start=start_date.strftime('%Y-%m-%d'), group_by='ticker', threads=True)

daily_rf_rate = 0.05 / 252
results = []

for ticker in tickers:
    try:
        df = data[ticker].dropna(subset=['High', 'Low', 'Close']).copy()
        if len(df) < 252:
            continue
            
        # --- Indicators ---
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
        
        # --- Weighted Sharpe ---
        df['Daily_Ret'] = df['Close'].pct_change()
        weighted_mean = df['Daily_Ret'].tail(63).ewm(span=63).mean().iloc[-1]
        stable_std = df['Daily_Ret'].tail(252).std()
        sharpe = ((weighted_mean - daily_rf_rate) / stable_std) * 10 if stable_std > 0.005 else 0
            
        rs_raw = (ret_3m * 0.40) + (ret_6m * 0.20) + (ret_9m * 0.20) + (ret_12m * 0.20)
        sma_dist = ((current_price / sma_200) - 1) * 100
        
        # --- YOUR SIGNAL LOGIC ---
        is_circuit_day = (high_now == low_now)
        
        # Sell Logic: Price < 200 SMA OR Price < SuperTrend
        sell_triggered = (current_price < sma_200) or (current_price < supertrend_val)
        
        # Buy Logic
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

        # DEBUG PRINT FOR MCX
        if "MCX" in ticker:
            print(f"\n--- MCX DEBUG DATA ---")
            print(f"Price: {current_price}, SMA200: {sma_200}, SuperTrend: {supertrend_val}")
            print(f"Condition Check: Price < SMA200 is {current_price < sma_200}")
            print(f"Condition Check: Price < SuperTrend is {current_price < supertrend_val}")
            print(f"Final Signal: {signal}\n")

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

# 2. Process Ranking
df_final = pd.DataFrame(results)
df_final['RS (1-100)'] = (df_final['RS_Raw'].rank(pct=True) * 100).round(2)
df_final['SMA_Rank'] = (df_final['SMA_Dist'].rank(pct=True) * 100).round(2)
df_final['MintingM Score'] = ((df_final['RS (1-100)'] + df_final['SMA_Rank']) / 2).round(2)

top_20 = df_final.sort_values(by="RS_Raw", ascending=False).head(20).copy()

# 3. Final CSV Saving
top_20['Last updated'] = datetime.now().strftime("%Y-%m-%d %H:%M")
top_20.to_csv("live_cmp.csv", index=False)
print(f"Scan Complete. Data saved to live_cmp.csv")
