import yfinance as yf
import pandas as pd
import pandas_ta as ta
import numpy as np
from datetime import datetime, timedelta
import math
import os

print("Starting Morning Master (750 Universe Scan)...")

# 1. Load Universe
df_tickers = pd.read_csv("ind_nifty750list.csv") # Make sure this file exists in your repo
tickers = [str(symbol).strip() + ".NS" for symbol in df_tickers['Symbol'].tolist()]

# Download 1.5 years of data (for 252 days + 200 SMA)
start_date = datetime.today() - timedelta(days=400)
data = yf.download(tickers, start=start_date.strftime('%Y-%m-%d'), group_by='ticker', threads=True)

daily_rf_rate = 0.05 / 252
results = []

for ticker in tickers:
    try:
        df = data[ticker].dropna(subset=['High', 'Low', 'Close']).copy()
        if len(df) < 200: continue
            
        # --- TA Indicators ---
        df['SMA_50'] = ta.sma(df['Close'], length=50)
        df['SMA_200'] = ta.sma(df['Close'], length=200)
        df['EMA_9'] = ta.ema(df['Close'], length=9)
        df['RSI_14'] = ta.rsi(df['Close'], length=14)
        
        try:
            st = ta.supertrend(df['High'], df['Low'], df['Close'], length=15, multiplier=2.75)
            st_col = [c for c in st.columns if c.startswith('SUPERT_')][0]
            df['SuperTrend'] = st[st_col]
        except:
            df['SuperTrend'] = np.nan
            
        current_price = float(df['Close'].iloc[-1])
        sma_200 = float(df['SMA_200'].iloc[-1])
        sma_50 = float(df['SMA_50'].iloc[-1])
        ema_9 = float(df['EMA_9'].iloc[-1])
        supertrend_val = float(df['SuperTrend'].iloc[-1])
        rsi_14 = float(df['RSI_14'].iloc[-1])

        def get_ret(days): return ((current_price / float(df['Close'].iloc[-min(len(df), days)])) - 1) * 100

        ret_1d, ret_1w, ret_1m = get_ret(2), get_ret(6), get_ret(21)
        ret_3m, ret_6m, ret_9m, ret_12m = get_ret(63), get_ret(126), get_ret(189), get_ret(252)
        
        # RS, SMA Dist, Sharpe
        rs_raw = (ret_3m * 0.40) + (ret_6m * 0.20) + (ret_9m * 0.20) + (ret_12m * 0.20)
        sma_dist = ((current_price / sma_200) - 1) * 100
        
        std_dev = rolling_window.std()
        
        # Protect against artificially low volatility (Upper Circuits/Illiquidity)
        # 0.005 daily std_dev is roughly an 8% annualized volatility. 
        # Anything below that in the Nifty 750 is highly suspicious.
        if std_dev < 0.005: 
            sharpe = 0 
        elif std_dev != 0:
            sharpe = ((rolling_window.mean() - daily_rf_rate) / std_dev) * math.sqrt(252)
        else:
            sharpe = 0
        # --- SIGNAL LOGIC ---
        sell_triggered = (current_price < sma_200) or (current_price < supertrend_val)
        buy_triggered = (
            (rsi_14 > 55) and (ret_1d > -5.0) and 
            ((ret_3m > 20.0) or (ret_6m > 30.0) or (ret_1m > 10.0)) and
            (sma_50 > sma_200) and (rs_raw > 80) and 
            (current_price > sma_200) and (current_price > supertrend_val) and
            (abs((current_price / ema_9) - 1) <= 0.05)
        )

        signal = "SELL" if sell_triggered else ("BUY" if buy_triggered else "HOLD")

        results.append({
            "Stock Name": ticker.replace('.NS', ''), "CMP": round(current_price, 2), "SMA 200": round(sma_200, 2),
            "1 Day Return (%)": round(ret_1d, 2), "1 Week Return (%)": round(ret_1w, 2), "1M Return (%)": round(ret_1m, 2), 
            "3M Return (%)": round(ret_3m, 2), "6M Return (%)": round(ret_6m, 2), "9M Return (%)": round(ret_9m, 2), "12M Return (%)": round(ret_12m, 2),
            "RS_Raw": rs_raw, "SMA_Dist": sma_dist, "Sharpe": round(sharpe, 2), "Signal": signal
        })
    except Exception: continue

# 2. Rank and Cut Top 20
df_final = pd.DataFrame(results)
df_final['RS (1-100)'] = (df_final['RS_Raw'].rank(pct=True) * 100).round(2)
df_final['SMA_Rank'] = (df_final['SMA_Dist'].rank(pct=True) * 100).round(2)
df_final['MintingM Score'] = ((df_final['RS (1-100)'] + df_final['SMA_Rank']) / 2).round(2)
top_20 = df_final.sort_values(by="MintingM Score", ascending=False).head(20).copy()

# 3. Merge Quarterly Data from Yesterday
qtr_cols = ['Qtr Profit Var %', 'QoQ profits %', 'QoQ sales %', 'OPM']
try:
    yesterday_df = pd.read_csv("live_cmp.csv")
    top_20 = pd.merge(top_20, yesterday_df[['Stock Name'] + qtr_cols], on='Stock Name', how='left')
    top_20[qtr_cols] = top_20[qtr_cols].fillna(0)
except FileNotFoundError:
    for col in qtr_cols: top_20[col] = 0

top_20['Last updated'] = datetime.now().strftime("%Y-%m-%d %H:%M")
cols = ["Stock Name", "CMP", "MintingM Score", "RS (1-100)", "SMA 200", "1 Day Return (%)", "1 Week Return (%)", "1M Return (%)", "3M Return (%)", "6M Return (%)", "9M Return (%)", "12M Return (%)", "Sharpe", "Signal"] + qtr_cols + ["Last updated"]

top_20[cols].to_csv("live_cmp.csv", index=False)
print("Morning Master Complete. Top 20 Saved.")
