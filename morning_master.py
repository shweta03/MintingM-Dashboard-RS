import yfinance as yf
import pandas as pd
import pandas_ta as ta
import numpy as np
from datetime import datetime, timedelta

print("Starting Morning Master (750 Universe Scan) with RS-Top 6 Logic...")

# 1. Load Universe
try:
    df_tickers = pd.read_csv("ind_nifty750list.csv")
    tickers = [str(symbol).strip() + ".NS" for symbol in df_tickers['Symbol'].tolist()]
except FileNotFoundError:
    print("Error: ind_nifty750list.csv not found!")
    exit(1)

start_date = datetime.today() - timedelta(days=400)
data = yf.download(tickers, start=start_date.strftime('%Y-%m-%d'), group_by='ticker', threads=True)

daily_rf_rate = 0.05 / 252
results = []

for ticker in tickers:
    try:
        df = data[ticker].dropna(subset=['High', 'Low', 'Close']).copy()
        if len(df) < 252:
            continue
            
        # --- TA Indicators ---
        df['SMA_50'] = ta.sma(df['Close'], length=50)
        df['SMA_200'] = ta.sma(df['Close'], length=200)
        df['EMA_9'] = ta.ema(df['Close'], length=9)
        df['RSI_14'] = ta.rsi(df['Close'], length=14)
        
        # SuperTrend (15, 2.75)
        st = ta.supertrend(df['High'], df['Low'], df['Close'], length=15, multiplier=2.75)
        st_col = [c for c in st.columns if c.startswith('SUPERT_')][0]
        df['SuperTrend'] = st[st_col]
            
        current_price = float(df['Close'].iloc[-1])
        high_now = float(df['High'].iloc[-1])
        low_now = float(df['Low'].iloc[-1])
        sma_200 = float(df['SMA_200'].iloc[-1])
        sma_50 = float(df['SMA_50'].iloc[-1])
        ema_9 = float(df['EMA_9'].iloc[-1])
        supertrend_val = float(df['SuperTrend'].iloc[-1])
        rsi_14 = float(df['RSI_14'].iloc[-1])

        # --- Returns ---
        def get_ret(days): 
            try:
                return ((current_price / float(df['Close'].iloc[-min(len(df), days)])) - 1) * 100
            except: return 0

        ret_1d, ret_1m = get_ret(2), get_ret(21)
        ret_3m, ret_6m, ret_9m, ret_12m = get_ret(63), get_ret(126), get_ret(189), get_ret(252)
        
        # Sharpe Calculation
        df['Daily_Ret'] = df['Close'].pct_change()
        weighted_mean = df['Daily_Ret'].tail(63).ewm(span=63).mean().iloc[-1]
        stable_std = df['Daily_Ret'].tail(252).std()
        sharpe = ((weighted_mean - daily_rf_rate) / stable_std) * 10 if stable_std > 0.005 else 0
            
        # RS and SMA Distance
        rs_raw = (ret_3m * 0.40) + (ret_6m * 0.20) + (ret_9m * 0.20) + (ret_12m * 0.20)
        sma_dist = ((current_price / sma_200) - 1) * 100
        
        # --- UPDATED SIGNAL LOGIC ---
        
        # 1. Circuit Filter: Avoid trading if High == Low
        is_circuit_day = (high_now == low_now)
        
        # 2. Sell Logic: Price < 200 SMA OR Price < SuperTrend
        # Note: 10% Stop Loss logic usually requires knowing your purchase price.
        sell_triggered = (current_price < sma_200) or (current_price < supertrend_val)
        
        # 3. Buy Logic
        buy_triggered = (
            not is_circuit_day and
            (rsi_14 > 55) and 
            (ret_1d > -5.0) and 
            ((ret_3m > 20.0) or (ret_6m > 30.0) or (ret_1m > 10.0)) and
            (sma_50 > sma_200) and 
            (rs_raw > 80) and 
            (current_price > sma_200) and 
            (current_price > supertrend_val) and
            (abs((current_price / ema_9) - 1) <= 0.05) # Price within 5% of EMA 9
        )

        if is_circuit_day:
            signal = "CIRCUIT/HOLD"
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
            "1M Return (%)": round(ret_1m, 2), 
            "3M Return (%)": round(ret_3m, 2), 
            "6M Return (%)": round(ret_6m, 2), 
            "RS_Raw": rs_raw, 
            "SMA_Dist": sma_dist, 
            "Sharpe": round(sharpe, 2), 
            "Signal": signal
        })
    except Exception:
        continue

# 2. Process Results and Ranking
df_final = pd.DataFrame(results)

# Filter for BUY signals and take the TOP 6 by RS_Raw Descending
buy_only = df_final[df_final['Signal'] == "BUY"].copy()
top_6_picks = buy_only.sort_values(by="RS_Raw", ascending=False).head(6)

# Fallback: if fewer than 6 buys, show top 20 by MintingM Score as you had before
df_final['RS (1-100)'] = (df_final['RS_Raw'].rank(pct=True) * 100).round(2)
df_final['SMA_Rank'] = (df_final['SMA_Dist'].rank(pct=True) * 100).round(2)
df_final['MintingM Score'] = ((df_final['RS (1-100)'] + df_final['SMA_Rank']) / 2).round(2)
top_20 = df_final.sort_values(by="MintingM Score", ascending=False).head(20).copy()

# 3. Save to CSV (using top_20 for broad view, but highlighted top_6 in console)
top_20.to_csv("live_cmp.csv", index=False)
print(f"Morning Master Complete. {len(top_20)} stocks saved. Top 6 RS Buys identified.")
print(top_6_picks[["Stock Name", "CMP", "RS_Raw", "Signal"]])
