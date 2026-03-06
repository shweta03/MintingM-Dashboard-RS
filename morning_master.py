import yfinance as yf
import pandas as pd
import pandas_ta as ta
import numpy as np
from datetime import datetime, timedelta
import time
import math
import os

def run_market_scan():
    print(f"\n--- Starting Morning Master (750 Universe Scan) at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")

    # 1. Load Universe
    try:
        df_tickers = pd.read_csv("ind_nifty750list.csv")
        tickers = [str(symbol).strip() + ".NS" for symbol in df_tickers['Symbol'].tolist()]
    except FileNotFoundError:
        print("Error: ind_nifty750list.csv not found!")
        return # Prevents the whole loop from crashing if the file is missing

    # Download 1.5 years of data (for 252 days + 200 SMA buffer)
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
                # More robust SuperTrend extraction
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
            
            # --- WEIGHTED MOMENTUM SHARPE (NOT ANNUALIZED) ---
            df['Daily_Ret'] = df['Close'].pct_change()
            weighted_mean = df['Daily_Ret'].tail(63).ewm(span=63).mean().iloc[-1]
            stable_std = df['Daily_Ret'].tail(252).std()

            if stable_std > 0.005:
                sharpe = ((weighted_mean - daily_rf_rate) / stable_std) * 10
            else:
                sharpe = 0
                
            # RS and SMA Distance
            rs_raw = (ret_3m * 0.40) + (ret_6m * 0.20) + (ret_9m * 0.20) + (ret_12m * 0.20)
            sma_dist = ((current_price / sma_200) - 1) * 100
            
            # --- NEW SIGNAL LOGIC ---
            is_circuit_day = (high_now == low_now)
            price_above_sma200 = (current_price > sma_200) if not pd.isna(sma_200) else False
            price_above_supertrend = (current_price > supertrend_val) if not pd.isna(supertrend_val) else True

            # Priority 1: Sell Logic
            sell_triggered = (not price_above_sma200) or (not price_above_supertrend)
            
            # Priority 2: Buy Logic
            buy_triggered = (
                not is_circuit_day and
                (rsi_14 > 55) and 
                (ret_1d > -5.0) and 
                ((ret_3m > 20.0) or (ret_6m > 30.0) or (ret_1m > 10.0)) and
                (sma_50 > sma_200) and 
                (rs_raw > 80) and 
                price_above_sma200 and 
                price_above_supertrend and
                (abs((current_price / ema_9) - 1) <= 0.05)
            )

            # Final Assignment
            if sell_triggered:
                signal = "SELL"
            elif buy_triggered:
                signal = "BUY"
            else:
                signal = "HOLD"

            results.append({
                "Stock Name": ticker.replace('.NS', ''), 
                "CMP": round(current_price, 2), 
                "SMA 200": round(sma_200, 2),
                "SuperTrend": round(supertrend_val, 2) if not pd.isna(supertrend_val) else 0,
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
        print("No data collected. Check internet or ticker list.")
        return

    df_final = pd.DataFrame(results)

    if 'RS_Raw' in df_final.columns and 'SMA_Dist' in df_final.columns:
        df_final['RS (1-100)'] = (df_final['RS_Raw'].rank(pct=True) * 100).round(2)
        df_final['SMA_Rank'] = (df_final['SMA_Dist'].rank(pct=True) * 100).round(2)
        df_final['MintingM Score'] = ((df_final['RS (1-100)'] + df_final['SMA_Rank']) / 2).round(2)
        top_20 = df_final.sort_values(by="MintingM Score", ascending=False).head(20).copy()
    else:
        print("Ranking failed: Missing data columns.")
        return

    # 3. Merge Quarterly Data
    qtr_cols = ['Qtr Profit Var %', 'QoQ profits %', 'QoQ sales %', 'OPM']
    try:
        yesterday_df = pd.read_csv("live_cmp.csv")
        top_20 = pd.merge(top_20, yesterday_df[['Stock Name'] + qtr_cols], on='Stock Name', how='left')
        top_20[qtr_cols] = top_20[qtr_cols].fillna(0)
    except Exception:
        for col in qtr_cols: 
            top_20[col] = 0

    top_20['Last updated'] = datetime.now().strftime("%Y-%m-%d %H:%M")

    # --- ADD TRADINGVIEW LINK HERE ---
    top_20['TradingView Link'] = "https://in.tradingview.com/chart/?symbol=NSE:" + top_20['Stock Name'].astype(str)

    # UPDATED TO INCLUDE SUPERTREND & TRADINGVIEW LINK
    final_cols = ["Stock Name", "CMP", "MintingM Score", "RS (1-100)", "SMA 200", "SuperTrend", "1 Day Return (%)", 
                  "1 Week Return (%)", "1M Return (%)", "3M Return (%)", "6M Return (%)", 
                  "9M Return (%)", "12M Return (%)", "Sharpe", "Signal"] + qtr_cols + ["Last updated", "TradingView Link"]

    top_20[final_cols].to_csv("live_cmp.csv", index=False)
    print(f"Morning Master Complete. {len(top_20)} stocks saved to live_cmp.csv")

    # --- AUTO-UPLOAD TO GITHUB ---
    print("Uploading fresh data directly to GitHub...")
    os.system('git add live_cmp.csv')
    os.system('git commit -m "Auto-update MintingM scores"')
    os.system('git push')
    print("Upload complete! GitHub now has the live data.")


# =====================================================================
# --- THE MASTER EXECUTION LOOP (MON-FRI, 9:30 AM TO 3:30 PM) ---
# =====================================================================
if __name__ == "__main__":
    while True:
        now = datetime.now()
        
        # In Python, Monday is 0, Friday is 4. Weekend is 5 and 6.
        is_weekday = now.weekday() < 5 
        # Check if time is between 9:30 AM and 3:30 PM inclusive
        is_market_hours = (now.hour > 9 or (now.hour == 9 and now.minute >= 30)) and (now.hour < 15 or (now.hour == 15 and now.minute <= 30))
        
        if is_weekday and is_market_hours:
            print(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] Market is OPEN. Running scan...")
            try:
                run_market_scan()
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
            
            # Synchronize to the exact top (:00) or bottom (:30) of the hour
            now_after = datetime.now()
            
            if now_after.minute < 30:
                next_run = now_after.replace(minute=30, second=0, microsecond=0)
            else:
                next_run = (now_after + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            
            wait_secs = (next_run - now_after).total_seconds()
            print(f"Scan finished! Synchronizing clock. Next run exactly at {next_run.strftime('%H:%M:%S')}...")
            time.sleep(wait_secs)
            
        else:
            # Market is CLOSED. Figure out exactly when it opens next.
            print(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] Market is CLOSED.")
            
            # Assume next open is today at 9:30 AM
            next_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
            
            # If it's already past 3:30 PM today, push the target to tomorrow
            if now.hour > 15 or (now.hour == 15 and now.minute > 30):
                next_open += timedelta(days=1)
            
            # If the target day falls on Saturday (5) or Sunday (6), push it to Monday
            while next_open.weekday() >= 5:
                next_open += timedelta(days=1)
                
            wait_secs = (next_open - now).total_seconds()
            print(f"Sleeping until market opens on {next_open.strftime('%A, %Y-%m-%d at %H:%M:%S')}...")
            time.sleep(wait_secs)
