import yfinance as yf
import pandas as pd
import pandas_ta as ta
import numpy as np
from datetime import datetime, timedelta
import math

def run_market_scan():
    # Calculate Indian Standard Time (UTC + 5:30)
    ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
    print(f"\n--- Starting Morning Master at {ist_now.strftime('%Y-%m-%d %H:%M:%S')} IST ---")

    # 1. Load Universe
    try:
        df_tickers = pd.read_csv("ind_nifty750list.csv")
        tickers = [str(symbol).strip() + ".NS" for symbol in df_tickers['Symbol'].tolist()]
    except FileNotFoundError:
        print("Error: ind_nifty750list.csv not found!")
        return

    # 2. Download exactly 1 year of data (365 calendar days)
    start_date = datetime.utcnow() - timedelta(days=365)
    data = yf.download(tickers, start=start_date.strftime('%Y-%m-%d'), group_by='ticker', threads=True)

    daily_rf_rate = 0.05 / 252
    results = []

    for ticker in tickers:
        try:
            df = data[ticker].dropna(subset=['High', 'Low', 'Close']).copy()
            
            # Since 1 calendar year = ~250 trading days, we need at least 200 days to calculate a 200 SMA
            if len(df) < 200:
                continue
                
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
            high_now = float(df['High'].iloc[-1])
            low_now = float(df['Low'].iloc[-1])
            sma_200 = float(df['SMA_200'].iloc[-1])
            sma_50 = float(df['SMA_50'].iloc[-1])
            ema_9 = float(df['EMA_9'].iloc[-1])
            supertrend_val = float(df['SuperTrend'].iloc[-1])
            rsi_14 = float(df['RSI_14'].iloc[-1])

            # --- Performance Returns ---
            # Safe return calculation based on available rows
            def get_ret(days): 
                safe_days = min(len(df) - 1, days)
                try:
                    return ((current_price / float(df['Close'].iloc[-safe_days])) - 1) * 100
                except:
                    return 0

            ret_1d, ret_1w, ret_1m = get_ret(2), get_ret(6), get_ret(21)
            ret_3m, ret_6m, ret_9m, ret_12m = get_ret(63), get_ret(126), get_ret(189), get_ret(252)
            
            # --- WEIGHTED MOMENTUM SHARPE ---
            df['Daily_Ret'] = df['Close'].pct_change()
            weighted_mean = df['Daily_Ret'].tail(63).ewm(span=63).mean().iloc[-1]
            stable_std = df['Daily_Ret'].std() # Uses available 1-year data

            if stable_std > 0.005:
                sharpe = ((weighted_mean - daily_rf_rate) / stable_std) * 10
            else:
                sharpe = 0
                
            rs_raw = (ret_3m * 0.40) + (ret_6m * 0.20) + (ret_9m * 0.20) + (ret_12m * 0.20)
            sma_dist = ((current_price / sma_200) - 1) * 100
            
            # --- SIGNAL LOGIC ---
            is_circuit_day = (high_now == low_now)
            price_above_sma200 = (current_price > sma_200) if not pd.isna(sma_200) else False
            price_above_supertrend = (current_price > supertrend_val) if not pd.isna(supertrend_val) else True

            sell_triggered = (not price_above_sma200) or (not price_above_supertrend)
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

            if sell_triggered: signal = "SELL"
            elif buy_triggered: signal = "BUY"
            else: signal = "HOLD"

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

    # 3. Process Results and Ranking
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

    qtr_cols = ['Qtr Profit Var %', 'QoQ profits %', 'QoQ sales %', 'OPM']
    try:
        yesterday_df = pd.read_csv("live_cmp.csv")
        top_20 = pd.merge(top_20, yesterday_df[['Stock Name'] + qtr_cols], on='Stock Name', how='left')
        top_20[qtr_cols] = top_20[qtr_cols].fillna(0)
    except Exception:
        for col in qtr_cols: 
            top_20[col] = 0

    # --- INDIAN TIME ASSIGNMENT ---
    top_20['Last updated'] = ist_now.strftime("%Y-%m-%d %H:%M")
    top_20['TradingView Link'] = "https://in.tradingview.com/chart/?symbol=NSE:" + top_20['Stock Name'].astype(str)

    final_cols = ["Stock Name", "CMP", "MintingM Score", "RS (1-100)", "SMA 200", "SuperTrend", "1 Day Return (%)", 
                  "1 Week Return (%)", "1M Return (%)", "3M Return (%)", "6M Return (%)", 
                  "9M Return (%)", "12M Return (%)", "Sharpe", "Signal"] + qtr_cols + ["Last updated", "TradingView Link"]

    top_20[final_cols].to_csv("live_cmp.csv", index=False)
    print(f"Morning Master Complete. {len(top_20)} stocks saved to live_cmp.csv")

# Only run once, immediately, and then let the file close so GitHub Actions can finish
if __name__ == "__main__":
    run_market_scan()
