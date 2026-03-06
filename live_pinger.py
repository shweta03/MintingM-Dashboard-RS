import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import time
import os

def update_live_prices():
    print(f"\n--- Starting 30-Min Live CMP Pinger at {datetime.now().strftime('%H:%M:%S')} ---")
    try:
        df = pd.read_csv("live_cmp.csv")
        
        for index, row in df.iterrows():
            try:
                ticker = str(row['Stock Name']) + ".NS"
                # Safest way to grab the exact live price
                live_price = round(yf.Ticker(ticker).history(period="1d")['Close'].iloc[-1], 2)
                df.at[index, 'CMP'] = live_price
            except Exception as e: 
                pass # If Yahoo fails for one stock, keep the old price and move on
        
        df['Last updated'] = datetime.now().strftime("%Y-%m-%d %H:%M")
        df.to_csv("live_cmp.csv", index=False)
        print(f"Live CMPs updated at {df['Last updated'].iloc[0]}")
        
        # --- AUTO-UPLOAD TO GITHUB ---
        print("Pushing quick price updates to GitHub...")
        os.system('git add live_cmp.csv')
        os.system('git commit -m "Auto-update Live CMPs"')
        os.system('git push')
        print("Upload complete! GitHub now has the live prices.")
        
    except Exception as e:
        print(f"Error reading or updating CSV: {e}")

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
            update_live_prices()
            
            # Synchronize to the exact top (:00) or bottom (:30) of the hour
            now_after = datetime.now()
            
            if now_after.minute < 30:
                next_run = now_after.replace(minute=30, second=0, microsecond=0)
            else:
                next_run = (now_after + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            
            wait_secs = (next_run - now_after).total_seconds()
            print(f"Pinger finished! Next run exactly at {next_run.strftime('%H:%M:%S')}...")
            time.sleep(wait_secs)
            
        else:
            print(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')}] Market is CLOSED.")
            
            # Figure out exactly when it opens next (9:30 AM)
            next_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
            
            if now.hour > 15 or (now.hour == 15 and now.minute > 30):
                next_open += timedelta(days=1)
            
            while next_open.weekday() >= 5:
                next_open += timedelta(days=1)
                
            wait_secs = (next_open - now).total_seconds()
            print(f"Sleeping until market opens on {next_open.strftime('%A, %Y-%m-%d at %H:%M:%S')}...")
            time.sleep(wait_secs)
