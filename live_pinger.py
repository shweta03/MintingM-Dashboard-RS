import pandas as pd
import yfinance as yf
from datetime import datetime
import os

def update_live_prices():
    print(f"\n--- Starting Live CMP Pinger at {datetime.now().strftime('%H:%M:%S')} ---")
    try:
        df = pd.read_csv("live_cmp.csv")
        
        for index, row in df.iterrows():
            try:
                ticker = str(row['Stock Name']) + ".NS"
                live_price = round(yf.Ticker(ticker).history(period="1d")['Close'].iloc[-1], 2)
                df.at[index, 'CMP'] = live_price
            except Exception as e: 
                pass 
        
        df['Last updated'] = datetime.now().strftime("%Y-%m-%d %H:%M")
        df.to_csv("live_cmp.csv", index=False)
        print(f"Live CMPs updated at {df['Last updated'].iloc[0]}")
        
    except Exception as e:
        print(f"Error reading or updating CSV: {e}")

if __name__ == "__main__":
    update_live_prices()
