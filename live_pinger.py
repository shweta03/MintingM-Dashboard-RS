import pandas as pd
import yfinance as yf
from datetime import datetime

print("Starting 45-Min Live CMP Pinger...")
df = pd.read_csv("live_cmp.csv")

for index, row in df.iterrows():
    try:
        ticker = str(row['Stock Name']) + ".NS"
        live_price = round(yf.Ticker(ticker).fast_info['last_price'], 2)
        df.at[index, 'CMP'] = live_price
    except: pass

df['Last updated'] = datetime.now().strftime("%Y-%m-%d %H:%M")
df.to_csv("live_cmp.csv", index=False)
print(f"Live CMPs updated at {df['Last updated'][0]}")
