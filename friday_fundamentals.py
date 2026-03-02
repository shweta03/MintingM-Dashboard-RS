import yfinance as yf
import pandas as pd
import numpy as np

def safe_div(n, d): return 0 if d == 0 or pd.isna(n) or pd.isna(d) else n / d

print("Starting Friday Fundamentals Fetch for Top 20...")
df = pd.read_csv("live_cmp.csv")

for index, row in df.iterrows():
    ticker = str(row['Stock Name']) + ".NS"
    try:
        q_fin = yf.Ticker(ticker).quarterly_financials
        if not q_fin.empty and q_fin.shape[1] >= 2:
            opm = round(safe_div(q_fin.loc['Operating Income'].iloc[0], q_fin.loc['Total Revenue'].iloc[0]) * 100, 2)
            qoq_p = round((safe_div(q_fin.loc['Net Income'].iloc[0], q_fin.loc['Net Income'].iloc[1]) - 1) * 100, 2)
            qoq_s = round((safe_div(q_fin.loc['Total Revenue'].iloc[0], q_fin.loc['Total Revenue'].iloc[1]) - 1) * 100, 2)
            
            df.at[index, "OPM"] = opm
            df.at[index, "QoQ profits %"] = qoq_p
            df.at[index, "Qtr Profit Var %"] = qoq_p
            df.at[index, "QoQ sales %"] = qoq_s
    except Exception as e:
        print(f"Skipped fundamental fetch for {ticker}: {e}")

df.to_csv("live_cmp.csv", index=False)
print("Fundamentals updated successfully.")
