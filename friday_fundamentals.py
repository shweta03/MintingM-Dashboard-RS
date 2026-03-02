import yfinance as yf
import pandas as pd
import numpy as np

def safe_div(n, d): 
    if d == 0 or pd.isna(n) or pd.isna(d): 
        return 0
    return n / d

def get_safe_value(df, row_name, col_index):
    """Safely extracts a value if the row exists, otherwise returns 0."""
    if row_name in df.index:
        val = df.loc[row_name].iloc[col_index]
        return 0 if pd.isna(val) else val
    return 0

print("Starting Friday Fundamentals Fetch for Top 20...")
df = pd.read_csv("live_cmp.csv")

for index, row in df.iterrows():
    ticker = str(row['Stock Name']) + ".NS"
    try:
        stock = yf.Ticker(ticker)
        # Using the newer, more stable income statement parser
        q_fin = stock.quarterly_income_stmt
        
        if not q_fin.empty and q_fin.shape[1] >= 2:
            # Safely get values, defaulting to 0 if the row doesn't exist
            op_income = get_safe_value(q_fin, 'Operating Income', 0)
            total_rev = get_safe_value(q_fin, 'Total Revenue', 0)
            
            net_inc_0 = get_safe_value(q_fin, 'Net Income', 0)
            net_inc_1 = get_safe_value(q_fin, 'Net Income', 1)
            
            total_rev_0 = get_safe_value(q_fin, 'Total Revenue', 0)
            total_rev_1 = get_safe_value(q_fin, 'Total Revenue', 1)
            
            # Calculations
            opm = round(safe_div(op_income, total_rev) * 100, 2)
            qoq_p = round((safe_div(net_inc_0, net_inc_1) - 1) * 100, 2)
            qoq_s = round((safe_div(total_rev_0, total_rev_1) - 1) * 100, 2)
            
            # Update Dataframe
            df.at[index, "OPM"] = opm
            df.at[index, "QoQ profits %"] = qoq_p
            df.at[index, "Qtr Profit Var %"] = qoq_p
            df.at[index, "QoQ sales %"] = qoq_s
            
            print(f"SUCCESS: {ticker} fundamentals updated.")
        else:
            print(f"SKIPPED: {ticker} - Not enough quarterly data available.")
            
    except Exception as e:
        print(f"FAILED: {ticker} - Error: {e}")

# Save the updated dataframe
df.to_csv("live_cmp.csv", index=False)
print("Fundamentals update process completed.")
