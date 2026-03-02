import yfinance as yf
import pandas as pd
import numpy as np

print("Starting Friday Fundamentals Fetch for Top 20...")
df = pd.read_csv("live_cmp.csv")

# Helper to find the right column name since Indian stocks vary
def get_col(df_transposed, possible_names):
    for name in possible_names:
        if name in df_transposed.columns:
            return name
    return None

rev_names = ['Total Revenue', 'Operating Revenue', 'Revenue']
op_names = ['Operating Income', 'EBIT', 'Gross Profit']
net_names = ['Net Income', 'Net Income Common Stockholders', 'Net Income Continuous Operations']

for index, row in df.iterrows():
    ticker_str = str(row['Stock Name']) + ".NS"
    
    # Set default values
    qoq_profit, qtr_profit_var, qoq_sales, opm = 0.0, 0.0, 0.0, 0.0
    
    try:
        ticker = yf.Ticker(ticker_str)
        q_income = ticker.quarterly_income_stmt
        
        if not q_income.empty:
            # Transpose so Dates become rows (Row 0 = Latest, Row 1 = Previous, Row 4 = YoY)
            q_income = q_income.T  
            
            # Find the actual column names present for this specific stock
            rev_col = get_col(q_income, rev_names)
            op_col = get_col(q_income, op_names)
            net_col = get_col(q_income, net_names)

            # --- 1. Calculate OPM ---
            if op_col and rev_col:
                try:
                    op_inc = q_income[op_col].iloc[0]
                    tot_rev = q_income[rev_col].iloc[0]
                    if tot_rev and tot_rev != 0 and not pd.isna(tot_rev):
                        opm = (op_inc / tot_rev) * 100
                except: pass
            
            # --- 2. Calculate QoQ Sales & Profits ---
            if len(q_income) >= 2:
                if rev_col:
                    try:
                        curr_rev = q_income[rev_col].iloc[0]
                        prev_rev = q_income[rev_col].iloc[1]
                        if prev_rev and prev_rev != 0 and not pd.isna(prev_rev):
                            qoq_sales = ((curr_rev / prev_rev) - 1) * 100
                    except: pass
                
                if net_col:
                    try:
                        curr_ni = q_income[net_col].iloc[0]
                        prev_ni = q_income[net_col].iloc[1]
                        if prev_ni and prev_ni != 0 and not pd.isna(prev_ni):
                            # Using abs() to correctly calculate % change on negative profits
                            qoq_profit = ((curr_ni - prev_ni) / abs(prev_ni)) * 100 
                    except: pass
            
            # --- 3. Calculate Qtrly Profit Variance (YoY) ---
            if len(q_income) >= 5 and net_col:
                try:
                    curr_ni = q_income[net_col].iloc[0]
                    yoy_ni = q_income[net_col].iloc[4] # 4 quarters ago
                    if yoy_ni and yoy_ni != 0 and not pd.isna(yoy_ni):
                        qtr_profit_var = ((curr_ni - yoy_ni) / abs(yoy_ni)) * 100
                except: pass

    except Exception as e:
        print(f"Error fetching {ticker_str}: {e}")

    # Update Dataframe safely (fallback to 0 if NaN)
    df.at[index, "OPM"] = round(opm, 2) if pd.notna(opm) else 0.0
    df.at[index, "QoQ sales %"] = round(qoq_sales, 2) if pd.notna(qoq_sales) else 0.0
    df.at[index, "QoQ profits %"] = round(qoq_profit, 2) if pd.notna(qoq_profit) else 0.0
    df.at[index, "Qtr Profit Var %"] = round(qtr_profit_var, 2) if pd.notna(qtr_profit_var) else 0.0
    
    # Print clean progress to GitHub Actions console
    print(f"SUCCESS: {ticker_str} | OPM: {round(opm,2)}% | QoQ Profit: {round(qoq_profit,2)}% | YoY Profit: {round(qtr_profit_var,2)}% | QoQ Sales: {round(qoq_sales,2)}%")

# Save to CSV
df.to_csv("live_cmp.csv", index=False)
print("Fundamentals update process completed.")
