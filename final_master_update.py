import pandas as pd
import os

# --- Configuration ---
# 1. PATH TO THE OLD CONSOLIDATED FILE (Your Jan-Sep Master with OLD Sep data)
old_consolidated_path = r'C:\temp\client_sales_by_month_report_final.csv'

# 2. PATH TO THE NEW SEP CONSOLIDATED FILE (The correct data you just generated)
new_sep_consolidated_path = r'C:\temp\client_monthly_sales_report_SEPTEMBER_FINAL.csv'

# 3. Output path for the FINAL, UPDATED Jan-Sep Report
output_dir = r'C:\temp'
output_filename = 'client_sales_JAN_TO_SEP_MASTER_FINAL_CORRECTED.csv'
output_path = os.path.join(output_dir, output_filename)

months_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep']

# The descriptive columns used for grouping (must match between files)
descriptive_cols = [
    'sourcezone', 'sourcestate', 'source_id', 'sourcename', 
    'distributor_type', 'client_unique_id'
]
# NOTE: The column for the total sales in your old file is likely 'Total_Sales_Jan_to_Sep'.
old_total_col = 'Total_Sales_Jan_to_Sep' 
new_total_col = 'Total_PTSRateOrderValue' # From the new Sep file

# ---------------------

# 1. Load the old consolidated data (Jan-Sep with old Sep)
print(f"Loading old consolidated data from: {old_consolidated_path}")
try:
    df_old = pd.read_csv(old_consolidated_path)
except Exception as e:
    print(f"ERROR: Could not load old consolidated file: {e}")
    exit()

# 2. Load the new September consolidated data
print(f"Loading new September consolidated data from: {new_sep_consolidated_path}")
try:
    df_new_sep = pd.read_csv(new_sep_consolidated_path)
    
    # Rename 'Sale_Sep' to 'Sep' to match the old file's column naming convention
    if 'Sale_Sep' in df_new_sep.columns:
        df_new_sep.rename(columns={'Sale_Sep': 'Sep'}, inplace=True)
    
    # Keep only the grouping columns and the new 'Sep' sales column
    df_new_sep = df_new_sep[descriptive_cols + ['Sep']]
except Exception as e:
    print(f"ERROR: Could not load new September file: {e}")
    exit()

# 3. Prepare the old data for merge
print("Merging data: Replacing old September sales with new data...")

# Drop the old 'Sep' sales column and the old total column
# This removes the incomplete data
df_final = df_old.drop(columns=['Sep', old_total_col], errors='ignore')

# Perform a merge (outer merge ensures clients who only bought in Sep are included)
# This inserts the corrected 'Sep' column
df_final = pd.merge(
    df_final, 
    df_new_sep, 
    on=descriptive_cols, 
    how='outer'
).fillna(0)

# 4. Final calculation and cleanup
# Identify all monthly columns (Jan through Sep)
final_monthly_cols = [m for m in months_order if m in df_final.columns]

# Recalculate the Grand Total Sales using the new, correct Sep data
# Note: Using the original column name for the total: 'Total_Sales_Jan_to_Sep'
df_final[old_total_col] = df_final[final_monthly_cols].sum(axis=1)

# Ensure the columns are in the correct final order
final_cols = descriptive_cols + final_monthly_cols + [old_total_col]
df_final = df_final[final_cols]

# 5. Save the final report
try:
    os.makedirs(output_dir, exist_ok=True)
    df_final.to_csv(output_path, index=False)
    print("-" * 50)
    print("FINAL MASTER REPORT UPDATED SUCCESSFULLY.")
    print(f"Report has {len(df_final)} unique client rows.")
    print(f"Saved to: {output_path}")
    print("-" * 50)
except Exception as e:
    print(f"ERROR saving final file: {e}")