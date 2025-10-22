import pandas as pd
import os

# --- 1. CONFIGURATION (FOR JAN TO FULL OCT) ---

# Path to the finalized September data file (No change needed)
RAW_SEP_PATH = r"C:\Users\ADMIN\Documents\DMS MASTER_25 NEW\DMS Order Report_Sep.xlsx"

# Path to the NEW cumulative October data file (CONFIRMED PATH)
RAW_OCT_PATH = r"C:\Users\ADMIN\Documents\DMS MASTER_25 NEW\DMS Order Report_Oct.xlsx"

# Path to the old master report (Input for merging - Jan to Aug base)
OLD_MASTER_PATH = r'C:\temp\client_sales_by_month_report_final.csv'

# Output path for the FINAL, UPDATED Jan-Full Oct Report
OUTPUT_DIR = r'C:\temp'
OUTPUT_FILENAME = 'client_sales_JAN_TO_OCT_MASTER_FINAL.csv'
OUTPUT_PATH = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)

# Common data columns
MONTHS_ORDER = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct'] 
DATE_COLUMN = 'orderdate'
SALES_VALUE_COLUMN = 'ptsrateordervalue'
DESCRIPTIVE_COLS = [
    'sourcezone', 'sourcestate', 'source_id', 'sourcename', 
    'distributor_type', 'client_unique_id'
]
COLUMN_MAPPING = {
    'sourcezone': 'sourcezone', 'sourcestate': 'sourcestate', 
    'source id': 'source_id', 'sourcename': 'sourcename', 
    'distributor type': 'distributor_type', 'client unique id': 'client_unique_id',
}
FINAL_TOTAL_COL = 'Total_Sales_Jan_to_Oct' 

# --------------------------------------------------------------------------

def consolidate_month(file_path, month_name):
    """Loads raw data, aggressively cleans, consolidates sales, and returns a summary DataFrame."""
    print(f"\n--- Consolidating Raw Data for {month_name} ---")
    
    try:
        excel_data = pd.read_excel(file_path, sheet_name=None)
        df_raw = pd.concat(excel_data.values(), ignore_index=True)
        print(f"-> {len(df_raw)} total records loaded from {file_path}")
        
    except Exception as e:
        print(f"ERROR: Could not load raw file {file_path}. {e}")
        return pd.DataFrame() 

    # Clean column headers
    df_raw.columns = df_raw.columns.str.strip().str.lower()
    
    df_raw[DATE_COLUMN] = pd.to_datetime(df_raw[DATE_COLUMN], errors='coerce', dayfirst=True)
    df_raw['extracted_month'] = df_raw[DATE_COLUMN].dt.strftime('%b')

    df_month = df_raw[df_raw['extracted_month'] == month_name].copy()
    print(f"-> {len(df_month)} transactions filtered for {month_name}")

    # <<< CRITICAL FIX: AGGRESSIVE CLEANING BEFORE PIVOTING >>>
    # This step ensures inconsistent formatting doesn't split a single client into multiple rows.
    for col in COLUMN_MAPPING.keys():
        if col in df_month.columns:
            df_month[col] = df_month[col].astype(str).str.strip().str.lower()
    
    # Create the pivot table (Sum Sales by Client/Source) to get UNIQUE sales values
    pivot_index_cols = list(COLUMN_MAPPING.keys()) 
    pivot_df = df_month.pivot_table(
        index=pivot_index_cols, 
        values=SALES_VALUE_COLUMN,
        aggfunc='sum'
    ).reset_index()

    # Final Formatting
    pivot_df.rename(columns=COLUMN_MAPPING, inplace=True)
    pivot_df.rename(columns={SALES_VALUE_COLUMN: month_name}, inplace=True)
    
    # Final cleanup on the client ID column after pivot
    pivot_df['client_unique_id'] = pivot_df['client_unique_id'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

    return pivot_df[DESCRIPTIVE_COLS + [month_name]]

# --------------------------------------------------------------------------

# 1. STEP 1 & 2: CONSOLIDATE SEPTEMBER & OCTOBER
df_sep_summary = consolidate_month(RAW_SEP_PATH, 'Sep')
df_oct_summary = consolidate_month(RAW_OCT_PATH, 'Oct')

if df_sep_summary.empty or df_oct_summary.empty:
    print("FATAL: Consolidation failed for Sep or Oct. Exiting.")
    exit()

# 3. STEP 3: MERGE DATA INTO MASTER REPORT

print("\n--- Merging Consolidated Data into Master Report ---")

# A. Load the old master file (Jan-Aug base)
try:
    df_master = pd.read_csv(OLD_MASTER_PATH, dtype={'client_unique_id': str}) 
    
    # <<< CRITICAL FIX: CLEAN MASTER FILE COLUMNS >>>
    # Apply the same aggressive cleaning to the master file columns before merge
    for col in DESCRIPTIVE_COLS:
         df_master[col] = df_master[col].astype(str).str.strip().str.lower()
    
    # Drop ANY columns that might exist for Sep, Oct, or Total sales to ensure a clean merge
    df_master = df_master.drop(columns=['Sep', 'Oct', 'Total_Sales_Jan_to_Sep', 'Total_Sales_Jan_to_Oct_16'], errors='ignore') 
    
    df_master['client_unique_id'] = df_master['client_unique_id'].astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    
    print(f"-> Master file loaded with {len(df_master)} unique client rows.")
except Exception as e:
    print(f"ERROR: Could not load old master file {OLD_MASTER_PATH}. {e}")
    exit()

# B. Merge FINAL September Data
df_master = pd.merge(
    df_master, 
    df_sep_summary, 
    on=DESCRIPTIVE_COLS, 
    how='outer'
).fillna(0)
print("-> Final September data merged.")

# C. Merge FINAL October Data
df_master = pd.merge(
    df_master, 
    df_oct_summary, 
    on=DESCRIPTIVE_COLS, 
    how='outer'
).fillna(0)
print("-> October data merged.")


# D. Final Calculation and Cleanup
final_monthly_cols = [m for m in MONTHS_ORDER if m in df_master.columns]

# Recalculate the Grand Total Sales (Jan to Full Oct)
df_master[FINAL_TOTAL_COL] = df_master[final_monthly_cols].sum(axis=1)

# Ensure the columns are in the correct final order
final_cols = DESCRIPTIVE_COLS + final_monthly_cols + [FINAL_TOTAL_COL]
df_master = df_master[final_cols]


# 4. Save the Final Report
try:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df_master.to_csv(OUTPUT_PATH, index=False)
    
    print("\n" + "=" * 60)
    print("✅ FINAL MASTER REPORT UPDATED SUCCESSFULLY (JAN TO FULL OCT).")
    print(f"Report has {len(df_master)} unique client rows.")
    print(f"Saved to: {OUTPUT_PATH}")
    print("=" * 60)
except Exception as e:
    print(f"FATAL ERROR: Could not save final file: {e}")
