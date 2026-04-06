import pandas as pd
import os
from pathlib import Path

"""
ETL - Sales Processing with Date Conversion and Export

This script extracts sales data from a CSV file, calculates total sales revenue, 
converts the sale date strings to datetime objects, and exports the final report 
sorted by date (descending) to Excel.

Steps:
1. Extraction: Load data from 'sales.csv'.
2. Transformation:
   - Calculate 'total_revenue' as unit_price * quantity.
   - Convert 'sale_date' to datetime objects.
   - Sort data by 'sale_date' (descending).
3. Loading: Export the result as an Excel file named 'sales_report.xlsx'.
"""

# Define paths relative to the script directory
script_dir = Path(__file__).parent
input_file = script_dir / 'data' / 'raw' / 'sales.csv'
output_file = script_dir / 'data' / 'processed' / 'sales_report.xlsx'

# Extraction Phase
print('🚀 Extracting sales data...')
if input_file.exists():
    df = pd.read_csv(input_file)
else:
    print(f'Critical: Input file not found at {input_file}')
    exit()

# Transformation Phase
print('⚙️ Calculating revenue and formatting dates...')
# Calculate total revenue per sale entry
df['total_revenue'] = df['unit_price'] * df['quantity']
# Convert the sale_date column to proper datetime objects
df['sale_date'] = pd.to_datetime(df['sale_date'])
# Sort the DataFrame by sale_date in descending order
df_sorted = df.sort_values(by='sale_date', ascending=False)

# Loading Phase
print('💾 Saving report to processed data folder...')
# Create processed folder if it doesn't already exist
output_file.parent.mkdir(parents=True, exist_ok=True)
# Export final report without indices
df_sorted.to_excel(output_file, index=False)

print(f'✅ ETL complete! Sales report saved to: {output_file}')
