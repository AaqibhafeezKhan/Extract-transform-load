import pandas as pd
import os
from datetime import datetime
from pathlib import Path

"""
ETL - Customer Analysis with Segmentation

This script performs an ETL (Extract, Transform, Load) process from a CSV file 
containing customer data.

Steps:
1. Extraction: Load data from 'customers.csv'.
2. Transformation:
   - Fill missing age values with the mean.
   - Convert date strings to datetime objects.
   - Classify customers into segments based on total purchases:
        VIP → ≥ 4,000
        Frequent → 2,000 - 3,999
        Occasional → < 2,000
   - Calculate days since last purchase.
   - Sort customers by total purchases in descending order.
3. Loading: Export the processed data to an Excel file named 'customer_segmentation_report.xlsx'.
"""

# Extraction phase
def extract_data(filepath):
    try:
        data = pd.read_csv(filepath)
        return data
    except Exception as e:
        print(f'Extraction failed! Error: {e}')
        return None

# Transformation phase
def transform_data(df):
    # Fill null ages with the mean
    df['age'] = df['age'].fillna(df['age'].mean())
    # Convert 'last_purchase' to datetime
    df['last_purchase'] = pd.to_datetime(df['last_purchase'])

    # Customer segmentation logic
    def classify_segment(value):
        if value >= 4000:
            return 'VIP'
        elif value >= 2000:
            return 'Frequent'
        else:
            return 'Occasional'

    df['customer_segment'] = df['total_purchases'].apply(classify_segment)

    # Days since last purchase
    current_date = datetime.today()
    df['days_since_last_purchase'] = (current_date - df['last_purchase']).dt.days

    # Sort by total purchases (descending)
    df_sorted = df.sort_values(by='total_purchases', ascending=False)

    return df_sorted

# Loading phase
def load_to_excel(df, output_path):
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(output_path, index=False)
        print(f'Report saved successfully at: {output_path}')
    except Exception as e:
        print(f'Loading failed! Error: {e}')

def run_pipeline():
    # Base path relative to this script
    base_dir = Path(__file__).parent
    input_file = base_dir / 'data' / 'raw' / 'customers.csv'
    output_file = base_dir / 'data' / 'processed' / 'customer_segmentation_report.xlsx'

    print('🚀 Starting Customer ETL...')
    df = extract_data(input_file)
    if df is not None:
        print('⚙️ Processing segments and metrics...')
        df_transformed = transform_data(df)
        print('💾 Generating Excel report...')
        load_to_excel(df_transformed, output_file)
        print('✅ Customer ETL complete!')

if __name__ == "__main__":
    run_pipeline()
