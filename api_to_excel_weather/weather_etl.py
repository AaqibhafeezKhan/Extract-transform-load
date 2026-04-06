import requests
import pandas as pd
import numpy as np
import os
from time import sleep
from pathlib import Path

'''
ETL - Weather Forecast with Classification and Export

This script performs an ETL (Extract, Transform, Load) process on meteorological data 
provided by a public API (Open-Meteo).

Steps:
1. Extraction: Fetch hourly forecast data for the next 7 days in São Paulo.
2. Transformation:
   - Convert timestamps to local timezone (America/Sao_Paulo).
   - Filter to keep only the current week's data.
   - Classify weather into three categories based on temperature:
        Cold → < 15°C  
        Mild → 15°C - 25°C  
        Hot → > 25°C
   - Classify time of day:
        Morning (6h-12h), Afternoon (12h-18h), Evening (18h-23h), and Overnight (others).
   - Split date and time columns for readability.
3. Loading: Export the transformed data into an Excel file named `weather_forecast.xlsx`.
'''

# Extraction Phase
def extract_data(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        json_data = response.json()
        data = json_data['hourly']
        return data
    except Exception as e:
        print(f'Extraction failed! Error: {e}')
        return None

# Transformation: Dataframe creation
def create_dataframe(data):
    # Rename columns to English
    df = pd.DataFrame({
        'timestamp': data['time'],
        'temperature': data['temperature_2m'],
        'relative_humidity': data['relative_humidity_2m'],
        'wind_speed': data['windspeed_10m']
    })

    df.set_index('timestamp', inplace=True)
    df.index = pd.to_datetime(df.index)
    # Localize from UTC to São Paulo time
    df.index = df.index.tz_localize('UTC').tz_convert('America/Sao_Paulo')

    # Filter for the next 7 days
    today = pd.Timestamp.now(tz='America/Sao_Paulo').normalize()
    end_date = today + pd.Timedelta(days=7)
    df = df.loc[(df.index >= today) & (df.index < end_date)]

    return df

# Transformation: Feature classification
def add_classification_columns(df):
    # Weather classification
    weather_conditions = [
        df['temperature'] < 15, 
        (df['temperature'] >= 15) & (df['temperature'] <= 25)
    ]
    weather_labels = ['Cold', 'Mild']
    df['weather_type'] = np.select(weather_conditions, weather_labels, default='Hot')

    # Time of day classification
    day_conditions = [
        (df.index.hour >= 6) & (df.index.hour < 12),
        (df.index.hour >= 12) & (df.index.hour < 18),
        (df.index.hour >= 18) & (df.index.hour < 23)
    ]
    day_labels = ['Morning', 'Afternoon', 'Evening']
    df['day_period'] = np.select(day_conditions, day_labels, default='Overnight')

    # Format date and time
    df['date'] = df.index.strftime('%Y-%m-%d')
    df['time'] = df.index.strftime('%H:%M:%S')

    return df

# Helper to reorder columns
def organize_columns(df):
    ordered_cols = ['date', 'time'] + [col for col in df.columns if col not in ['date', 'time']]
    return df[ordered_cols]

# Loading phase: Export to Excel
def export_to_excel(filename, directory, df):
    try:
        output_path = Path(directory) / filename
        # Ensure directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Remove timezone for Excel compatibility
        df_export = df.copy()
        df_export.index = df_export.index.tz_localize(None)
        
        df_export.to_excel(output_path, index=True)
        print(f'Data exported successfully to "{output_path}"!')
    except Exception as e:
        print(f'Export failed! Error: {e}')

def run_pipeline():
    api_url = "https://api.open-meteo.com/v1/forecast?latitude=-23.55&longitude=-46.63&hourly=temperature_2m,relative_humidity_2m,windspeed_10m"
    
    print('🚀 Extracting weather data...')
    raw_data = extract_data(api_url)
    if raw_data is None:
        print("Pipeline aborted due to extraction failure.")
        return

    print('⚙️ Transforming data...')
    df = create_dataframe(raw_data)
    df = add_classification_columns(df)
    df_final = organize_columns(df)
    sleep(1)

    print('💾 Saving output...')
    # Use relative path by default
    current_dir = Path(__file__).parent
    output_filename = 'weather_forecast.xlsx'
    
    export_to_excel(output_filename, current_dir, df_final)
    print('✅ Done!')

if __name__ == "__main__":
    run_pipeline()
