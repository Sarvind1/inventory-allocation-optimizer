"""
Utility functions
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

def create_output_folder(base_dir="output"):
    """Create output folder for today's run"""
    
    base_path = Path(base_dir)
    base_path.mkdir(exist_ok=True)
    
    today_folder = base_path / datetime.now().strftime('%Y-%m-%d')
    today_folder.mkdir(exist_ok=True)
    
    return today_folder

def save_debug_files(output_folder, dataframes_dict):
    """Save debug CSV files"""
    
    debug_folder = output_folder / "debug"
    debug_folder.mkdir(exist_ok=True)
    
    for name, df in dataframes_dict.items():
        if df is not None and not df.empty:
            file_path = debug_folder / f"{name}.csv"
            df.to_csv(file_path)
            print(f"  Saved debug file: {name}.csv")

def format_number(num):
    """Format number with commas"""
    return f"{num:,.0f}"

def calculate_week_difference(week_str1, week_str2):
    """Calculate difference between two CW strings"""
    
    def parse_week(week_str):
        if not week_str or not isinstance(week_str, str):
            return None, None
        try:
            week = int(week_str[2:4])
            year = int(week_str[-4:])
            return year, week
        except:
            return None, None
    
    year1, week1 = parse_week(week_str1)
    year2, week2 = parse_week(week_str2)
    
    if None in (year1, week1, year2, week2):
        return 0
    
    return (year2 - year1) * 52 + (week2 - week1)

def is_week_after(week_str1, week_str2):
    """Check if week1 is after week2"""
    return calculate_week_difference(week_str2, week_str1) > 0

def standardize_marketplace(df, col_name='marketplace'):
    """Standardize marketplace codes"""
    
    mappings = {
        'Pan-EU': 'EU',
        'DE': 'EU',
        'GB': 'UK',
        'North America': 'US'
    }
    
    df = df.copy()
    for old, new in mappings.items():
        df[col_name] = df[col_name].replace(old, new)
    
    return df

def create_ref_column(df, asin_col='asin', razin_col='razin', mp_col='mp'):
    """Create standardized ref column"""
    
    df = df.copy()
    df['ref'] = df.apply(
        lambda row: (row[asin_col] if pd.notna(row[asin_col]) and str(row[asin_col]).strip() 
                    else row[razin_col]) + row[mp_col],
        axis=1
    )
    
    return df

def safe_divide(numerator, denominator, default=0):
    """Safe division with default value for division by zero"""
    return numerator / denominator if denominator != 0 else default

def get_current_cw():
    """Get current calendar week string"""
    today = datetime.now()
    year, week, _ = today.isocalendar()
    return f"CW{week:02d}-{year}"

def validate_data(df, required_columns, df_name="DataFrame"):
    """Validate that required columns exist in dataframe"""
    
    missing = [col for col in required_columns if col not in df.columns]
    
    if missing:
        print(f"Warning: {df_name} missing columns: {missing}")
        return False
    
    return True

def clean_numeric_column(df, column_name, default_value=0):
    """Clean and convert column to numeric"""
    
    df = df.copy()
    df[column_name] = pd.to_numeric(df[column_name], errors='coerce').fillna(default_value)
    
    return df

def merge_with_logging(left_df, right_df, on, how='left', df_names=('left', 'right')):
    """Merge dataframes with logging of row counts"""
    
    initial_rows = len(left_df)
    result = left_df.merge(right_df, on=on, how=how)
    final_rows = len(result)
    
    if final_rows != initial_rows:
        print(f"  Merge {df_names[0]} with {df_names[1]}: {initial_rows} → {final_rows} rows")
    
    return result