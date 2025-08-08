"""
Data processing functions - Fixed version
Contains all missing processing functions from the notebook
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def process_demand_data(df):
    """Process demand forecast data and convert monthly to weekly"""
    if df.empty:
        logger.warning("Empty demand data received")
        return pd.DataFrame()
    
    logger.info(f"Processing demand data: {len(df)} rows")
    
    # Create ref column
    df['ref'] = df.apply(
        lambda row: (row['asin'] if pd.notna(row['asin']) and row['asin'] != '' 
                    else row['razin']) + row['mp'],
        axis=1
    )
    
    # Pivot data
    df_pivoted = df.pivot(index='ref', columns='date', values='quantity').fillna(0)
    df_pivoted = df_pivoted.reset_index()
    
    # Convert monthly to weekly
    dim_demand = pd.DataFrame()
    dim_demand['ref'] = df_pivoted['ref']
    
    for col in df_pivoted.columns[1:]:
        try:
            month_date = pd.to_datetime(col)
            first_day = pd.Timestamp(year=month_date.year, month=month_date.month, day=1)
            month_quantities = df_pivoted[col]
            
            # Distribute to weeks
            weekly_dist = distribute_monthly_to_weekly(month_quantities, first_day)
            for week_col, values in weekly_dist.items():
                if week_col in dim_demand.columns:
                    dim_demand[week_col] += values
                else:
                    dim_demand[week_col] = values
        except Exception as e:
            logger.warning(f"Error processing month {col}: {str(e)}")
            continue
    
    # Fill missing weeks
    all_weeks = generate_week_list()
    for week in all_weeks:
        if week not in dim_demand.columns:
            dim_demand[week] = 0
    
    return dim_demand.set_index('ref')

def distribute_monthly_to_weekly(monthly_quantities, first_day):
    """Convert monthly quantities to weekly distribution"""
    month_end = first_day + pd.offsets.MonthEnd(0)
    all_days = pd.date_range(start=first_day, end=month_end, freq='D')
    
    week_map = {}
    for day in all_days:
        iso_year, iso_week, _ = day.isocalendar()
        week_col = f"CW{iso_week:02d}-{iso_year}_demand"
        if week_col not in week_map:
            week_map[week_col] = []
        week_map[week_col].append(day)
    
    total_days = len(all_days)
    weekly_quantities = {
        week_col: (monthly_quantities * len(days) / total_days).round(0)
        for week_col, days in week_map.items()
    }
    
    return weekly_quantities

def generate_week_list():
    """Generate list of weeks for next 2 years"""
    today = datetime.now()
    end_date = datetime(today.year + 2, 12, 31)
    weeks = []
    current = today
    
    while current <= end_date:
        iso_year, iso_week, _ = current.isocalendar()
        if iso_week != 53:  # exclude CW53
            weeks.append(f"CW{iso_week:02d}-{iso_year}_demand")
        current += timedelta(days=7)
    
    return weeks

def process_inventory_data(df):
    """Process inventory data"""
    if df.empty:
        logger.warning("Empty inventory data received")
        return pd.DataFrame()
    
    logger.info(f"Processing inventory data: {len(df)} rows")
    
    # Standardize marketplace
    df['mp'] = df['marketplace'].replace('Pan-EU', 'EU')
    
    # Create ref column
    df['ref'] = df.apply(
        lambda row: row['mp'] if pd.isna(row['asin']) or str(row['asin']).strip() == '' 
                   else row['asin'] + row['mp'],
        axis=1
    )
    
    # Calculate inventory positions
    df['total_inventory'] = (
        df['total_inventory'] - df.get('in_walmart', 0) - 
        df.get('in_to_osc_l3m', 0) - df.get('in_fm', 0) - 
        df.get('units_in_d2amz', 0)
    )
    
    return df.set_index('ref')

def process_open_po_data(df_po, df_master_data):
    """Process open PO data and split into signed/unsigned"""
    
    if df_po.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    logger.info(f"Processing open PO data: {len(df_po)} rows")
    
    # Merge with master data
    df_po = df_po.merge(df_master_data, on='razin', how='left')
    
    # Update ASIN
    df_po['asin'] = df_po['asin'].combine_first(df_po.get('asin_master', pd.Series()))
    df_po['asin'] = df_po['asin'].fillna(df_po['razin'])
    
    # Create ref column
    df_po['ref'] = df_po['asin'] + df_po['mp']
    
    # Calculate expected delivery dates (simplified version)
    df_po = calculate_expected_dates(df_po)
    
    # Split into signed and unsigned based on status stages
    signed_stages = [
        '12. Ready for Batching Pending', '13. Batch Creation Pending',
        '14. SM Sign-Off Pending', '15. CI Approval Pending',
        '16. CI Payment Pending', '17. QC Schedule Pending',
        '18. FFW Booking Missing', '19. Supplier Pickup Date Pending',
        '20. Pre Pickup Check', '21. FOB Pickup Pending',
        '22. Non FOB Pickup Pending', '23. INB Creation Pending'
    ]
    
    unsigned_stages = [
        '01. PO Approval Pending', '02. Supplier Confirmation Pending',
        '03. PI Upload Pending', '04. PI Approval Pending',
        '05. PI Payment Pending', '06. Packaging Pending',
        '07. Transperancy Label Pending', '08. PRD Pending',
        '09. Under Production', '10. PRD Confirmation Pending',
        '11. IM Sign-Off Pending', 'A. Anti PO Line', 'B. Compliance Blocked'
    ]
    
    # Filter based on current status
    dim_open_po_signed = df_po[df_po['current status'].isin(signed_stages)]
    dim_open_po_unsigned = df_po[df_po['current status'].isin(unsigned_stages)]
    
    # Pivot by week
    dim_open_po_signed = pivot_by_week(dim_open_po_signed, 'open_po_signed')
    dim_open_po_unsigned = pivot_by_week(dim_open_po_unsigned, 'open_po_unsigned')
    
    return dim_open_po_signed, dim_open_po_unsigned

def calculate_expected_dates(df):
    """Calculate expected delivery dates for POs"""
    
    # Load transport mapping (hardcoded for now, can be moved to config)
    transport_map = {
        ('CN', 'US'): 39, ('CN', 'EU'): 42, ('CN', 'UK'): 34,
        ('IN', 'US'): 45, ('IN', 'EU'): 33, ('IN', 'UK'): 26,
        ('EU', 'US'): 40, ('UK', 'US'): 36, ('US', 'UK'): 52,
        ('US', 'EU'): 20
    }
    
    p2pbf_map = {
        ('3PL', 'US'): 39, ('3PL', 'EU'): 40, ('3PL', 'UK'): 39,
        ('AMZ', 'US'): 25, ('AMZ', 'EU'): 26, ('AMZ', 'UK'): 22
    }
    
    # Calculate lead times
    df['p2plt_non_air'] = df.apply(
        lambda row: transport_map.get((row.get('shipping_region', 'CN'), row.get('mp', 'US')), 39),
        axis=1
    )
    df['p2pbf'] = df.apply(
        lambda row: p2pbf_map.get((row.get('wh_type', '3PL'), row.get('mp', 'US')), 39),
        axis=1
    )
    
    # Calculate dates
    df['crd'] = pd.to_datetime(df['crd'], errors='coerce')
    today = pd.Timestamp.today()
    df.loc[df['crd'] < today, 'crd'] = today
    
    df['expected_delivery_date'] = (
        df['crd'] + 
        pd.to_timedelta(df['p2plt_non_air'], unit='D') +
        pd.to_timedelta(df['p2pbf'], unit='D')
    )
    
    # Extract week
    df['cw'] = df['expected_delivery_date'].dt.strftime('CW%V-%G')
    
    return df

def pivot_by_week(df, suffix):
    """Pivot data by calendar week"""
    if df.empty:
        return pd.DataFrame()
    
    # Group by ref and week
    grouped = df.groupby(['ref', 'cw'])['leftover_quantity'].sum().reset_index()
    
    # Add suffix to week
    grouped['cw'] = grouped['cw'] + f'_{suffix}'
    
    # Pivot
    pivoted = grouped.pivot(index='ref', columns='cw', values='leftover_quantity').fillna(0)
    
    return pivoted

def process_inbound_data(df):
    """Process inbound shipment data"""
    
    if df.empty:
        logger.warning("Empty inbound data received")
        return pd.DataFrame()
    
    logger.info(f"Processing inbound data: {len(df)} rows")
    
    # Create ref column
    df['ref'] = df.apply(
        lambda row: str(row['asin']) + str(row['mkt_place']) 
        if pd.notna(row['asin']) and str(row['asin']).strip() != ''
        else str(row['razin']) + str(row['mkt_place']),
        axis=1
    )
    
    # Calculate expected delivery date with fallbacks
    df = calculate_inbound_dates(df)
    
    # Extract week
    df['cw'] = df['expected_delivery_date_final'].dt.strftime('CW%V-%G_inbound')
    
    # Pivot by week
    pivoted = df.pivot(index='ref', columns='cw', values='quantity').fillna(0)
    
    return pivoted

def calculate_inbound_dates(df):
    """Calculate expected delivery dates for inbound shipments"""
    
    # Convert date columns
    date_cols = ['expected_delivery_date', 'actual_arrival_date', 'movement_date', 'final_crd']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Transport mapping
    transport_map = {
        ('CN', 'US'): 39, ('CN', 'EU'): 42, ('CN', 'UK'): 34,
        ('IN', 'US'): 45, ('IN', 'EU'): 33, ('IN', 'UK'): 26
    }
    
    # Calculate transport lead time
    df['transport_leadtime'] = df.apply(
        lambda row: transport_map.get((row.get('shipping_region', 'CN'), row.get('mkt_place', 'US')), 45),
        axis=1
    )
    
    # Calculate p2cbf
    mp_mapping = {
        'US': 39, 'CO': 39, 'MX': 39, 'CA': 39,
        'UK': 39, 'BR': 36, 'EU': 40, 'Other': 39
    }
    
    df['p2cbf'] = df.apply(
        lambda row: 0 if row.get('mkt_place') == row.get('shipping_region') 
                     else mp_mapping.get(row.get('mkt_place'), 39),
        axis=1
    )
    
    # Calculate final expected date with fallbacks
    today = pd.Timestamp.today()
    
    conditions = [
        df.get('expected_delivery_date', pd.Series()).notna(),
        df.get('actual_arrival_date', pd.Series()).notna(),
        df.get('movement_date', pd.Series()).notna(),
        df.get('final_crd', pd.Series()).notna()
    ]
    
    choices = [
        df.get('expected_delivery_date', pd.Series()),
        df.get('actual_arrival_date', pd.Series()) + pd.to_timedelta(df['p2cbf'], unit='D'),
        df.get('movement_date', pd.Series()) + pd.to_timedelta(df['p2cbf'] + df['transport_leadtime'], unit='D'),
        df.get('final_crd', pd.Series()) + pd.to_timedelta(df['p2cbf'] + df['transport_leadtime'] + 12, unit='D')
    ]
    
    default_value = today + pd.Timedelta(days=55)
    df['expected_delivery_date_final'] = np.select(conditions, choices, default=default_value)
    
    # Replace past dates with today + 7
    df.loc[df['expected_delivery_date_final'] < today, 'expected_delivery_date_final'] = today + pd.Timedelta(days=7)
    
    return df

def process_master_data(df):
    """Process master data"""
    
    if df.empty:
        return pd.DataFrame()
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['razin', 'asin', 'size_tier'])
    
    # Create asin_razin column
    df['asin_razin'] = df['asin'].replace(['', None], pd.NA).fillna(df['razin'])
    
    return df

def process_vendor_data(df):
    """Process vendor data"""
    
    if df.empty:
        return pd.DataFrame()
    
    # Basic processing - can be expanded based on requirements
    df = df.drop_duplicates(subset=['vendor_id'])
    
    return df.set_index('vendor_id')

def process_gfl_data(df):
    """Process GFL (Go Forward List) data"""
    
    if df.empty:
        return pd.DataFrame()
    
    # Create ref column and set GFL flag
    df['mp'] = df['marketplace'].replace('Pan-EU', 'EU')
    df['ref'] = df['asin'] + df['mp']
    df['gfl_list'] = 'Yes'
    
    # Keep only relevant columns
    df = df[['ref', 'gfl_list']].drop_duplicates(subset='ref', keep='first')
    
    return df.set_index('ref')
