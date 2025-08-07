"""
Data processing functions
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def process_demand_data(df):
    """Process demand forecast data and convert to weekly"""
    if df.empty:
        return pd.DataFrame()
    
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
        if iso_week != 53:
            weeks.append(f"CW{iso_week:02d}-{iso_year}_demand")
        current += timedelta(days=7)
    
    return weeks

def process_inventory_data(df):
    """Process inventory data"""
    if df.empty:
        return pd.DataFrame()
    
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

def process_open_po_data(df_po, df_otif, dim_master_data, dim_product_market, config):
    """Process open PO data and split into signed/unsigned"""
    
    if df_po.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    # Merge with master data
    df_po = df_po.merge(dim_master_data, on='razin', how='left')
    
    # Update ASIN
    df_po['asin'] = df_po['asin'].combine_first(df_po.get('asin_master', pd.Series()))
    df_po['asin'] = df_po['asin'].fillna(df_po['razin'])
    
    # Create ref column
    df_po['ref'] = df_po['asin'] + df_po['mp']
    
    # Merge with vendor info
    df_po['vendor_name_short'] = df_po['vendor_name'].str[:5]
    df_po = df_po.merge(dim_product_market, 
                       left_on='vendor_name_short', 
                       right_on='vendor_id', 
                       how='left')
    
    # Merge with OTIF status
    if not df_otif.empty:
        df_po['link'] = df_po['po#'] + df_po['line_id'].astype(str)
        df_otif['link'] = df_otif['document number'] + df_otif['line id'].astype(str)
        df_po = df_po.merge(df_otif, on='link', how='left')
    
    # Calculate expected delivery dates
    df_po = calculate_expected_dates(df_po, config)
    
    # Split into signed and unsigned
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
    
    dim_open_po_signed = df_po[df_po['current status'].isin(signed_stages)]
    dim_open_po_unsigned = df_po[df_po['current status'].isin(unsigned_stages)]
    
    # Pivot by week
    dim_open_po_signed = pivot_by_week(dim_open_po_signed, 'open_po_signed')
    dim_open_po_unsigned = pivot_by_week(dim_open_po_unsigned, 'open_po_unsigned')
    
    return dim_open_po_signed, dim_open_po_unsigned

def calculate_expected_dates(df, config):
    """Calculate expected delivery dates for POs"""
    
    # Load lead time mappings
    transport_map = config.get('transport_map', {})
    p2pbf_map = config.get('p2pbf_map', {})
    
    # Calculate lead times
    df['region_mp'] = df['shipping_region'].astype(str) + df['mp'].astype(str)
    df['p2plt_non_air'] = df['region_mp'].map(transport_map).fillna(39)
    df['p2pbf'] = df.apply(
        lambda row: p2pbf_map.get((row['wh_type'], row['mp']), 39),
        axis=1
    )
    
    # Calculate dates
    df['crd'] = pd.to_datetime(df['crd'], format='%d/%m/%Y', errors='coerce')
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

def process_inbound_data(df, dim_product_market, config):
    """Process inbound shipment data"""
    
    if df.empty:
        return pd.DataFrame()
    
    # Create ref column
    df['ref'] = df.apply(
        lambda row: str(row['asin']) + str(row['mkt_place']) 
        if pd.notna(row['asin']) and str(row['asin']).strip() != ''
        else str(row['razin']) + str(row['mkt_place']),
        axis=1
    )
    
    # Merge with vendor info
    df['vendor_prefix'] = df['vendor_name'].str[:5]
    df = df.merge(
        dim_product_market[['vendor_id', 'shipping_region']],
        left_on='vendor_prefix',
        right_on='vendor_id',
        how='left'
    )
    
    # Calculate expected delivery date
    df = calculate_inbound_dates(df, config)
    
    # Extract week
    df['cw'] = df['expected_delivery_date_final'].dt.strftime('CW%V-%G_inbound')
    
    # Pivot by week
    pivoted = df.pivot(index='ref', columns='cw', values='quantity').fillna(0)
    
    return pivoted

def calculate_inbound_dates(df, config):
    """Calculate expected delivery dates for inbound shipments"""
    
    # Convert date columns
    date_cols = ['expected_delivery_date', 'actual_arrival_date', 'movement_date', 'final_crd']
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Get lead times from config
    transport_map = config.get('transport_map', {})
    
    # Calculate transport lead time
    df['transport_leadtime'] = df.apply(
        lambda row: transport_map.get((row['shipping_region'], row['mkt_place']), 45),
        axis=1
    )
    
    # Calculate p2cbf
    df['p2cbf'] = df.apply(
        lambda row: 0 if row['mkt_place'] == row['shipping_region'] 
                     else config.get('mp_mapping', {}).get(row['mkt_place'], 39),
        axis=1
    )
    
    # Calculate final expected date with fallbacks
    today = pd.Timestamp.today()
    
    conditions = [
        df['expected_delivery_date'].notna(),
        df['actual_arrival_date'].notna(),
        df['movement_date'].notna(),
        df['final_crd'].notna()
    ]
    
    choices = [
        df['expected_delivery_date'],
        df['actual_arrival_date'] + pd.to_timedelta(df['p2cbf'], unit='D'),
        df['movement_date'] + pd.to_timedelta(df['p2cbf'] + df['transport_leadtime'], unit='D'),
        df['final_crd'] + pd.to_timedelta(df['p2cbf'] + df['transport_leadtime'] + 12, unit='D')
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