"""
Data processing functions - OPTIMIZED VERSION
Fixed DataFrame fragmentation and improved performance
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Cache for week lists
_week_list_cache = {}

def process_demand_data(df):
    """Process demand forecast data and convert monthly to weekly - OPTIMIZED"""
    if df.empty:
        logger.warning("Empty demand data received")
        return pd.DataFrame()
    
    logger.info(f"Processing demand data: {len(df)} rows")
    
    # Create ref column - vectorized
    df['ref'] = np.where(
        (df['asin'].notna()) & (df['asin'] != ''),
        df['asin'].astype(str) + df['mp'].astype(str),
        df['razin'].astype(str) + df['mp'].astype(str)
    )
    
    # Pivot data
    df_pivoted = df.pivot(index='ref', columns='date', values='quantity').fillna(0)
    df_pivoted = df_pivoted.reset_index()
    
    # OPTIMIZATION: Pre-generate all weeks to avoid fragmentation
    all_weeks = generate_week_list()
    
    # Pre-allocate DataFrame with all columns
    dim_demand = pd.DataFrame(index=df_pivoted.index)
    dim_demand['ref'] = df_pivoted['ref']
    
    # Initialize all week columns at once to avoid fragmentation
    week_columns = {week: 0.0 for week in all_weeks}
    week_df = pd.DataFrame(week_columns, index=dim_demand.index)
    dim_demand = pd.concat([dim_demand, week_df], axis=1)
    
    # Process each month and update the pre-allocated columns
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
        except Exception as e:
            logger.warning(f"Error processing month {col}: {str(e)}")
            continue
    
    return dim_demand.set_index('ref')

def distribute_monthly_to_weekly(monthly_quantities, first_day):
    """Convert monthly quantities to weekly distribution - OPTIMIZED"""
    month_end = first_day + pd.offsets.MonthEnd(0)
    all_days = pd.date_range(start=first_day, end=month_end, freq='D')
    
    # OPTIMIZATION: Vectorized week mapping
    week_map = {}
    for day in all_days:
        iso_year, iso_week, _ = day.isocalendar()
        week_col = f"CW{iso_week:02d}-{iso_year}_demand"
        if week_col not in week_map:
            week_map[week_col] = []
        week_map[week_col].append(day)
    
    total_days = len(all_days)
    
    # OPTIMIZATION: Vectorized calculation
    weekly_quantities = {}
    for week_col, days in week_map.items():
        proportion = len(days) / total_days
        weekly_quantities[week_col] = (monthly_quantities * proportion).round(0)
    
    return weekly_quantities

def generate_week_list():
    """Generate list of weeks for next 2 years - OPTIMIZED with caching"""
    global _week_list_cache
    
    # Use cached version if available
    cache_key = datetime.now().strftime('%Y-%W')
    if cache_key in _week_list_cache:
        return _week_list_cache[cache_key]
    
    today = datetime.now()
    end_date = datetime(today.year + 2, 12, 31)
    weeks = []
    current = today
    
    while current <= end_date:
        iso_year, iso_week, _ = current.isocalendar()
        if iso_week != 53:  # exclude CW53
            weeks.append(f"CW{iso_week:02d}-{iso_year}_demand")
        current += timedelta(days=7)
    
    # Cache the result
    _week_list_cache[cache_key] = weeks
    
    return weeks

def process_inventory_data(df):
    """Process inventory data - OPTIMIZED"""
    if df.empty:
        logger.warning("Empty inventory data received")
        return pd.DataFrame()
    
    logger.info(f"Processing inventory data: {len(df)} rows")
    
    # Standardize marketplace - vectorized
    df['mp'] = df['mp'].replace('Pan-EU', 'EU')
    
    # Create ref column - vectorized
    df['ref'] = np.where(
        (df['asin'].isna()) | (df['asin'].astype(str).str.strip() == ''),
        df['mp'].astype(str),
        df['asin'].astype(str) + df['mp'].astype(str)
    )
    
    # Calculate inventory positions - vectorized
    df['total_inventory'] = (
        df['total_inventory'] - 
        df.get('in_walmart', 0).fillna(0) - 
        df.get('in_to_osc_l3m', 0).fillna(0) - 
        df.get('in_fm', 0).fillna(0) - 
        df.get('units_in_d2amz', 0).fillna(0)
    )
    
    return df.set_index('ref')

def process_open_po_data(df_po, df_master_data):
    """Process open PO data and split into signed/unsigned - OPTIMIZED"""
    
    if df_po.empty:
        return pd.DataFrame(), pd.DataFrame()
    
    logger.info(f"Processing open PO data: {len(df_po)} rows")
    
    # Merge with master data
    df_po = df_po.merge(df_master_data, on='razin', how='left')
    
    # Update ASIN - vectorized
    df_po['asin'] = df_po['asin'].combine_first(df_po.get('asin_master', pd.Series()))
    df_po['asin'] = df_po['asin'].fillna(df_po['razin'])
    
    # Create ref column - vectorized
    df_po['ref'] = df_po['asin'].astype(str) + df_po['mp'].astype(str)
    
    # Calculate expected delivery dates
    df_po = calculate_expected_dates_optimized(df_po)
    
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
    
    # Filter based on current status - vectorized
    dim_open_po_signed = df_po[df_po['current status'].isin(signed_stages)]
    dim_open_po_unsigned = df_po[df_po['current status'].isin(unsigned_stages)]
    
    # Pivot by week
    dim_open_po_signed = pivot_by_week_optimized(dim_open_po_signed, 'open_po_signed')
    dim_open_po_unsigned = pivot_by_week_optimized(dim_open_po_unsigned, 'open_po_unsigned')
    
    return dim_open_po_signed, dim_open_po_unsigned

def calculate_expected_dates(df):
    """Calculate expected delivery dates for POs - ORIGINAL"""
    
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

def calculate_expected_dates_optimized(df):
    """Calculate expected delivery dates for POs - OPTIMIZED"""
    
    # Transport mapping - could be loaded from config
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
    
    # OPTIMIZATION: Vectorized lead time calculation
    df['shipping_region'] = df.get('shipping_region', 'CN').fillna('CN')
    df['mp'] = df.get('mp', 'US').fillna('US')
    df['wh_type'] = df.get('wh_type', '3PL').fillna('3PL')
    
    # Create tuple columns for mapping
    df['transport_key'] = list(zip(df['shipping_region'], df['mp']))
    df['p2pbf_key'] = list(zip(df['wh_type'], df['mp']))
    
    # Map values - vectorized
    df['p2plt_non_air'] = df['transport_key'].map(transport_map).fillna(39)
    df['p2pbf'] = df['p2pbf_key'].map(p2pbf_map).fillna(39)
    
    # Calculate dates - vectorized
    df['crd'] = pd.to_datetime(df['crd'], errors='coerce')
    today = pd.Timestamp.today()
    df['crd'] = df['crd'].where(df['crd'] >= today, today)
    
    df['expected_delivery_date'] = (
        df['crd'] + 
        pd.to_timedelta(df['p2plt_non_air'], unit='D') +
        pd.to_timedelta(df['p2pbf'], unit='D')
    )
    
    # Extract week - vectorized
    df['cw'] = df['expected_delivery_date'].dt.strftime('CW%V-%G')
    
    # Clean up temporary columns
    df = df.drop(columns=['transport_key', 'p2pbf_key'], errors='ignore')
    
    return df

def pivot_by_week(df, suffix):
    """Pivot data by calendar week - ORIGINAL"""
    if df.empty:
        return pd.DataFrame()
    
    # Group by ref and week
    grouped = df.groupby(['ref', 'cw'])['leftover_quantity'].sum().reset_index()
    
    # Add suffix to week
    grouped['cw'] = grouped['cw'] + f'_{suffix}'
    
    # Pivot
    pivoted = grouped.pivot(index='ref', columns='cw', values='leftover_quantity').fillna(0)
    
    return pivoted

def pivot_by_week_optimized(df, suffix):
    """Pivot data by calendar week - OPTIMIZED"""
    if df.empty:
        return pd.DataFrame()
    
    # Group by ref and week - already efficient
    grouped = df.groupby(['ref', 'cw'])['leftover_quantity'].sum().reset_index()
    
    # Add suffix to week
    grouped['cw'] = grouped['cw'] + f'_{suffix}'
    
    # Pivot - efficient operation
    pivoted = grouped.pivot(index='ref', columns='cw', values='leftover_quantity').fillna(0)
    
    return pivoted

def process_inbound_data(df):
    """Process inbound shipment data - OPTIMIZED"""
    
    if df.empty:
        logger.warning("Empty inbound data received")
        return pd.DataFrame()
    
    logger.info(f"Processing inbound data: {len(df)} rows")
    
    # Create ref column - vectorized
    df['ref'] = np.where(
        (df['asin'].notna()) & (df['asin'].astype(str).str.strip() != ''),
        df['asin'].astype(str) + df['mkt_place'].astype(str),
        df['razin'].astype(str) + df['mkt_place'].astype(str)
    )
    
    # Calculate expected delivery date with fallbacks
    df = calculate_inbound_dates(df)
    
    # Extract week - vectorized
    df['cw'] = df['expected_delivery_date_final'].dt.strftime('CW%V-%G_inbound')
    
    # Group by ref and cw, sum quantities to handle duplicates
    grouped = df.groupby(['ref', 'cw'], as_index=False).agg({
        'quantity': 'sum'
    })
    
    # Now pivot without duplicates
    pivoted = grouped.pivot(index='ref', columns='cw', values='quantity').fillna(0)
    
    return pivoted

def calculate_inbound_dates(df):
    """Calculate expected delivery dates for inbound shipments - FIXED"""
    
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
    default_value = today + pd.Timedelta(days=55)
    
    # Initialize with default value
    df['expected_delivery_date_final'] = default_value
    
    # Update with actual dates if available
    if 'expected_delivery_date' in df.columns:
        mask = df['expected_delivery_date'].notna()
        df.loc[mask, 'expected_delivery_date_final'] = df.loc[mask, 'expected_delivery_date']
    elif 'actual_arrival_date' in df.columns:
        mask = df['actual_arrival_date'].notna()
        df.loc[mask, 'expected_delivery_date_final'] = (
            df.loc[mask, 'actual_arrival_date'] + pd.to_timedelta(df.loc[mask, 'p2cbf'], unit='D')
        )
    elif 'movement_date' in df.columns:
        mask = df['movement_date'].notna()
        df.loc[mask, 'expected_delivery_date_final'] = (
            df.loc[mask, 'movement_date'] + 
            pd.to_timedelta(df.loc[mask, 'p2cbf'] + df.loc[mask, 'transport_leadtime'], unit='D')
        )
    elif 'final_crd' in df.columns:
        mask = df['final_crd'].notna()
        df.loc[mask, 'expected_delivery_date_final'] = (
            df.loc[mask, 'final_crd'] + 
            pd.to_timedelta(df.loc[mask, 'p2cbf'] + df.loc[mask, 'transport_leadtime'] + 12, unit='D')
        )
    
    # Ensure all values are timestamps
    df['expected_delivery_date_final'] = pd.to_datetime(df['expected_delivery_date_final'])
    
    # Replace past dates with today + 7
    past_mask = df['expected_delivery_date_final'] < today
    df.loc[past_mask, 'expected_delivery_date_final'] = today + pd.Timedelta(days=7)
    
    return df

def process_master_data(df):
    """Process master data"""
    
    if df.empty:
        return pd.DataFrame()
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['razin', 'asin', 'size_tier'])
    
    # Create asin_razin column - vectorized
    df['asin_razin'] = df['asin'].replace(['', None], pd.NA).fillna(df['razin'])
    
    return df

def process_vendor_data(df):
    """Process vendor data"""
    
    if df.empty:
        return pd.DataFrame()
    
    # Basic processing
    df = df.drop_duplicates(subset=['vendor_id'])
    
    return df.set_index('vendor_id')

def process_gfl_data(df):
    """Process GFL (Go Forward List) data - OPTIMIZED"""
    
    if df.empty:
        return pd.DataFrame()
    
    # Create ref column and set GFL flag - vectorized
    df['mp'] = df['marketplace'].replace('Pan-EU', 'EU')
    df['ref'] = df['asin'].astype(str) + df['mp'].astype(str)
    df['gfl_list'] = 'Yes'
    
    # Keep only relevant columns
    df = df[['ref', 'gfl_list']].drop_duplicates(subset='ref', keep='first')
    
    return df.set_index('ref')
