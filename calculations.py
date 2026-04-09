"""
Calculation functions for inventory allocation - OPTIMIZED VERSION
Fixed critical errors and improved performance
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Cache for week lists to avoid regeneration
_week_cache = {}

def calculate_sales_missed(dim_demand, dim_inventory, dim_open_po_signed, 
                          dim_open_po_unsigned, dim_inbound):
    """Calculate sales missed based on inventory waterfall - OPTIMIZED"""
    
    logger.info("Starting sales missed calculations...")
    
    # Get all unique refs
    all_refs = pd.Index(
        set(dim_demand.index) | set(dim_inventory.index) | 
        set(dim_open_po_signed.index) | set(dim_open_po_unsigned.index) | 
        set(dim_inbound.index)
    )
    
    # Reindex all dataframes to ensure alignment
    dim_demand = dim_demand.reindex(all_refs, fill_value=0)
    dim_inventory_start = dim_inventory.reindex(all_refs)
    dim_inbound = dim_inbound.reindex(all_refs, fill_value=0)
    dim_open_po_signed = dim_open_po_signed.reindex(all_refs, fill_value=0)
    dim_open_po_unsigned = dim_open_po_unsigned.reindex(all_refs, fill_value=0)
    
    # Get week list (use cached version)
    weeks = generate_cw_list()
    
    if not weeks:
        logger.error("No weeks generated")
        return pd.DataFrame(index=all_refs)
    
    # OPTIMIZATION: Pre-allocate all columns at once to avoid fragmentation
    inventory_end_cols = {f"{week}_inventory_end": 0.0 for week in weeks[:104]}
    inventory_start_cols = {f"{week}_inventory_start": 0.0 for week in weeks[:104]}
    sales_missed_cols = {f"{week}_sales_missed_w": 0.0 for week in weeks[:104]}
    
    dim_inventory_end = pd.DataFrame(inventory_end_cols, index=all_refs)
    dim_inventory_start_new = pd.DataFrame(inventory_start_cols, index=all_refs)
    dim_sales_missed = pd.DataFrame(sales_missed_cols, index=all_refs)
    
    # Merge pre-allocated columns with existing inventory start
    for col in dim_inventory_start.columns:
        if col in dim_inventory_start_new.columns:
            dim_inventory_start_new[col] = dim_inventory_start[col]
    dim_inventory_start = dim_inventory_start_new
    
    # Initialize tracking columns
    dim_sales_missed['OOS_week_with_signed'] = None
    dim_sales_missed['OOS_week_with_signed_last'] = None
    
    # First week calculations
    first_week = weeks[0]
    first_week_inventory = f"{first_week}_inventory_start"
    
    # Set initial inventory
    if 'total_inventory' in dim_inventory.columns:
        dim_inventory_start[first_week_inventory] = dim_inventory['total_inventory']
    else:
        dim_inventory_start[first_week_inventory] = 0
    
    # OPTIMIZATION: Vectorized operations for first week
    first_week_demand = dim_demand.get(f'{first_week}_demand', pd.Series(0, index=all_refs))
    first_week_inbound = dim_inbound.get(f'{first_week}_inbound', pd.Series(0, index=all_refs))
    first_week_signed = dim_open_po_signed.get(f'{first_week}_open_po_signed', pd.Series(0, index=all_refs))
    
    dim_inventory_end[f"{first_week}_inventory_end"] = np.maximum(
        dim_inventory_start[first_week_inventory] +
        first_week_inbound - first_week_demand,
        0
    )
    
    dim_sales_missed[f'{first_week}_sales_missed_w'] = np.maximum(
        first_week_demand -
        dim_inventory_start[first_week_inventory] -
        first_week_signed - first_week_inbound,
        0
    )
    
    # Track OOS week
    condition = dim_sales_missed[f'{first_week}_sales_missed_w'] > 0
    dim_sales_missed.loc[condition, 'OOS_week_with_signed'] = first_week
    
    # OPTIMIZATION: Process weeks in batches for better cache locality
    for i in range(1, min(len(weeks), 104)):
        current_week = weeks[i]
        previous_week = weeks[i-1]
        
        current_inventory_start = f'{current_week}_inventory_start'
        current_inventory_end = f'{current_week}_inventory_end'
        current_demand = f'{current_week}_demand'
        current_inbound = f'{current_week}_inbound'
        current_signed = f'{current_week}_open_po_signed'
        current_unsigned = f'{current_week}_open_po_unsigned'
        
        # Get values with vectorized defaults
        demand_val = dim_demand.get(current_demand, pd.Series(0, index=all_refs))
        inbound_val = dim_inbound.get(current_inbound, pd.Series(0, index=all_refs))
        signed_val = dim_open_po_signed.get(current_signed, pd.Series(0, index=all_refs))
        unsigned_val = dim_open_po_unsigned.get(current_unsigned, pd.Series(0, index=all_refs))
        
        # Calculate inventory flow - all vectorized
        dim_inventory_start[current_inventory_start] = dim_inventory_end[f'{previous_week}_inventory_end']
        
        dim_inventory_end[current_inventory_end] = np.maximum(
            dim_inventory_start[current_inventory_start] +
            inbound_val + unsigned_val + signed_val - demand_val,
            0
        )
        
        # Calculate sales missed
        dim_sales_missed[f'{current_week}_sales_missed_w'] = np.maximum(
            demand_val -
            dim_inventory_start[current_inventory_start] -
            signed_val - inbound_val,
            0
        )
        
        # Update OOS tracking - vectorized
        first_occurrence = (
            (dim_sales_missed[f'{current_week}_sales_missed_w'] > 0) & 
            dim_sales_missed['OOS_week_with_signed'].isna()
        )
        dim_sales_missed.loc[first_occurrence, 'OOS_week_with_signed'] = current_week
        
        # Track last OOS week
        prev_sales_missed = f'{previous_week}_sales_missed_w'
        if prev_sales_missed in dim_sales_missed.columns:
            last_occurrence = (
                (dim_sales_missed[prev_sales_missed] == 0) & 
                (dim_sales_missed[f'{current_week}_sales_missed_w'] > 0) & 
                (demand_val > 1)
            )
            dim_sales_missed.loc[last_occurrence, 'OOS_week_with_signed_last'] = current_week
    
    # Fill remaining OOS weeks
    dim_sales_missed['OOS_week_with_signed'] = dim_sales_missed['OOS_week_with_signed'].fillna(weeks[-1])
    dim_sales_missed['OOS_week_with_signed_final'] = dim_sales_missed['OOS_week_with_signed_last'].fillna(
        dim_sales_missed['OOS_week_with_signed']
    )
    
    logger.info("Sales missed calculations completed")
    return dim_sales_missed

def calculate_revenue_impact(dim_sales_missed, dim_target_sp):
    """Calculate revenue impact from sales missed - OPTIMIZED"""
    
    logger.info("Calculating revenue impact...")
    
    # Merge dataframes
    dim_final = dim_sales_missed.join(dim_target_sp, how='left')
    
    # Fill missing prices with 0
    dim_final['final_sales_price'] = dim_final.get('final_sales_price', pd.Series(0, index=dim_final.index)).fillna(0)
    
    # Calculate revenue missed until end of 2025
    current_year = datetime.today().year
    current_week = datetime.today().isocalendar().week
    
    if current_year == 2025:
        week_range = range(current_week, 53)
    else:
        week_range = range(1, 53)
    
    week_columns = [f'CW{str(w).zfill(2)}-2025_sales_missed_w' for w in week_range]
    existing_columns = [col for col in week_columns if col in dim_final.columns]
    
    if existing_columns:
        # OPTIMIZATION: Vectorized sum and multiplication
        dim_final['Revenue Miss Until Dec - 2025'] = (
            dim_final[existing_columns].sum(axis=1) * dim_final['final_sales_price']
        )
    else:
        dim_final['Revenue Miss Until Dec - 2025'] = 0
    
    # OPTIMIZATION: Vectorized OOS revenue calculation
    dim_final['Revenue Miss OOS - Until Dec - 2025'] = calculate_oos_revenue_vectorized(dim_final)
    
    # OPTIMIZATION: Vectorized DOH calculation
    dim_final['DOH'] = calculate_doh_vectorized(dim_final['OOS_week_with_signed'])
    
    logger.info("Revenue impact calculations completed")
    return dim_final

def calculate_oos_revenue_vectorized(df):
    """Vectorized OOS revenue calculation"""
    result = pd.Series(0, index=df.index)
    
    # Filter valid OOS weeks
    valid_mask = df['OOS_week_with_signed_final'].notna()
    if not valid_mask.any():
        return result
    
    valid_df = df[valid_mask].copy()
    
    # Extract week numbers efficiently
    valid_df['start_week'] = valid_df['OOS_week_with_signed_final'].str[2:4].astype(float)
    
    # Calculate for each unique start week (more efficient than row-by-row)
    for start_week in valid_df['start_week'].dropna().unique():
        mask = valid_df['start_week'] == start_week
        relevant_columns = [f'CW{int(w):02d}-2025_sales_missed_w' for w in range(int(start_week), 53)]
        existing_columns = [col for col in relevant_columns if col in df.columns]
        
        if existing_columns:
            missed_sales = valid_df.loc[mask, existing_columns].sum(axis=1)
            result.loc[valid_df[mask].index] = missed_sales * valid_df.loc[mask, 'final_sales_price']
    
    return result

def calculate_doh_vectorized(oos_week_series):
    """Vectorized DOH calculation"""
    result = pd.Series(0, index=oos_week_series.index)
    
    valid_mask = oos_week_series.notna()
    if not valid_mask.any():
        return result
    
    valid_weeks = oos_week_series[valid_mask]
    
    # Extract week and year efficiently
    weeks = valid_weeks.str[2:4].astype(float)
    years = valid_weeks.str[-4:].astype(float)
    
    today = datetime.today()
    
    # Vectorized date calculation
    for idx, (week, year) in zip(valid_weeks.index, zip(weeks, years)):
        if pd.notna(week) and pd.notna(year):
            try:
                first_day = datetime.strptime(f'{int(year)}-W{int(week)-1}-1', "%Y-W%U-%w")
                result.loc[idx] = max((first_day - today).days, 0)
            except:
                result.loc[idx] = 0
    
    return result

def calculate_oos_revenue(row):
    """Calculate revenue missed from OOS week until end of year"""
    start_week_str = row.get('OOS_week_with_signed_final')
    final_price = row.get('final_sales_price', 0)
    
    if pd.isna(start_week_str) or not isinstance(start_week_str, str):
        return 0
    
    try:
        start_week = int(start_week_str[2:4])
        relevant_columns = [f'CW{str(w).zfill(2)}-2025_sales_missed_w' for w in range(start_week, 53)]
        existing_columns = [col for col in relevant_columns if col in row.index]
        missed_sales_sum = row[existing_columns].sum() if existing_columns else 0
        return missed_sales_sum * final_price
    except:
        return 0

def calculate_doh(oos_week):
    """Calculate days on hand from OOS week"""
    if pd.isna(oos_week) or not isinstance(oos_week, str):
        return 0
    
    try:
        week = int(oos_week[2:4])
        year = int(oos_week[-4:])
        first_day = datetime.strptime(f'{year}-W{week-1}-1', "%Y-W%U-%w")
        today = datetime.today()
        return max((first_day - today).days, 0)
    except:
        return 0

def generate_recommendations(dim_final):
    """Generate TO and supply chain recommendations - OPTIMIZED"""
    
    logger.info("Generating recommendations...")
    
    # Get current week and year dynamically
    current_week = datetime.today().isocalendar().week
    current_year = datetime.today().isocalendar().year
    
    # OPTIMIZATION: Pre-calculate all demand columns at once
    demand_columns_10w = get_existing_demand_columns(dim_final, current_week, current_year, 10)
    demand_columns_7w = get_existing_demand_columns(dim_final, current_week, current_year, 7)
    demand_columns_14w = get_existing_demand_columns(dim_final, current_week, current_year, 14)
    demand_columns_18w = get_existing_demand_columns(dim_final, current_week, current_year, 18)
    
    # OPTIMIZATION: Vectorized sum operations
    dim_final['future_demand_10w'] = dim_final[demand_columns_10w].sum(axis=1) if demand_columns_10w else 0
    dim_final['future_demand_7w'] = dim_final[demand_columns_7w].sum(axis=1) if demand_columns_7w else 0
    dim_final['future_demand_14w'] = dim_final[demand_columns_14w].sum(axis=1) if demand_columns_14w else 0
    dim_final['future_demand_18w'] = dim_final[demand_columns_18w].sum(axis=1) if demand_columns_18w else 0
    
    # OPTIMIZATION: Vectorized TO Check logic
    us_ca_mask = dim_final.get('mp', pd.Series('', index=dim_final.index)).isin(['US', 'CA'])
    eu_uk_mask = dim_final.get('mp', pd.Series('', index=dim_final.index)).isin(['EU', 'UK'])
    
    inventory_sum = (
        dim_final.get('fulfillable_7d', pd.Series(0, index=dim_final.index)) +
        dim_final.get('at_amz_21d', pd.Series(0, index=dim_final.index)) +
        dim_final.get('on_the_way_to_amz_35d', pd.Series(0, index=dim_final.index))
    )
    
    to_condition = (
        ((us_ca_mask & (inventory_sum < dim_final['future_demand_10w'])) |
         (eu_uk_mask & (inventory_sum < dim_final['future_demand_7w']))) &
        (dim_final.get('local_market(lm)_49d', pd.Series(0, index=dim_final.index)) > 
         dim_final.get('units_per_carton', pd.Series(1, index=dim_final.index)))
    )
    
    dim_final['TO_Check'] = np.where(to_condition, "TO to be checked/created", "")
    
    # OPTIMIZATION: Vectorized FFW recommendations
    ffw_condition = (
        (dim_final.get('otw_35p_98d', pd.Series(0, index=dim_final.index)) < dim_final['future_demand_14w']) &
        (dim_final.get('manufacturing_28_126d', pd.Series(0, index=dim_final.index)) > dim_final['future_demand_18w'])
    )
    
    dim_final['FFW + Supply Ops'] = np.where(ffw_condition, "Expedite pick up goods from vendor", "")
    
    supply_condition = (
        (dim_final.get('otw_35p_98d', pd.Series(0, index=dim_final.index)) < dim_final['future_demand_14w']) &
        (dim_final.get('manufacturing_56p_168d', pd.Series(0, index=dim_final.index)) > 
         dim_final.get('otw_35p_98d', pd.Series(0, index=dim_final.index)))
    )
    
    dim_final['Supply Ops'] = np.where(supply_condition, "Prepone PRD to produce faster", "")
    
    # OPTIMIZATION: Vectorized ARM calculation
    arm_values = np.maximum(
        np.minimum(
            dim_final['future_demand_10w'] - inventory_sum,
            dim_final.get('local_market(lm)_49d', pd.Series(0, index=dim_final.index))
        ) * dim_final.get('final_sales_price', pd.Series(0, index=dim_final.index)),
        0
    )
    
    dim_final['TO_Check_arm'] = np.where(dim_final['TO_Check'] != "", arm_values, 0)
    
    logger.info("Recommendations generated")
    return dim_final

def get_existing_demand_columns(df, start_week, year, num_weeks):
    """Get demand column names that actually exist in the DataFrame"""
    columns = []
    week = start_week
    current_year = year
    
    for _ in range(num_weeks):
        if week > 52:
            week = 1
            current_year += 1
        
        col_name = f'CW{week:02d}-{current_year}_demand'
        if col_name in df.columns:
            columns.append(col_name)
        week += 1
    
    return columns

def get_demand_columns(start_week, year, num_weeks):
    """Get demand column names for specified number of weeks"""
    columns = []
    week = start_week
    current_year = year
    
    for _ in range(num_weeks):
        if week > 52:
            week = 1
            current_year += 1
        
        columns.append(f'CW{week:02d}-{current_year}_demand')
        week += 1
    
    return columns

def calculate_lead_times(dim_final, config):
    """Calculate total lead times using config functions - FIXED"""
    
    # Calculate transport lead time using config
    def get_transport_time(row):
        if hasattr(config, 'get_transport_leadtime'):
            return config.get_transport_leadtime(
                row.get('shipping_region', ''), 
                row.get('mp', ''), 
                45
            )
        else:
            return 45
    
    dim_final['transport_leadtime'] = dim_final.apply(get_transport_time, axis=1)
    
    # Calculate p2cbf using config
    def get_port_buffer(row):
        if hasattr(config, 'get_port_buffer_days'):
            wh_type = 'AMZ' if row.get('mp') in ['US', 'UK', 'EU', 'CA'] else '3PL'
            return config.get_port_buffer_days(wh_type, row.get('mp', ''))
        else:
            mp_mapping = {
                'US': 39, 'CO': 39, 'MX': 39, 'CA': 39,
                'UK': 39, 'BR': 36, 'EU': 40, 'Other': 39
            }
            return 0 if row.get('mp') == row.get('shipping_region') else mp_mapping.get(row.get('mp'), 39)
    
    dim_final['p2cbf'] = dim_final.apply(get_port_buffer, axis=1)
    
    # CRITICAL FIX: Handle missing lead_time_production_days column properly
    if 'lead_time_production_days' in dim_final.columns:
        dim_final['lead_time_production_days'] = dim_final['lead_time_production_days'].fillna(45)
    else:
        dim_final['lead_time_production_days'] = 45
    
    # Calculate total lead time
    dim_final['total_leadtime'] = (
        dim_final['lead_time_production_days'] + 
        dim_final['transport_leadtime'] + 
        dim_final['p2cbf'] + 
        15 + 30  # Processing time + buffer
    )
    
    return dim_final

def generate_cw_list():
    """Generate list of calendar weeks for next 2 years - OPTIMIZED with caching"""
    global _week_cache
    
    # Use cached result if available
    cache_key = datetime.now().strftime('%Y-%W')  # Cache per week
    if cache_key in _week_cache:
        return _week_cache[cache_key]
    
    start_date = datetime.now()
    end_date = datetime(start_date.year + 2, 12, 31)
    
    cw_set = set()
    current_date = start_date
    
    while current_date <= end_date:
        iso_year, iso_week, _ = current_date.isocalendar()
        if iso_week != 53:  # exclude CW53
            cw_label = f"CW{iso_week:02d}-{iso_year}"
            cw_set.add(cw_label)
        current_date += timedelta(days=7)
    
    result = sorted(cw_set, key=lambda x: (int(x.split('-')[1]), int(x[2:4])))
    
    # Cache the result
    _week_cache[cache_key] = result
    
    return result

def calculate_all(processed_data, config):
    """Main function to run all calculations - OPTIMIZED"""
    
    logger.info("Starting comprehensive inventory calculations...")
    
    try:
        # Extract processed data
        dim_demand = processed_data.get('dim_demand', pd.DataFrame())
        dim_inventory = processed_data.get('dim_inventory', pd.DataFrame())
        dim_open_po_signed = processed_data.get('dim_open_po_signed', pd.DataFrame())
        dim_open_po_unsigned = processed_data.get('dim_open_po_unsigned', pd.DataFrame())
        dim_inbound = processed_data.get('dim_inbound', pd.DataFrame())
        dim_target_sp = processed_data.get('dim_target_sp', pd.DataFrame())
        
        # Calculate sales missed
        dim_sales_missed = calculate_sales_missed(
            dim_demand, dim_inventory, dim_open_po_signed, 
            dim_open_po_unsigned, dim_inbound
        )
        
        # Calculate revenue impact
        dim_final = calculate_revenue_impact(dim_sales_missed, dim_target_sp)
        
        # Generate recommendations
        dim_final = generate_recommendations(dim_final)
        
        # Calculate lead times if config available
        if config:
            dim_final = calculate_lead_times(dim_final, config)
        
        # Create results dictionary
        results = {
            'final_allocation': dim_final,
            'sales_missed': dim_sales_missed,
            'demand_coverage': calculate_demand_coverage(dim_final),
            'oos_count': count_oos_items(dim_final)
        }
        
        logger.info("All calculations completed successfully")
        return results
        
    except Exception as e:
        logger.error(f"Error in calculations: {str(e)}")
        raise

def calculate_demand_coverage(dim_final):
    """Calculate overall demand coverage percentage - OPTIMIZED"""
    try:
        # OPTIMIZATION: Use column filtering instead of iteration
        demand_cols = [col for col in dim_final.columns if '_demand' in col]
        missed_cols = [col for col in dim_final.columns if '_sales_missed_w' in col]
        
        total_demand = dim_final[demand_cols].sum().sum() if demand_cols else 0
        total_missed = dim_final[missed_cols].sum().sum() if missed_cols else 0
        
        if total_demand > 0:
            coverage = ((total_demand - total_missed) / total_demand) * 100
            return round(coverage, 2)
        else:
            return 100.0
    except:
        return 0.0

def count_oos_items(dim_final):
    """Count items predicted to go out of stock"""
    try:
        if 'OOS_week_with_signed' in dim_final.columns:
            return dim_final['OOS_week_with_signed'].notna().sum()
        else:
            return 0
    except:
        return 0
