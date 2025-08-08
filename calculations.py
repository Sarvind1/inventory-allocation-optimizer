"""
Calculation functions for inventory allocation - Fixed version
Contains all core calculation logic from the notebook
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

def calculate_sales_missed(dim_demand, dim_inventory, dim_open_po_signed, 
                          dim_open_po_unsigned, dim_inbound):
    """Calculate sales missed based on inventory waterfall"""
    
    logger.info("Starting sales missed calculations...")
    
    # Get all unique refs
    all_refs = pd.Index(
        set(dim_demand.index) | set(dim_inventory.index) | 
        set(dim_open_po_signed.index) | set(dim_open_po_unsigned.index) | 
        set(dim_inbound.index)
    )
    
    # Reindex all dataframes to ensure alignment
    dim_demand = dim_demand.reindex(all_refs, fill_value=0)
    dim_inventory_start = dim_inventory.reindex(all_refs, fill_value=0)
    dim_inventory_end = pd.DataFrame(index=all_refs)
    dim_sales_missed = pd.DataFrame(index=all_refs)
    dim_inbound = dim_inbound.reindex(all_refs, fill_value=0)
    dim_open_po_signed = dim_open_po_signed.reindex(all_refs, fill_value=0)
    dim_open_po_unsigned = dim_open_po_unsigned.reindex(all_refs, fill_value=0)
    
    # Get week list
    weeks = generate_cw_list()
    
    if not weeks:
        logger.error("No weeks generated")
        return dim_sales_missed
    
    # First week calculations
    first_week = weeks[0]
    first_week_inventory = f"{first_week}_inventory_start"
    
    # Set initial inventory
    if 'total_inventory' in dim_inventory.columns:
        dim_inventory_start[first_week_inventory] = dim_inventory['total_inventory']
    else:
        dim_inventory_start[first_week_inventory] = 0
    
    # Calculate first week
    first_week_demand = dim_demand.get(f'{first_week}_demand', 0)
    first_week_inbound = dim_inbound.get(f'{first_week}_inbound', 0)
    first_week_signed = dim_open_po_signed.get(f'{first_week}_open_po_signed', 0)
    
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
    dim_sales_missed['OOS_week_with_signed'] = None
    dim_sales_missed['OOS_week_with_signed_last'] = None
    
    condition = dim_sales_missed[f'{first_week}_sales_missed_w'] > 0
    dim_sales_missed.loc[condition, 'OOS_week_with_signed'] = first_week
    
    # Loop through remaining weeks (up to 104 weeks for 2 years)
    for i in range(1, min(len(weeks), 104)):
        current_week = weeks[i]
        previous_week = weeks[i-1]
        
        current_inventory_start = f'{current_week}_inventory_start'
        current_inventory_end = f'{current_week}_inventory_end'
        current_demand = f'{current_week}_demand'
        current_inbound = f'{current_week}_inbound'
        current_signed = f'{current_week}_open_po_signed'
        current_unsigned = f'{current_week}_open_po_unsigned'
        
        # Ensure columns exist with defaults
        demand_val = dim_demand.get(current_demand, 0)
        inbound_val = dim_inbound.get(current_inbound, 0)
        signed_val = dim_open_po_signed.get(current_signed, 0)
        unsigned_val = dim_open_po_unsigned.get(current_unsigned, 0)
        
        # Calculate inventory flow
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
        
        # Update OOS tracking
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
    """Calculate revenue impact from sales missed"""
    
    logger.info("Calculating revenue impact...")
    
    # Merge dataframes
    dim_final = dim_sales_missed.join(dim_target_sp, how='left')
    
    # Fill missing prices with 0 (can be enhanced with LTM logic later)
    dim_final['final_sales_price'] = dim_final.get('final_sales_price', 0).fillna(0)
    
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
        dim_final['Revenue Miss Until Dec - 2025'] = (
            dim_final[existing_columns].sum(axis=1) * dim_final['final_sales_price']
        )
    else:
        dim_final['Revenue Miss Until Dec - 2025'] = 0
    
    # Calculate OOS revenue impact
    dim_final['Revenue Miss OOS - Until Dec - 2025'] = dim_final.apply(
        calculate_oos_revenue, axis=1
    )
    
    # Calculate DOH (Days on Hand)
    dim_final['DOH'] = dim_final['OOS_week_with_signed'].apply(calculate_doh)
    
    logger.info("Revenue impact calculations completed")
    return dim_final

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
    """Generate TO and supply chain recommendations"""
    
    logger.info("Generating recommendations...")
    
    # Get current week and year dynamically
    current_week = datetime.today().isocalendar().week
    current_year = datetime.today().isocalendar().year
    
    # Calculate future demand periods
    demand_columns = get_demand_columns(current_week, current_year, 10)
    dim_final['future_demand_10w'] = dim_final[demand_columns].sum(axis=1) if demand_columns else 0
    
    demand_columns_7w = get_demand_columns(current_week, current_year, 7)
    dim_final['future_demand_7w'] = dim_final[demand_columns_7w].sum(axis=1) if demand_columns_7w else 0
    
    # TO Check logic
    dim_final['TO_Check'] = np.where(
        (
            (
                (dim_final.get('mp', '') in ['US', 'CA']) & 
                ((dim_final.get('fulfillable_7d', 0) + 
                  dim_final.get('at_amz_21d', 0) + 
                  dim_final.get('on_the_way_to_amz_35d', 0)) < dim_final['future_demand_10w'])
            )
            |
            (
                (dim_final.get('mp', '') in ['EU', 'UK']) & 
                ((dim_final.get('fulfillable_7d', 0) + 
                  dim_final.get('at_amz_21d', 0) + 
                  dim_final.get('on_the_way_to_amz_35d', 0)) < dim_final['future_demand_7w'])
            )
        )
        &
        (dim_final.get('local_market(lm)_49d', 0) > dim_final.get('units_per_carton', 1)),
        "TO to be checked/created",
        ""
    )
    
    # FFW recommendations
    demand_columns_14w = get_demand_columns(current_week, current_year, 14)
    dim_final['future_demand_14w'] = dim_final[demand_columns_14w].sum(axis=1) if demand_columns_14w else 0
    
    demand_columns_18w = get_demand_columns(current_week, current_year, 18)
    dim_final['future_demand_18w'] = dim_final[demand_columns_18w].sum(axis=1) if demand_columns_18w else 0
    
    dim_final['FFW + Supply Ops'] = np.where(
        (dim_final.get('otw_35p_98d', 0) < dim_final['future_demand_14w']) & 
        (dim_final.get('manufacturing_28_126d', 0) > dim_final['future_demand_18w']),
        "Expedite pick up goods from vendor",
        ""
    )
    
    dim_final['Supply Ops'] = np.where(
        (dim_final.get('otw_35p_98d', 0) < dim_final['future_demand_14w']) & 
        (dim_final.get('manufacturing_56p_168d', 0) > dim_final.get('otw_35p_98d', 0)),
        "Prepone PRD to produce faster",
        ""
    )
    
    # Calculate ARM (At Risk Margin)
    dim_final['TO_Check_arm'] = np.where(
        dim_final['TO_Check'] != "",
        np.maximum(
            np.minimum(
                dim_final['future_demand_10w'] - 
                dim_final.get('fulfillable_7d', 0) - 
                dim_final.get('at_amz_21d', 0) - 
                dim_final.get('on_the_way_to_amz_35d', 0),
                dim_final.get('local_market(lm)_49d', 0)
            ) * dim_final.get('final_sales_price', 0),
            0
        ),
        0
    )
    
    logger.info("Recommendations generated")
    return dim_final

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
    
    return [col for col in columns]

def calculate_lead_times(dim_final, transport_map):
    """Calculate total lead times"""
    
    # Calculate transport lead time
    dim_final['transport_leadtime'] = dim_final.apply(
        lambda row: transport_map.get(
            (row.get('shipping_region'), row.get('mp')), 45
        ),
        axis=1
    )
    
    # Calculate p2cbf
    mp_mapping = {
        'US': 39, 'CO': 39, 'MX': 39, 'CA': 39,
        'UK': 39, 'BR': 36, 'EU': 40, 'Other': 39
    }
    
    dim_final['p2cbf'] = dim_final.apply(
        lambda row: 0 if row.get('mp') == row.get('shipping_region') 
                     else mp_mapping.get(row.get('mp'), 39),
        axis=1
    )
    
    # Fill missing lead times
    dim_final['lead_time_production_days'] = dim_final.get(
        'lead_time_production_days', 45
    ).fillna(45)
    
    # Calculate total lead time
    dim_final['total_leadtime'] = (
        dim_final['lead_time_production_days'] + 
        dim_final['transport_leadtime'] + 
        dim_final['p2cbf'] + 
        15 + 30  # Processing time + buffer
    )
    
    return dim_final

def generate_cw_list():
    """Generate list of calendar weeks for next 2 years"""
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
    
    return sorted(cw_set, key=lambda x: (int(x.split('-')[1]), int(x[2:4])))

def calculate_all(processed_data, config):
    """Main function to run all calculations - replaces InventoryCalculator class"""
    
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
        
        # Calculate lead times if transport map available
        transport_map = config.get_transport_map_dict() if hasattr(config, 'get_transport_map_dict') else {}
        if transport_map:
            dim_final = calculate_lead_times(dim_final, transport_map)
        
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
    """Calculate overall demand coverage percentage"""
    try:
        total_demand = 0
        total_missed = 0
        
        # Sum up demand and sales missed columns
        for col in dim_final.columns:
            if '_demand' in col:
                total_demand += dim_final[col].sum()
            elif '_sales_missed_w' in col:
                total_missed += dim_final[col].sum()
        
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
