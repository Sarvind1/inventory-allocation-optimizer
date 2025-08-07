#!/usr/bin/env python3
"""
Main script for inventory allocation and PO optimization
Simplified version for business users
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Import our modules
from database_loader import load_all_data_concurrent
from data_processor import (
    process_demand_data,
    process_inventory_data,
    process_open_po_data,
    process_inbound_data,
    process_master_data
)
from calculations import (
    calculate_sales_missed,
    calculate_revenue_impact,
    calculate_lead_times,
    generate_recommendations
)
from config_loader import load_config_files
from utils import create_output_folder, save_debug_files

def main():
    """
    Main execution function - runs the complete pipeline
    """
    print("="*60)
    print("Starting Inventory Allocation Optimization")
    print(f"Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # Create output folder for today's run
    output_folder = create_output_folder()
    
    # Step 1: Load configuration files
    print("\n[1/7] Loading configuration files...")
    config = load_config_files()
    
    # Step 2: Load all data from database (concurrent execution)
    print("\n[2/7] Loading data from Redshift (using parallel queries)...")
    start = datetime.now()
    all_data = load_all_data_concurrent(config['conn_params'])
    print(f"Data loaded in {(datetime.now()-start).total_seconds():.1f} seconds")
    
    # Step 3: Process master data
    print("\n[3/7] Processing master data...")
    dim_master_data = process_master_data(all_data['master_data'])
    dim_asin_vendor = all_data['asin_vendor']
    dim_product_market = all_data['product_market']
    dim_target_sp = all_data['target_sp']
    dim_gfl_list = all_data['gfl_list']
    
    # Step 4: Process demand data
    print("\n[4/7] Processing demand and forecast data...")
    dim_demand = process_demand_data(all_data['demand'])
    
    # Step 5: Process inventory and supply chain data
    print("\n[5/7] Processing inventory and supply chain data...")
    dim_inventory = process_inventory_data(all_data['inventory'])
    dim_open_po_signed, dim_open_po_unsigned = process_open_po_data(
        all_data['open_po'], 
        all_data['otif_status'],
        dim_master_data,
        dim_product_market,
        config
    )
    dim_inbound = process_inbound_data(
        all_data['inbound'],
        dim_product_market,
        config
    )
    
    # Step 6: Calculate sales missed and revenue impact
    print("\n[6/7] Calculating sales missed and revenue impact...")
    dim_sales_missed = calculate_sales_missed(
        dim_demand,
        dim_inventory,
        dim_open_po_signed,
        dim_open_po_unsigned,
        dim_inbound
    )
    
    # Calculate revenue impacts
    dim_final = calculate_revenue_impact(
        dim_sales_missed,
        dim_target_sp,
        all_data['finance']
    )
    
    # Step 7: Generate recommendations
    print("\n[7/7] Generating recommendations...")
    dim_final = generate_recommendations(
        dim_final,
        dim_demand,
        dim_inventory,
        config
    )
    
    # Add lead time calculations
    dim_final = calculate_lead_times(
        dim_final,
        dim_product_market,
        config
    )
    
    # Save debug files if needed
    if config['save_debug_files']:
        save_debug_files(output_folder, {
            'demand': dim_demand,
            'inventory': dim_inventory,
            'open_po_signed': dim_open_po_signed,
            'open_po_unsigned': dim_open_po_unsigned,
            'inbound': dim_inbound,
            'sales_missed': dim_sales_missed
        })
    
    # Save final output
    output_file = output_folder / f"final_{datetime.now().strftime('%y-%m-%d')}.csv"
    dim_final.to_csv(output_file, index=False)
    
    print("\n" + "="*60)
    print(f"✓ Complete! Output saved to: {output_file}")
    print(f"Total rows processed: {len(dim_final):,}")
    print(f"Total execution time: {(datetime.now()-start).total_seconds():.1f} seconds")
    print("="*60)
    
    return dim_final

if __name__ == "__main__":
    try:
        final_data = main()
        print("\n✓ Process completed successfully!")
    except Exception as e:
        print(f"\n✗ Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        exit(1)