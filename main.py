"""
Main execution script for Inventory Allocation Optimizer - Fixed version
All classes removed and replaced with functions as requested
"""

import os
import logging
from datetime import datetime
from database_connector import DatabaseConnector
from config_loader import ConfigLoader
from data_processor import (
    process_demand_data, process_inventory_data, process_open_po_data,
    process_inbound_data, process_master_data, process_vendor_data, process_gfl_data
)
from calculations import calculate_all

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main execution function - fixed to use functions instead of classes"""
    
    logger.info("Starting Inventory Allocation Optimizer")
    start_time = datetime.now()
    
    try:
        # Initialize components
        logger.info("Initializing components...")
        
        # Database connection (uses environment variables if not provided)
        conn_params = {
            'user': os.getenv('REDSHIFT_USER', 'manh.nguyen@razor-group.com'),
            'password': os.getenv('REDSHIFT_PASSWORD', 'qdkcTHB8CPfe7AQHVNEF'),
            'database': os.getenv('REDSHIFT_DATABASE', 'dev'),
            'host': os.getenv('REDSHIFT_HOST', 'datawarehouse-dev.cdg4y3yokxle.eu-central-1.redshift.amazonaws.com'),
            'port': int(os.getenv('REDSHIFT_PORT', 5439))
        }
        
        db = DatabaseConnector(conn_params)
        config = ConfigLoader()
        
        # Define required queries
        required_queries = [
            'asin_vendor',
            'target_sp',
            'demand',
            'master_data',
            'gfl_list',
            'vendor_master',
            'open_po',
            'otif_status',
            'inbound',
            'inventory'
        ]
        
        # Load data in parallel
        logger.info(f"Loading {len(required_queries)} queries in parallel...")
        data = db.load_queries_parallel(required_queries, max_workers=5)
        
        # Process data using functions instead of class methods
        logger.info("Processing data...")
        
        processed_data = {}
        
        # Process demand data
        if not data['demand'].empty:
            processed_data['dim_demand'] = process_demand_data(data['demand'])
        else:
            processed_data['dim_demand'] = pd.DataFrame()
            
        # Process inventory data
        if not data['inventory'].empty:
            processed_data['dim_inventory'] = process_inventory_data(data['inventory'])
        else:
            processed_data['dim_inventory'] = pd.DataFrame()
            
        # Process master data
        if not data['master_data'].empty:
            processed_data['dim_master'] = process_master_data(data['master_data'])
        else:
            processed_data['dim_master'] = pd.DataFrame()
            
        # Process open PO data (returns tuple of signed, unsigned)
        if not data['open_po'].empty:
            dim_open_po_signed, dim_open_po_unsigned = process_open_po_data(
                data['open_po'], processed_data['dim_master']
            )
            processed_data['dim_open_po_signed'] = dim_open_po_signed
            processed_data['dim_open_po_unsigned'] = dim_open_po_unsigned
        else:
            processed_data['dim_open_po_signed'] = pd.DataFrame()
            processed_data['dim_open_po_unsigned'] = pd.DataFrame()
            
        # Process inbound data
        if not data['inbound'].empty:
            processed_data['dim_inbound'] = process_inbound_data(data['inbound'])
        else:
            processed_data['dim_inbound'] = pd.DataFrame()
            
        # Process vendor data
        if not data['vendor_master'].empty:
            processed_data['dim_vendor'] = process_vendor_data(data['vendor_master'])
        else:
            processed_data['dim_vendor'] = pd.DataFrame()
            
        # Process target sales price
        if 'target_sp' in data and not data['target_sp'].empty:
            if 'ref' in data['target_sp'].columns:
                processed_data['dim_target_sp'] = data['target_sp'].set_index('ref')
            else:
                processed_data['dim_target_sp'] = data['target_sp']
        else:
            processed_data['dim_target_sp'] = pd.DataFrame()
            
        # Process GFL data
        if not data['gfl_list'].empty:
            processed_data['dim_gfl'] = process_gfl_data(data['gfl_list'])
        else:
            processed_data['dim_gfl'] = pd.DataFrame()
        
        # Run calculations using function instead of class
        logger.info("Running inventory calculations...")
        results = calculate_all(processed_data, config)
        
        # Save results
        output_dir = 'output'
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"{output_dir}/inventory_allocation_{timestamp}.csv"
        
        if 'final_allocation' in results and not results['final_allocation'].empty:
            results['final_allocation'].to_csv(output_file, index=False)
            logger.info(f"Results saved to {output_file}")
        else:
            logger.warning("No final allocation data to save")
        
        # Generate summary
        elapsed_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Process completed in {elapsed_time:.2f} seconds")
        
        # Print summary statistics
        print("\n" + "="*50)
        print("INVENTORY ALLOCATION SUMMARY")
        print("="*50)
        
        if 'final_allocation' in results and not results['final_allocation'].empty:
            print(f"Total SKUs processed: {len(results['final_allocation'])}")
        else:
            print("Total SKUs processed: 0")
            
        print(f"Demand coverage: {results.get('demand_coverage', 'N/A')}%")
        print(f"Out of stock predictions: {results.get('oos_count', 0)} items")
        print(f"Processing time: {elapsed_time:.2f} seconds")
        print("="*50)
        
        return results
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise
    finally:
        # Cleanup
        if 'db' in locals():
            db.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    # Import pandas here since it's used in the main function
    import pandas as pd
    results = main()
