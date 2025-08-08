"""
Main execution script for Inventory Allocation Optimizer
"""

import os
import logging
from datetime import datetime
from database_connector import DatabaseConnector
from config_loader import ConfigLoader
from data_processor import DataProcessor
from calculations import InventoryCalculator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Main execution function"""
    
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
        
        # Process data
        logger.info("Processing data...")
        processor = DataProcessor(config)
        
        processed_data = {
            'dim_demand': processor.process_demand_data(data['demand']),
            'dim_inventory': processor.process_inventory_data(data['inventory']),
            'dim_open_po': processor.process_open_po_data(data['open_po'], data['master_data']),
            'dim_inbound': processor.process_inbound_data(data['inbound']),
            'dim_master': processor.process_master_data(data['master_data']),
            'dim_vendor': processor.process_vendor_data(data['vendor_master']),
            'dim_target_sp': data['target_sp'].set_index('ref') if 'ref' in data['target_sp'].columns else data['target_sp'],
            'dim_gfl': processor.process_gfl_data(data['gfl_list'])
        }
        
        # Run calculations
        logger.info("Running inventory calculations...")
        calculator = InventoryCalculator(processed_data, config)
        results = calculator.calculate_all()
        
        # Save results
        output_dir = 'output'
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f"{output_dir}/inventory_allocation_{timestamp}.csv"
        
        results['final_allocation'].to_csv(output_file, index=False)
        logger.info(f"Results saved to {output_file}")
        
        # Generate summary
        elapsed_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Process completed in {elapsed_time:.2f} seconds")
        
        # Print summary statistics
        print("\n" + "="*50)
        print("INVENTORY ALLOCATION SUMMARY")
        print("="*50)
        print(f"Total SKUs processed: {len(results['final_allocation'])}")
        print(f"Total demand covered: {results.get('demand_coverage', 'N/A')}%")
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
    results = main()