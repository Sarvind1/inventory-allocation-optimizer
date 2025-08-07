"""
Enhanced Database Loader with SQL file integration
Handles all Redshift data loading with connection pooling and parallel execution
"""

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from redshift_connector import connect
import threading
import logging
from sql_query_loader import SQLQueryLoader

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RedshiftDataLoader:
    """Handles all Redshift data loading with connection pooling and parallel execution"""
    
    def __init__(self, conn_params):
        self.conn_params = conn_params
        self._local = threading.local()
        self.query_loader = SQLQueryLoader()
    
    def get_connection(self):
        """Get thread-local connection"""
        if not hasattr(self._local, 'conn') or self._local.conn.closed:
            self._local.conn = connect(**self.conn_params)
        return self._local.conn
    
    def execute_query(self, query_name):
        """Execute a single query with error handling"""
        try:
            logger.info(f"Starting query: {query_name}")
            
            # Get query from SQL file
            query = self.query_loader.get_query(query_name)
            if not query:
                logger.error(f"Query {query_name} not found in SQL files")
                return query_name, pd.DataFrame()
            
            conn = self.get_connection()
            df = pd.read_sql(query, conn)
            logger.info(f"Completed query: {query_name}, rows: {len(df)}")
            return query_name, df
        except Exception as e:
            logger.error(f"Error in query {query_name}: {str(e)}")
            return query_name, pd.DataFrame()
    
    def load_all_data(self, query_names=None):
        """
        Load all queries in parallel
        
        Args:
            query_names: List of query names to execute. If None, executes all available queries.
        """
        if query_names is None:
            query_names = list(self.query_loader.get_all_queries().keys())
        
        results = {}
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(self.execute_query, name): name 
                for name in query_names
            }
            
            for future in as_completed(futures):
                query_name, df = future.result()
                results[query_name] = df
        
        return results
    
    def load_single_query(self, query_name):
        """Load a single query"""
        _, df = self.execute_query(query_name)
        return df
    
    def close_connections(self):
        """Close all thread-local connections"""
        if hasattr(self._local, 'conn'):
            try:
                self._local.conn.close()
                logger.info("Connection closed successfully")
            except:
                pass

def optimize_dataframe_memory(df):
    """Reduce memory usage of DataFrame"""
    for col in df.columns:
        col_type = df[col].dtype
        
        if col_type != object:
            if col_type == 'float64':
                df[col] = pd.to_numeric(df[col], downcast='float')
            elif col_type == 'int64':
                df[col] = pd.to_numeric(df[col], downcast='integer')
    
    return df

# Main execution function
def load_inventory_data(conn_params, optimize_memory=True):
    """
    Main function to load all inventory allocation data
    
    Args:
        conn_params: Database connection parameters
        optimize_memory: Whether to optimize DataFrame memory usage
    
    Returns:
        Dictionary of DataFrames with query results
    """
    loader = RedshiftDataLoader(conn_params)
    
    # Define which queries to load for inventory allocation
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
    
    try:
        # Load all data in parallel
        logger.info("Starting parallel data load...")
        data_dict = loader.load_all_data(required_queries)
        
        # Optimize memory if requested
        if optimize_memory:
            logger.info("Optimizing DataFrame memory usage...")
            for name, df in data_dict.items():
                data_dict[name] = optimize_dataframe_memory(df)
        
        logger.info("Data loading completed successfully")
        return data_dict
        
    except Exception as e:
        logger.error(f"Error during data loading: {e}")
        raise
    finally:
        loader.close_connections()

# Example usage
if __name__ == "__main__":
    # Connection parameters (these should be moved to environment variables)
    conn_params = {
        'user': 'manh.nguyen@razor-group.com',
        'password': 'qdkcTHB8CPfe7AQHVNEF',
        'database': 'dev',
        'host': 'datawarehouse-dev.cdg4y3yokxle.eu-central-1.redshift.amazonaws.com',
        'port': 5439
    }
    
    # Load all data
    data = load_inventory_data(conn_params)
    
    # Display summary
    for name, df in data.items():
        print(f"{name}: {len(df)} rows, {len(df.columns)} columns")