"""
Database connection management - OPTIMIZED VERSION
Fixed connection checking and improved parallel loading
"""

import pandas as pd
import redshift_connector
from redshift_connector import connect
import threading
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

logger = logging.getLogger(__name__)

class DatabaseConnector:
    """Manages Redshift database connections with connection pooling"""
    
    def __init__(self, host, dbname, user, password, port=5439):
        """Initialize database connection parameters"""
        self.conn_params = {
            'host': host,
            'database': dbname,
            'user': user,
            'password': password,
            'port': port
        }
        self._local = threading.local()
        self.query_loader = None
        
    def set_query_loader(self, query_loader):
        """Set the SQL query loader"""
        self.query_loader = query_loader
        
    def get_connection(self):
        """Get thread-local database connection - FIXED"""
        # CRITICAL FIX: Proper connection checking
        if not hasattr(self._local, 'conn'):
            self._local.conn = connect(**self.conn_params)
        else:
            # Try to check if connection is alive
            try:
                # Test the connection with a simple query
                cursor = self._local.conn.cursor()
                cursor.execute("SELECT 1")
                cursor.close()
            except:
                # Connection is dead, recreate it
                try:
                    self._local.conn.close()
                except:
                    pass
                self._local.conn = connect(**self.conn_params)
        
        return self._local.conn
    
    def execute_query(self, query_name):
        """Execute a single query - OPTIMIZED"""
        start_time = time.time()
        
        try:
            query = self.query_loader.get_query(query_name)
            if not query:
                logger.error(f"Query {query_name} not found")
                return query_name, pd.DataFrame()
            
            conn = self.get_connection()
            
            # OPTIMIZATION: Use chunksize for large queries
            if query_name in ['demand', 'inventory', 'open_po']:
                df_list = []
                for chunk in pd.read_sql(query, conn, chunksize=10000):
                    df_list.append(chunk)
                df = pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()
            else:
                df = pd.read_sql(query, conn)
            
            elapsed = time.time() - start_time
            logger.info(f"Query {query_name}: {len(df)} rows loaded in {elapsed:.2f}s")
            
            # OPTIMIZATION: Aggressive memory optimization
            df = self.optimize_memory(df)
            
            return query_name, df
            
        except Exception as e:
            logger.error(f"Error in query {query_name}: {str(e)}")
            return query_name, pd.DataFrame()
    
    def optimize_memory(self, df):
        """Optimize DataFrame memory usage - ENHANCED"""
        if df.empty:
            return df
        
        # Optimize numeric columns
        for col in df.columns:
            col_type = df[col].dtype
            
            if col_type != 'object':
                if 'float' in str(col_type):
                    # Downcast floats
                    df[col] = pd.to_numeric(df[col], downcast='float', errors='ignore')
                elif 'int' in str(col_type):
                    # Downcast integers
                    df[col] = pd.to_numeric(df[col], downcast='integer', errors='ignore')
            else:
                # Convert low cardinality strings to category
                num_unique = df[col].nunique()
                num_total = len(df[col])
                if num_unique / num_total < 0.5 and num_unique < 1000:
                    df[col] = df[col].astype('category')
        
        return df
    
    def load_queries_parallel(self, query_names, max_workers=6):
        """Load multiple queries in parallel - OPTIMIZED"""
        results = {}
        
        # OPTIMIZATION: Prioritize smaller queries first
        priority_order = {
            'gfl_list': 1,
            'vendor_master': 2,
            'asin_vendor': 3,
            'target_sp': 4,
            'master_data': 5,
            'otif_status': 6,
            'open_po': 7,
            'inbound': 8,
            'inventory': 9,
            'demand': 10
        }
        
        sorted_queries = sorted(query_names, key=lambda x: priority_order.get(x, 99))
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.execute_query, name): name for name in sorted_queries}
            
            for future in as_completed(futures):
                query_name, df = future.result()
                results[query_name] = df
        
        return results
    
    def execute_query_with_retry(self, query_name, max_retries=3):
        """Execute query with retry logic"""
        for attempt in range(max_retries):
            try:
                return self.execute_query(query_name)
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Query {query_name} failed after {max_retries} attempts: {str(e)}")
                    return query_name, pd.DataFrame()
                logger.warning(f"Query {query_name} attempt {attempt + 1} failed, retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return query_name, pd.DataFrame()
    
    def close(self):
        """Close connection"""
        if hasattr(self._local, 'conn'):
            try:
                self._local.conn.close()
            except:
                pass
