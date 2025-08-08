"""
Simplified Database Connector
Handles Redshift connections and query execution with threading support
"""

import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from redshift_connector import connect
import threading
import logging
import os
from sql_query_loader import SQLQueryLoader

logger = logging.getLogger(__name__)

class DatabaseConnector:
    """Simplified database connector with parallel query execution"""
    
    def __init__(self, conn_params=None):
        """Initialize with connection parameters or environment variables"""
        if conn_params is None:
            conn_params = {
                'user': os.getenv('REDSHIFT_USER'),
                'password': os.getenv('REDSHIFT_PASSWORD'),
                'database': os.getenv('REDSHIFT_DATABASE', 'dev'),
                'host': os.getenv('REDSHIFT_HOST'),
                'port': int(os.getenv('REDSHIFT_PORT', 5439))
            }
        self.conn_params = conn_params
        self._local = threading.local()
        self.query_loader = SQLQueryLoader()
    
    def get_connection(self):
        """Get thread-local connection"""
        if not hasattr(self._local, 'conn') or self._local.conn.closed:
            self._local.conn = connect(**self.conn_params)
        return self._local.conn
    
    def execute_query(self, query_name):
        """Execute a single query"""
        try:
            query = self.query_loader.get_query(query_name)
            if not query:
                logger.error(f"Query {query_name} not found")
                return query_name, pd.DataFrame()
            
            conn = self.get_connection()
            df = pd.read_sql(query, conn)
            logger.info(f"Query {query_name}: {len(df)} rows loaded")
            
            # Optimize memory
            for col in df.columns:
                col_type = df[col].dtype
                if col_type != object:
                    if col_type == 'float64':
                        df[col] = pd.to_numeric(df[col], downcast='float')
                    elif col_type == 'int64':
                        df[col] = pd.to_numeric(df[col], downcast='integer')
            
            return query_name, df
        except Exception as e:
            logger.error(f"Error in query {query_name}: {str(e)}")
            return query_name, pd.DataFrame()
    
    def load_queries_parallel(self, query_names, max_workers=5):
        """Load multiple queries in parallel"""
        results = {}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.execute_query, name): name for name in query_names}
            
            for future in as_completed(futures):
                query_name, df = future.result()
                results[query_name] = df
        
        return results
    
    def close(self):
        """Close connection"""
        if hasattr(self._local, 'conn'):
            try:
                self._local.conn.close()
            except:
                pass