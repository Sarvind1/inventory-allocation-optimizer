"""
SQL Query Loader
Loads SQL queries from separate .sql files for better organization
"""

import os
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class SQLQueryLoader:
    """Loads SQL queries from separate files"""
    
    def __init__(self, query_dir='sql_queries'):
        """
        Initialize the SQL query loader
        
        Args:
            query_dir: Directory containing SQL query files
        """
        self.query_dir = Path(query_dir)
        self.queries = {}
        self._load_all_queries()
    
    def _load_all_queries(self):
        """Load all SQL queries from the directory"""
        if not self.query_dir.exists():
            logger.error(f"SQL query directory {self.query_dir} does not exist")
            return
        
        sql_files = {
            'asin_vendor': 'asin_vendor_mapping.sql',
            'target_sp': 'target_sales_price.sql',
            'demand': 'demand_forecast.sql',
            'master_data': 'master_data.sql',
            'gfl_list': 'gfl_list.sql',
            'vendor_master': 'vendor_master.sql',
            'open_po': 'open_po.sql',
            'otif_status': 'otif_status.sql',
            'inbound': 'inbound_shipments.sql',
            'inventory': 'inventory_sop.sql'
        }
        
        for query_name, filename in sql_files.items():
            filepath = self.query_dir / filename
            if filepath.exists():
                try:
                    with open(filepath, 'r') as f:
                        self.queries[query_name] = f.read()
                    logger.info(f"Loaded query: {query_name} from {filename}")
                except Exception as e:
                    logger.error(f"Error loading {filename}: {e}")
            else:
                logger.warning(f"SQL file not found: {filepath}")
    
    def get_query(self, query_name):
        """
        Get a specific query by name
        
        Args:
            query_name: Name of the query to retrieve
            
        Returns:
            SQL query string or None if not found
        """
        if query_name not in self.queries:
            logger.error(f"Query '{query_name}' not found")
            return None
        return self.queries[query_name]
    
    def get_all_queries(self):
        """Get all loaded queries as a dictionary"""
        return self.queries.copy()
    
    def reload_queries(self):
        """Reload all queries from disk"""
        self.queries.clear()
        self._load_all_queries()
        logger.info("Reloaded all SQL queries")

# Convenience function for backward compatibility
def get_all_sql_queries():
    """
    Get all SQL queries as a dictionary
    This function maintains backward compatibility with existing code
    """
    loader = SQLQueryLoader()
    return loader.get_all_queries()