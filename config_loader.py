"""
Optimized Configuration Loader
Simple functions for loading configuration data from CSV files
Designed for business users with focus on efficiency and readability
"""

import pandas as pd
import logging
from pathlib import Path
from functools import lru_cache
from typing import Dict, List, Optional, Union

logger = logging.getLogger(__name__)

# Global cache for config data to avoid repeated file reads
_config_cache = {}
_config_dir = Path('config')

def set_config_directory(config_dir: str = 'config'):
    """Set the configuration directory path"""
    global _config_dir
    _config_dir = Path(config_dir)
    clear_config_cache()

def clear_config_cache():
    """Clear the configuration cache to force reload"""
    global _config_cache
    _config_cache.clear()
    # Clear LRU cache as well
    _load_csv_file.cache_clear()
    logger.info("Configuration cache cleared")

@lru_cache(maxsize=128)
def _load_csv_file(filename: str) -> Optional[pd.DataFrame]:
    """
    Load a single CSV file with caching
    Uses LRU cache to avoid repeated file reads
    """
    filepath = _config_dir / filename
    
    if not filepath.exists():
        logger.warning(f"Configuration file not found: {filepath}")
        return None
    
    try:
        # Read CSV with optimized settings
        df = pd.read_csv(
            filepath,
            dtype=str,  # Read all as strings first to avoid type issues
            na_filter=False  # Don't convert empty strings to NaN
        )
        
        # Strip whitespace from all string columns
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()
        
        logger.info(f"Loaded {filename}: {len(df)} rows")
        return df
        
    except Exception as e:
        logger.error(f"Error loading {filename}: {e}")
        return None

def get_transport_leadtimes() -> Dict[str, int]:
    """
    Get transport lead times as a dictionary for quick lookup
    Returns: {route_id: leadtime_days}
    """
    cache_key = 'transport_leadtimes_dict'
    
    if cache_key in _config_cache:
        return _config_cache[cache_key]
    
    df = _load_csv_file('transport_leadtimes.csv')
    if df is None:
        logger.warning("Transport leadtimes file not found, using empty dict")
        _config_cache[cache_key] = {}
        return {}
    
    # Convert to dictionary for fast lookup
    transport_dict = {}
    for _, row in df.iterrows():
        try:
            route_id = f"{row['departure_region']}{row['arrival_region']}"
            leadtime = int(row['p2plt_non_air'])
            transport_dict[route_id] = leadtime
        except (ValueError, KeyError) as e:
            logger.warning(f"Invalid transport data: {row.to_dict()}, error: {e}")
    
    _config_cache[cache_key] = transport_dict
    logger.info(f"Cached {len(transport_dict)} transport routes")
    return transport_dict

def get_port_buffer_dict() -> Dict[tuple, int]:
    """
    Get port to channel buffer as dictionary
    Returns: {(wh_type, location): buffer_days}
    """
    cache_key = 'port_buffer_dict'
    
    if cache_key in _config_cache:
        return _config_cache[cache_key]
    
    df = _load_csv_file('port_to_channel_buffer.csv')
    if df is None:
        logger.warning("Port buffer file not found, using empty dict")
        _config_cache[cache_key] = {}
        return {}
    
    buffer_dict = {}
    for _, row in df.iterrows():
        try:
            key = (row['wh_type_LT'], row['WH_Location'])
            buffer_days = int(row['p2pbf'])
            buffer_dict[key] = buffer_days
        except (ValueError, KeyError) as e:
            logger.warning(f"Invalid port buffer data: {row.to_dict()}, error: {e}")
    
    _config_cache[cache_key] = buffer_dict
    logger.info(f"Cached {len(buffer_dict)} port buffer configs")
    return buffer_dict

def get_country_region_dict() -> Dict[str, str]:
    """
    Get country to region mapping
    Returns: {country: region}
    """
    cache_key = 'country_region_dict'
    
    if cache_key in _config_cache:
        return _config_cache[cache_key]
    
    df = _load_csv_file('country_region_mapping.csv')
    if df is None:
        logger.warning("Country region mapping file not found, using empty dict")
        _config_cache[cache_key] = {}
        return {}
    
    country_dict = {}
    for _, row in df.iterrows():
        try:
            country_dict[row['country']] = row['region']
        except KeyError as e:
            logger.warning(f"Invalid country mapping data: {row.to_dict()}, error: {e}")
    
    _config_cache[cache_key] = country_dict
    logger.info(f"Cached {len(country_dict)} country mappings")
    return country_dict

def get_asia_countries_list() -> List[str]:
    """
    Get list of Asia countries
    Returns: List of country names
    """
    cache_key = 'asia_countries_list'
    
    if cache_key in _config_cache:
        return _config_cache[cache_key]
    
    df = _load_csv_file('asia_countries.csv')
    if df is None:
        logger.warning("Asia countries file not found, using empty list")
        _config_cache[cache_key] = []
        return []
    
    try:
        asia_list = df['country'].tolist()
        _config_cache[cache_key] = asia_list
        logger.info(f"Cached {len(asia_list)} Asia countries")
        return asia_list
    except KeyError:
        logger.error("Asia countries CSV missing 'country' column")
        _config_cache[cache_key] = []
        return []

# Main lookup functions for business logic

def get_transport_leadtime(departure_region: str, arrival_region: str, default_days: int = 30) -> int:
    """
    Get transport lead time for a route
    
    Args:
        departure_region: Departure region code
        arrival_region: Arrival region code
        default_days: Default leadtime if route not found
        
    Returns:
        Lead time in days (default if not found)
    """
    transport_dict = get_transport_leadtimes()
    route_id = f"{departure_region}{arrival_region}"
    leadtime = transport_dict.get(route_id)
    
    if leadtime is None:
        logger.warning(f"Transport route {route_id} not found, using default {default_days} days")
        return default_days
    
    return leadtime

def get_port_buffer_days(wh_type: str, location: str) -> int:
    """
    Get port to channel buffer days
    
    Args:
        wh_type: Warehouse type (3PL or AMZ)
        location: Warehouse location
        
    Returns:
        Buffer days (default: 39 if not found)
    """
    buffer_dict = get_port_buffer_dict()
    key = (wh_type, location)
    buffer_days = buffer_dict.get(key)
    
    if buffer_days is None:
        logger.warning(f"Port buffer for {key} not found, using default 39 days")
        return 39
    
    return buffer_days

def get_region_for_country(country: str) -> Optional[str]:
    """
    Get region code for a country
    
    Args:
        country: Country name
        
    Returns:
        Region code or None if not found
    """
    country_dict = get_country_region_dict()
    region = country_dict.get(country)
    
    if region is None:
        logger.warning(f"Region for country '{country}' not found")
    
    return region

def is_asia_country(country: str) -> bool:
    """
    Check if a country is in Asia
    
    Args:
        country: Country name
        
    Returns:
        True if country is in Asia, False otherwise
    """
    asia_countries = get_asia_countries_list()
    return country in asia_countries

# Bulk loading functions for efficient data processing

def load_all_config_data() -> Dict[str, Union[Dict, List]]:
    """
    Load all configuration data at once for batch processing
    Useful when you need all configs and want to minimize function calls
    
    Returns:
        Dictionary with all config data
    """
    return {
        'transport_leadtimes': get_transport_leadtimes(),
        'port_buffers': get_port_buffer_dict(),
        'country_regions': get_country_region_dict(),
        'asia_countries': get_asia_countries_list()
    }

def apply_transport_leadtimes_vectorized(df: pd.DataFrame, 
                                       departure_col: str = 'departure_region',
                                       arrival_col: str = 'arrival_region',
                                       output_col: str = 'transport_leadtime',
                                       default_days: int = 30) -> pd.DataFrame:
    """
    Apply transport lead times to a DataFrame efficiently
    Uses vectorized operations instead of row-by-row lookups
    
    Args:
        df: DataFrame to process
        departure_col: Column name for departure region
        arrival_col: Column name for arrival region
        output_col: Column name for output leadtime
        default_days: Default leadtime for missing routes
        
    Returns:
        DataFrame with transport leadtimes added
    """
    if df.empty:
        return df
    
    transport_dict = get_transport_leadtimes()
    
    # Create route_id column for lookup
    df_copy = df.copy()
    df_copy['_route_id'] = df_copy[departure_col].astype(str) + df_copy[arrival_col].astype(str)
    
    # Map leadtimes using vectorized operation with default
    df_copy[output_col] = df_copy['_route_id'].map(transport_dict).fillna(default_days).astype(int)
    
    # Log missing routes
    missing_routes = df_copy[df_copy[output_col] == default_days]['_route_id'].unique()
    if len(missing_routes) > 0:
        logger.warning(f"Missing transport routes (using {default_days} days): {list(missing_routes)}")
    
    # Drop temporary column
    df_copy.drop('_route_id', axis=1, inplace=True)
    
    return df_copy

def apply_port_buffers_vectorized(df: pd.DataFrame,
                                wh_type_col: str = 'wh_type',
                                location_col: str = 'location',
                                output_col: str = 'port_buffer_days') -> pd.DataFrame:
    """
    Apply port buffer days to a DataFrame efficiently
    
    Args:
        df: DataFrame to process
        wh_type_col: Column name for warehouse type
        location_col: Column name for location
        output_col: Column name for output buffer days
        
    Returns:
        DataFrame with port buffer days added
    """
    if df.empty:
        return df
    
    buffer_dict = get_port_buffer_dict()
    
    df_copy = df.copy()
    
    # Create lookup function for tuple keys
    def lookup_buffer(row):
        key = (row[wh_type_col], row[location_col])
        buffer_days = buffer_dict.get(key)
        if buffer_days is None:
            logger.warning(f"Port buffer for {key} not found, using default 39 days")
            return 39
        return buffer_days
    
    # Apply function (still faster than individual lookups due to batching)
    df_copy[output_col] = df_copy.apply(lookup_buffer, axis=1)
    
    return df_copy

# Backward compatibility functions (for existing code that uses the class)

def load_transport_mappings():
    """Backward compatibility: Load transport mappings"""
    return get_transport_leadtimes(), get_port_buffer_dict()

def get_asia_countries():
    """Backward compatibility: Get Asia countries list"""
    return get_asia_countries_list()

def get_country_to_region_mapping():
    """Backward compatibility: Get country to region mapping"""
    return get_country_region_dict()

# Performance monitoring

def get_cache_info():
    """Get information about cache performance"""
    return {
        'cached_items': list(_config_cache.keys()),
        'cache_size': len(_config_cache),
        'csv_cache_info': _load_csv_file.cache_info()
    }

def validate_config_files():
    """
    Validate all configuration files exist and have correct structure
    Returns dict with validation results
    """
    validation_results = {}
    
    # Required files and their expected columns
    required_files = {
        'transport_leadtimes.csv': ['departure_region', 'arrival_region', 'p2plt_non_air'],
        'port_to_channel_buffer.csv': ['wh_type_LT', 'WH_Location', 'p2pbf'],
        'country_region_mapping.csv': ['country', 'region'],
        'asia_countries.csv': ['country']
    }
    
    for filename, expected_cols in required_files.items():
        filepath = _config_dir / filename
        
        if not filepath.exists():
            validation_results[filename] = {'exists': False, 'valid_columns': False, 'error': 'File not found'}
            continue
        
        try:
            df = pd.read_csv(filepath)
            missing_cols = set(expected_cols) - set(df.columns)
            
            validation_results[filename] = {
                'exists': True,
                'valid_columns': len(missing_cols) == 0,
                'missing_columns': list(missing_cols),
                'row_count': len(df)
            }
        except Exception as e:
            validation_results[filename] = {'exists': True, 'valid_columns': False, 'error': str(e)}
    
    return validation_results

# Example usage and testing
if __name__ == "__main__":
    # Test validation first
    print("Validating configuration files...")
    validation = validate_config_files()
    for file, result in validation.items():
        print(f"{file}: {result}")
    
    # Test all functions
    print("\nTesting optimized config loader...")
    
    # Test individual lookups
    leadtime = get_transport_leadtime('CN', 'US')
    print(f"Transport leadtime CN->US: {leadtime} days")
    
    buffer_days = get_port_buffer_days('3PL', 'US')
    print(f"Port buffer for 3PL-US: {buffer_days} days")
    
    region = get_region_for_country('China')
    print(f"Region for China: {region}")
    
    is_asia = is_asia_country('China')
    print(f"Is China in Asia? {is_asia}")
    
    # Test bulk loading
    all_configs = load_all_config_data()
    print(f"Loaded configs: {list(all_configs.keys())}")
    
    # Show cache performance
    cache_info = get_cache_info()
    print(f"Cache info: {cache_info}")
