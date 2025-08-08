"""
Optimized Configuration Loader - Notebook Aligned
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
    if hasattr(_load_csv_file, 'cache_clear'):
        _load_csv_file.cache_clear()
    logger.info("Configuration cache cleared")

@lru_cache(maxsize=128)
def _load_csv_file(filename: str) -> Optional[pd.DataFrame]:
    """Load a single CSV file with caching"""
    filepath = _config_dir / filename
    
    if not filepath.exists():
        logger.warning(f"Configuration file not found: {filepath}")
        return None
    
    try:
        df = pd.read_csv(filepath, dtype=str, na_filter=False)
        
        # Strip whitespace from all string columns
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()
        
        logger.info(f"Loaded {filename}: {len(df)} rows")
        return df
        
    except Exception as e:
        logger.error(f"Error loading {filename}: {e}")
        return None

# Main data loading functions matching notebook structure

def get_port_to_channel_buffer():
    """Load port to channel buffer data as used in notebook"""
    cache_key = 'p2pbf'
    
    if cache_key in _config_cache:
        return _config_cache[cache_key]
    
    # Use the hardcoded data from notebook
    port_to_channel_buffer = {
        'wh_type_LT': [
            '3PL', '3PL', '3PL', 'AMZ', '3PL', '3PL',
            'AMZ', '3PL', 'AMZ', '3PL', '3PL', 'AMZ'
        ],
        'WH_Location': [
            'US', 'CO', 'MX', 'US', 'BR', 'EU',
            'EU', 'CA', 'UK', 'UK', 'Other', 'CA'
        ],
        'p2pbf': [
            39, 39, 39, 25, 39, 40,
            26, 39, 22, 39, 39, 25
        ]
    }
    
    p2pbf = pd.DataFrame(port_to_channel_buffer)
    _config_cache[cache_key] = p2pbf
    return p2pbf

def get_transport_leadtimes():
    """Load transport leadtimes as used in notebook"""
    cache_key = 'p2plt_non_air'
    
    if cache_key in _config_cache:
        return _config_cache[cache_key]
    
    # Use the hardcoded data from notebook
    data = {
        'index': list(range(1, 62)),
        'departure_region': ['CN', 'CN', 'CN', 'CN', 'IN', 'IN', 'IN', 'EU', 'UK', 'US', 'US', 'CN', 'CN', 'CN', 'CN', 'US', 'US', 'US', 'US', 'EU', 'EU', 'EU', 'EU', 'EU', 'MX', 'CO', 'BR', 'MX', 'CA', 'MX', 'IN', 'MX', 'IN', 'MX', 'MX', 'BR', 'BR', 'UK', 'EU', 'CN', 'CN', 'US', 'CN', 'US', 'AU', 'Other', 'EU', 'UK', 'BR', 'MX', 'CO', 'IN', 'CA','CN','UK','CA','IN','JP','CA','CA','UK'],
        'arrival_region': ['US', 'EU', 'UK', 'Asia', 'US', 'EU', 'UK', 'US', 'US', 'UK', 'EU', 'MX', 'CO', 'BR', 'CA', 'MX', 'CO', 'BR', 'CA', 'MX', 'CO', 'BR', 'CA', 'UK', 'US', 'US', 'BR', 'CA', 'MX', 'EU', 'CA', 'BR', 'MX', 'UK', 'CO', 'EU', 'UK', 'CA', 'Other', 'AU', 'Other', 'AU', 'CN', 'US', 'AU', 'Other', 'EU', 'UK', 'BR', 'MX', 'CO', 'IN', 'CA','JP','CO','US','AU','US','CO','BR','EU'],
        'p2plt_non_air': [39, 42, 34, 23, 45, 33, 26, 40, 36, 52, 20, 39, 39, 39, 39, 15, 15, 15, 15, 40, 40, 40, 40, 20, 15, 15, 15, 15, 15, 40, 45, 14, 45, 52, 30, 40, 52, 36, 45, 23, 40, 40, 6, 6, 6, 7, 7, 2, 2, 2, 2, 2, 2,10,39,10,20,39,20,20,7],
        'id_route': ['CNUS', 'CNEU', 'CNUK', 'CNAsia', 'INUS', 'INEU', 'INUK', 'EUUS', 'UKUS', 'USUK', 'USEU', 'CNMX', 'CNCO', 'CNBR', 'CNCA', 'USMX', 'USCO', 'USBR', 'USCA', 'EUMX', 'EUCO', 'EUBR', 'EUCA', 'EUUK', 'MXUS', 'COUS', 'BRUS', 'MXCA', 'CAMX', 'MXEU', 'INCA', 'MXBR', 'INMX', 'MXUK', 'MXCO', 'BREU', 'BRUK', 'UKCA', 'EUOther', 'CNAU', 'CNOther', 'USAU', 'CNCN', 'USUS', 'AU', 'OtherOther', 'EUEU', 'UKUK', 'BRBR', 'MXMX', 'COCO', 'ININ', 'CACA','CNJP','UKCO','CAUS','INAU','JPUS','CACO','CABR','UKEU']
    }
    
    p2plt_non_air = pd.DataFrame(data)
    p2plt_non_air.set_index('index', inplace=True)
    _config_cache[cache_key] = p2plt_non_air
    return p2plt_non_air

def get_country_region_mapping():
    """Get country to region mapping as used in notebook"""
    cache_key = 'country_to_region'
    
    if cache_key in _config_cache:
        return _config_cache[cache_key]
    
    # Use the hardcoded mapping from notebook
    country_to_region = {
        "China": "CN", "Hong Kong": "CN", "Poland": "EU", "United Kingdom": "UK",
        "Germany": "EU", "United States": "US", "Ukraine": "EU", "Singapore": "CN",
        "Italy": "EU", "Czechia": "EU", "Nepal": "CN", "Australia": "AU",
        "Canada": "CA", "Tunisia": "CN", "India": "IN", "Viet Nam": "CN",
        "Taiwan (Province of China)": "CN", "Türkiye": "EU", "Austria": "EU",
        "Netherlands": "EU", "Ireland": "EU", "Luxembourg": "EU", "Switzerland": "EU",
        "Spain": "EU", "France": "EU", "Eswatini": "CN", "Sweden": "EU",
        "Philippines": "CN", "Portugal": "EU", "Gabon": "CN", "Denmark": "EU",
        "Israel": "EU", "Malaysia": "CN", "Argentina": "BR", "Pakistan": "CN",
        "Japan": "JP", "Romania": "EU", "Georgia": "EU", "Bulgaria": "EU",
        "Lithuania": "EU", "Hungary": "EU", "Belgium": "EU", "Finland": "EU",
        "Thailand": "CN", "Kosovo": "EU", "Mexico": "MX", "Brazil": "BR",
        "Colombia": "CO", "Indonesia": "CN", "United Arab Emirates": "CN",
        "Estonia": "EU", "Slovenia": "EU", "Sri Lanka": "CN", "Slovakia": "EU",
        "Korea (the Republic of)": "CN", "Greece": "EU", "Latvia": "EU", "Malta": "EU"
    }
    
    _config_cache[cache_key] = country_to_region
    return country_to_region

def get_asia_countries():
    """Get Asia countries list as used in notebook"""
    cache_key = 'asia_countries'
    
    if cache_key in _config_cache:
        return _config_cache[cache_key]
    
    # Use the hardcoded list from notebook
    asia_countries = [
        "China", "Hong Kong", "Malaysia", "Taiwan (Province of China)",
        "Viet Nam", "Korea (the Republic of)", "Singapore", "Japan"
    ]
    
    _config_cache[cache_key] = asia_countries
    return asia_countries

def get_transport_map():
    """Get transport map as dictionary for notebook compatibility"""
    cache_key = 'transport_map'
    
    if cache_key in _config_cache:
        return _config_cache[cache_key]
    
    # Use the hardcoded transport map from notebook
    transport_map = {
        ('CN', 'US'): 39, ('CN', 'EU'): 42, ('CN', 'UK'): 34, ('CN', 'Asia'): 23,
        ('IN', 'US'): 45, ('IN', 'EU'): 33, ('IN', 'UK'): 26, ('EU', 'US'): 40,
        ('UK', 'US'): 36, ('US', 'UK'): 52, ('US', 'EU'): 20, ('CN', 'MX'): 39,
        ('CN', 'CO'): 39, ('CN', 'BR'): 39, ('CN', 'CA'): 39, ('US', 'MX'): 15,
        ('US', 'CO'): 15, ('US', 'BR'): 15, ('US', 'CA'): 15, ('EU', 'MX'): 40,
        ('EU', 'CO'): 40, ('EU', 'BR'): 40, ('EU', 'CA'): 40, ('MX', 'CN'): 20,
        ('MX', 'UK'): 15, ('CO', 'MX'): 15, ('BR', 'MX'): 15, ('BR', 'CO'): 15,
        ('BR', 'EU'): 15, ('BR', 'IN'): 15, ('UK', 'CA'): 15, ('UK', 'JP'): 15,
        ('AU', 'Other'): 15, ('Other', 'AU'): 15, ('EU', 'JP'): 10, ('CO', 'EU'): 39,
        ('CN', 'CN'): 39, ('US', 'CN'): 39, ('CN', 'CO'): 39, ('CN', 'BR'): 39,
        ('CN', 'MX'): 39, ('BR', 'BR'): 15, ('MX', 'BR'): 15, ('BR', 'MX'): 15,
        ('EU', 'UK'): 40, ('US', 'MX'): 45, ('CA', 'MX'): 40, ('IN', 'MX'): 40,
        ('MX', 'MX'): 15, ('BR', 'CA'): 15, ('MX', 'CA'): 40, ('IN', 'US'): 45,
        ('EU', 'MX'): 40, ('CO', 'BR'): 39, ('EU', 'CO'): 39, ('MX', 'CO'): 15,
        ('BR', 'US'): 15, ('US', 'BR'): 40, ('BR', 'CO'): 15, ('CA', 'EU'): 40,
        ('CO', 'US'): 15, ('MX', 'US'): 40, ('CO', 'MX'): 15, ('CO', 'CA'): 39,
        ('BR', 'CO'): 15, ('EU', 'MX'): 40, ('IN', 'CO'): 15, ('IN', 'BR'): 15,
        ('BR', 'MX'): 15, ('CN', 'JP'): 20, ('BR', 'JP'): 15, ('IN', 'JP'): 15,
        ('AU', 'CN'): 15, ('EU', 'AU'): 7, ('MX', 'AU'): 7, ('UK', 'AU'): 2,
        ('CO', 'JP'): 2, ('EU', 'AU'): 7, ('CO', 'MX'): 7,('US', 'US'): 7,
        ('US', 'CA'): 7,('CA', 'UK'): 15,('EU', 'EU'): 7,('CA', 'US'): 2,
        ('IN', 'CA'): 45,('CA', 'CA'): 2,('CA', 'BR'): 40,('CA', 'MX'): 40
    }
    
    _config_cache[cache_key] = transport_map
    return transport_map

# Lookup functions for business logic (matching notebook usage)

def get_transport_leadtime(departure_region: str, arrival_region: str, default_days: int = 45) -> int:
    """Get transport lead time for a route"""
    transport_map = get_transport_map()
    return transport_map.get((departure_region, arrival_region), default_days)

def get_port_buffer_days(wh_type: str, location: str) -> int:
    """Get port to channel buffer days"""
    p2pbf = get_port_to_channel_buffer()
    
    # Create lookup from DataFrame
    for _, row in p2pbf.iterrows():
        if row['wh_type_LT'] == wh_type and row['WH_Location'] == location:
            return int(row['p2pbf'])
    
    # Default mapping from notebook
    mp_mapping = {
        'US': 39, 'CO': 39, 'MX': 39, 'CA': 39,
        'UK': 39, 'BR': 36, 'EU': 40, 'Other': 39
    }
    return mp_mapping.get(location, 39)

def get_region_for_country(country: str) -> Optional[str]:
    """Get region code for a country"""
    country_dict = get_country_region_mapping()
    return country_dict.get(country)

def is_asia_country(country: str) -> bool:
    """Check if a country is in Asia"""
    asia_countries = get_asia_countries()
    return country in asia_countries

# Backward compatibility functions

def load_transport_mappings():
    """Backward compatibility: Load transport mappings"""
    return get_transport_leadtimes(), get_port_to_channel_buffer()

# Performance monitoring

def get_cache_info():
    """Get information about cache performance"""
    return {
        'cached_items': list(_config_cache.keys()),
        'cache_size': len(_config_cache),
        'csv_cache_info': _load_csv_file.cache_info() if hasattr(_load_csv_file, 'cache_info') else None
    }

# Example usage and testing
if __name__ == "__main__":
    # Test all functions
    print("Testing notebook-aligned config loader...")
    
    # Test individual lookups
    leadtime = get_transport_leadtime('CN', 'US')
    print(f"Transport leadtime CN->US: {leadtime} days")
    
    buffer_days = get_port_buffer_days('3PL', 'US')
    print(f"Port buffer for 3PL-US: {buffer_days} days")
    
    region = get_region_for_country('China')
    print(f"Region for China: {region}")
    
    is_asia = is_asia_country('China')
    print(f"Is China in Asia? {is_asia}")
    
    # Show cache performance
    cache_info = get_cache_info()
    print(f"Cache info: {cache_info}")
