"""
Enhanced Configuration Loader
Loads configuration data from CSV files for better maintainability
"""

import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class ConfigLoader:
    """Loads and manages configuration data from CSV files"""
    
    def __init__(self, config_dir='config'):
        """
        Initialize the configuration loader
        
        Args:
            config_dir: Directory containing configuration CSV files
        """
        self.config_dir = Path(config_dir)
        self.transport_leadtimes = None
        self.port_to_channel_buffer = None
        self.country_region_mapping = None
        self.asia_countries = None
        self._load_all_configs()
    
    def _load_all_configs(self):
        """Load all configuration files"""
        try:
            # Load transport lead times
            self.transport_leadtimes = self._load_csv('transport_leadtimes.csv')
            if self.transport_leadtimes is not None:
                # Create id_route as index for quick lookup
                self.transport_leadtimes.set_index('id_route', inplace=True)
                logger.info(f"Loaded {len(self.transport_leadtimes)} transport routes")
            
            # Load port to channel buffer
            self.port_to_channel_buffer = self._load_csv('port_to_channel_buffer.csv')
            if self.port_to_channel_buffer is not None:
                logger.info(f"Loaded {len(self.port_to_channel_buffer)} port buffer configs")
            
            # Load country to region mapping
            self.country_region_mapping = self._load_csv('country_region_mapping.csv')
            if self.country_region_mapping is not None:
                # Create dictionary for quick lookup
                self.country_to_region_dict = dict(
                    zip(self.country_region_mapping['country'], 
                        self.country_region_mapping['region'])
                )
                logger.info(f"Loaded {len(self.country_region_mapping)} country mappings")
            
            # Load Asia countries list
            self.asia_countries = self._load_csv('asia_countries.csv')
            if self.asia_countries is not None:
                self.asia_countries_list = self.asia_countries['country'].tolist()
                logger.info(f"Loaded {len(self.asia_countries_list)} Asia countries")
            
        except Exception as e:
            logger.error(f"Error loading configurations: {e}")
    
    def _load_csv(self, filename):
        """Load a single CSV file"""
        filepath = self.config_dir / filename
        if filepath.exists():
            try:
                df = pd.read_csv(filepath)
                logger.info(f"Successfully loaded {filename}")
                return df
            except Exception as e:
                logger.error(f"Error loading {filename}: {e}")
                return None
        else:
            logger.warning(f"Configuration file not found: {filepath}")
            return None
    
    def get_transport_leadtime(self, departure_region, arrival_region):
        """
        Get transport lead time for a specific route
        
        Args:
            departure_region: Departure region code
            arrival_region: Arrival region code
            
        Returns:
            Lead time in days or None if route not found
        """
        route_id = f"{departure_region}{arrival_region}"
        try:
            return self.transport_leadtimes.loc[route_id, 'p2plt_non_air']
        except:
            logger.warning(f"Route not found: {route_id}")
            return None
    
    def get_p2pbf(self, wh_type, location):
        """
        Get port to channel buffer days
        
        Args:
            wh_type: Warehouse type (3PL or AMZ)
            location: Warehouse location
            
        Returns:
            Buffer days or default value
        """
        try:
            mask = (self.port_to_channel_buffer['wh_type_LT'] == wh_type) & \
                   (self.port_to_channel_buffer['WH_Location'] == location)
            result = self.port_to_channel_buffer[mask]['p2pbf'].values
            if len(result) > 0:
                return result[0]
        except:
            pass
        
        # Return default value
        return 39
    
    def get_region_for_country(self, country):
        """
        Get region code for a country
        
        Args:
            country: Country name
            
        Returns:
            Region code or None if not found
        """
        return self.country_to_region_dict.get(country)
    
    def is_asia_country(self, country):
        """
        Check if a country is in Asia
        
        Args:
            country: Country name
            
        Returns:
            True if country is in Asia, False otherwise
        """
        return country in self.asia_countries_list
    
    def get_transport_map_dict(self):
        """
        Get transport lead times as a dictionary for compatibility
        
        Returns:
            Dictionary with (departure, arrival) tuples as keys
        """
        transport_map = {}
        for idx, row in self.transport_leadtimes.iterrows():
            key = (row['departure_region'], row['arrival_region'])
            transport_map[key] = row['p2plt_non_air']
        return transport_map
    
    def reload_configs(self):
        """Reload all configuration files"""
        logger.info("Reloading all configuration files...")
        self._load_all_configs()

# Convenience functions for backward compatibility
def load_transport_mappings():
    """Load transport mappings from CSV files"""
    loader = ConfigLoader()
    return loader.transport_leadtimes, loader.port_to_channel_buffer

def get_asia_countries():
    """Get list of Asia countries"""
    loader = ConfigLoader()
    return loader.asia_countries_list

def get_country_to_region_mapping():
    """Get country to region mapping dictionary"""
    loader = ConfigLoader()
    return loader.country_to_region_dict

# Example usage
if __name__ == "__main__":
    # Initialize config loader
    config = ConfigLoader()
    
    # Example: Get transport lead time
    leadtime = config.get_transport_leadtime('CN', 'US')
    print(f"Lead time from CN to US: {leadtime} days")
    
    # Example: Get region for country
    region = config.get_region_for_country('China')
    print(f"Region for China: {region}")
    
    # Example: Check if country is in Asia
    is_asia = config.is_asia_country('China')
    print(f"Is China in Asia? {is_asia}")