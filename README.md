# Inventory Allocation Optimizer

An advanced inventory optimization engine that calculates optimal inventory allocation across regions and channels based on demand forecasts, inbound shipments, and open purchase orders. The system performs complex waterfall calculations to minimize sales lost and optimize inventory positioning.

## Key Features

- **Demand Forecasting**: Processes monthly demand forecasts and distributes to weekly granularity
- **Inventory Waterfall**: Calculates inventory flows, inbound shipments, and open PO impacts across 104 weeks
- **Sales Miss Calculation**: Determines potential lost sales based on inventory constraints
- **Multi-Region Support**: Handles Asia-Pacific countries, channels, and ports with configurable routing rules
- **Redshift Integration**: Direct integration with AWS Redshift data warehouse for real-time data access
- **Configurable Rules**: Business rules, lead times, and channel mappings managed via CSV config files

## Tech Stack

- **Language**: Python 3.x
- **Data Processing**: pandas, NumPy
- **Database**: AWS Redshift (redshift-connector)
- **Configuration**: CSV-based config files, JSON business rules
- **Logging**: Python logging module

## Setup

1. **Clone and create virtual environment**:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Configure Redshift credentials** via environment variables:
   ```bash
   export REDSHIFT_USER=<your_user>
   export REDSHIFT_PASSWORD=<your_password>
   export REDSHIFT_HOST=<redshift_endpoint>
   export REDSHIFT_DATABASE=dev
   export REDSHIFT_PORT=5439
   ```

3. **Verify config files**: Ensure all CSV files in `config/` directory are present:
   - `asia_countries.csv`: Country codes for Asia-Pacific region
   - `country_region_mapping.csv`: Country to region mappings
   - `transport_leadtimes.csv`: Transportation lead times by route
   - `port_to_channel_buffer.csv`: Port to channel buffer levels
   - `p2pbf_mapping.csv`: Port to P2P buffer factor mapping
   - `business_rules.json`: Business rule configurations

4. **Verify SQL queries**: SQL query files in `sql_queries/` must be present for database connectivity

## Usage

Run the optimizer:
```bash
python main.py
```

The script will:
1. Connect to Redshift using environment variable credentials
2. Load all required data (demand, inventory, inbound, open POs, master data, vendor info)
3. Perform inventory allocation calculations
4. Generate output CSV with allocation results in `output/` directory

Output files are named with timestamp: `inventory_allocation_YYYYMMDD_HHMMSS.csv`

## Project Structure

```
├── main.py                      # Main execution script
├── calculations.py              # Core calculation functions (optimized)
├── data_processor.py            # Data transformation and processing
├── database_connector.py         # Redshift connection management
├── config_loader.py             # Configuration file loading
├── sql_query_loader.py          # SQL query management
├── utils.py                     # Utility functions
├── config/                      # Configuration CSV files
├── sql_queries/                 # SQL query files
├── output/                      # Generated allocation results
├── IAM.ipynb                    # Jupyter notebook with analysis
├── project_brain.md             # Project documentation
└── requirements.txt             # Python dependencies
```

## Notes

- Database credentials should never be committed; use environment variables
- Large output CSV files (>500MB) should be managed separately
- The system includes performance optimizations for processing large datasets
- Review `OPTIMIZATION_SUMMARY.md` for latest performance improvements