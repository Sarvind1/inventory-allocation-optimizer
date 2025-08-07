# Inventory Allocation Optimizer

## Overview
Inventory allocation and PO optimization system for supply chain management. This system analyzes inventory positions, calculates potential stockouts, and provides actionable recommendations to minimize revenue loss.

## Features
- **Concurrent Database Loading**: 5-10x faster data loading using parallel queries
- **Sales Missed Calculation**: Identifies products at risk of stockout
- **Revenue Impact Analysis**: Calculates financial impact of stockouts
- **PO Optimization**: Recommends PO splitting between AMZ and 3PL
- **Lead Time Management**: Configurable transport and production lead times
- **Transfer Order Recommendations**: Identifies products needing expedited transfer

## Installation

1. Clone the repository:
```bash
git clone https://github.com/Sarvind1/inventory-allocation-optimizer.git
cd inventory-allocation-optimizer
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure database connection in `config_loader.py`:
```python
config['conn_params'] = {
    'user': 'your_username',
    'password': 'your_password',
    'database': 'your_database',
    'host': 'your_host',
    'port': 5439
}
```

## Usage

Run the main script:
```bash
python main.py
```

The system will:
1. Load configuration files
2. Connect to Redshift and load data (using parallel queries)
3. Process demand, inventory, and PO data
4. Calculate sales missed and revenue impact
5. Generate recommendations
6. Save output to `output/YYYY-MM-DD/final_YY-MM-DD.csv`

## Configuration

### Transport Lead Times
Edit `config/transport_leadtimes.csv` to update shipping lead times:
```csv
shipping_region,arrival_region,leadtime_days
CN,US,39
CN,EU,42
CN,UK,34
```

### Business Rules
Edit `config/business_rules.json` to update business logic:
```json
{
  "asia_countries": ["China", "Hong Kong", ...],
  "po_splitting": {
    "max_cartons_for_3pl": 5,
    "default_lead_time_days": 45
  }
}
```

## Output Files

- **Final Report**: `output/YYYY-MM-DD/final_YY-MM-DD.csv`
- **Debug Files**: `output/YYYY-MM-DD/debug/` (if enabled)

## Key Metrics

- **OOS Week**: Week when product goes out of stock
- **DOH (Days on Hand)**: Days until stockout
- **Revenue Miss**: Lost revenue due to stockouts
- **TO Check**: Products needing transfer order review
- **FFW + Supply Ops**: Products needing expedited shipping

## Performance

- Sequential loading: ~60-90 seconds
- Parallel loading: ~10-15 seconds (5-10x improvement)
- Total execution: ~20-30 seconds for 5000 products

## Troubleshooting

### Database Connection Issues
- Verify credentials in `config_loader.py`
- Check network connectivity to Redshift
- Ensure proper IAM permissions

### Missing Data
- Check that all required tables exist in Redshift
- Verify query permissions
- Review debug files in `output/debug/`

### Performance Issues
- Adjust `max_workers` in `load_all_data_concurrent()`
- Optimize queries in `database_loader.py`
- Consider indexing database tables

## License
MIT License

## Contact
For questions or support, please contact the development team.