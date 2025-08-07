"""
Configuration loader - handles all configuration files
"""

import pandas as pd
import json
from pathlib import Path

def load_config_files(config_dir="config"):
    """Load all configuration files"""
    
    config_path = Path(config_dir)
    config_path.mkdir(exist_ok=True)
    
    config = {}
    
    # Database connection parameters
    config['conn_params'] = {
        'user': 'manh.nguyen@razor-group.com',
        'password': 'qdkcTHB8CPfe7AQHVNEF',
        'database': 'dev',
        'host': 'datawarehouse-dev.cdg4y3yokxle.eu-central-1.redshift.amazonaws.com',
        'port': 5439
    }
    
    # Load transport map
    transport_file = config_path / "transport_leadtimes.csv"
    if not transport_file.exists():
        create_default_transport_map(transport_file)
    
    df_transport = pd.read_csv(transport_file)
    config['transport_map'] = {
        (row['shipping_region'], row['arrival_region']): row['leadtime_days']
        for _, row in df_transport.iterrows()
    }
    
    # Load p2pbf map
    p2pbf_file = config_path / "p2pbf_mapping.csv"
    if not p2pbf_file.exists():
        create_default_p2pbf(p2pbf_file)
    
    df_p2pbf = pd.read_csv(p2pbf_file)
    config['p2pbf_map'] = {
        (row['wh_type'], row['location']): row['buffer_days']
        for _, row in df_p2pbf.iterrows()
    }
    
    # Load business rules
    rules_file = config_path / "business_rules.json"
    if not rules_file.exists():
        create_default_business_rules(rules_file)
    
    with open(rules_file, 'r') as f:
        config['business_rules'] = json.load(f)
    
    # Marketplace mapping
    config['mp_mapping'] = {
        'US': 39, 'CO': 39, 'MX': 39, 'CA': 39,
        'UK': 39, 'BR': 36, 'EU': 40, 'Other': 39
    }
    
    # Debug settings
    config['save_debug_files'] = True
    
    return config

def create_default_transport_map(file_path):
    """Create default transport lead times CSV"""
    
    data = [
        ('CN', 'US', 39), ('CN', 'EU', 42), ('CN', 'UK', 34), ('CN', 'Asia', 23),
        ('IN', 'US', 45), ('IN', 'EU', 33), ('IN', 'UK', 26), ('EU', 'US', 40),
        ('UK', 'US', 36), ('US', 'UK', 52), ('US', 'EU', 20), ('CN', 'MX', 39),
        ('CN', 'CO', 39), ('CN', 'BR', 39), ('CN', 'CA', 39), ('US', 'MX', 15),
        ('US', 'CO', 15), ('US', 'BR', 15), ('US', 'CA', 15), ('EU', 'MX', 40),
        ('EU', 'CO', 40), ('EU', 'BR', 40), ('EU', 'CA', 40), ('MX', 'CN', 20),
        ('MX', 'UK', 15), ('CO', 'MX', 15), ('BR', 'MX', 15), ('BR', 'CO', 15),
        ('BR', 'EU', 15), ('BR', 'IN', 15), ('UK', 'CA', 15), ('UK', 'JP', 15),
        ('AU', 'Other', 15), ('Other', 'AU', 15), ('EU', 'JP', 10), ('CO', 'EU', 39),
        ('CN', 'CN', 39), ('US', 'CN', 39), ('BR', 'BR', 15), ('MX', 'BR', 15),
        ('EU', 'UK', 40), ('US', 'US', 7), ('EU', 'EU', 7), ('CA', 'CA', 2),
        ('CA', 'US', 2), ('CA', 'UK', 15), ('CA', 'EU', 40), ('CA', 'BR', 40),
        ('CA', 'MX', 40), ('IN', 'CA', 45), ('IN', 'CO', 15), ('IN', 'BR', 15),
        ('IN', 'MX', 40), ('MX', 'MX', 15), ('CO', 'CO', 7), ('UK', 'UK', 2),
        ('BR', 'US', 15), ('BR', 'CA', 15), ('CO', 'US', 15), ('CO', 'CA', 39),
        ('MX', 'US', 40), ('MX', 'CA', 40), ('MX', 'CO', 15), ('AU', 'CN', 15),
        ('EU', 'AU', 7), ('MX', 'AU', 7), ('UK', 'AU', 2), ('CO', 'JP', 2),
        ('CN', 'JP', 20), ('BR', 'JP', 15), ('IN', 'JP', 15), ('IN', 'Asia', 23),
        ('JP', 'US', 39), ('CA', 'CO', 20), ('UK', 'EU', 7)
    ]
    
    df = pd.DataFrame(data, columns=['shipping_region', 'arrival_region', 'leadtime_days'])
    df.to_csv(file_path, index=False)
    print(f"Created transport map: {file_path}")

def create_default_p2pbf(file_path):
    """Create default p2pbf mapping CSV"""
    
    data = [
        ('3PL', 'US', 39), ('3PL', 'CO', 39), ('3PL', 'MX', 39),
        ('AMZ', 'US', 25), ('3PL', 'BR', 39), ('3PL', 'EU', 40),
        ('AMZ', 'EU', 26), ('3PL', 'CA', 39), ('AMZ', 'UK', 22),
        ('3PL', 'UK', 39), ('3PL', 'Other', 39), ('AMZ', 'CA', 25)
    ]
    
    df = pd.DataFrame(data, columns=['wh_type', 'location', 'buffer_days'])
    df.to_csv(file_path, index=False)
    print(f"Created p2pbf map: {file_path}")

def create_default_business_rules(file_path):
    """Create default business rules JSON"""
    
    rules = {
        "asia_countries": [
            "China", "Hong Kong", "Malaysia", "Taiwan (Province of China)",
            "Viet Nam", "Korea (the Republic of)", "Singapore", "Japan"
        ],
        "po_splitting": {
            "max_cartons_for_3pl": 5,
            "default_lead_time_days": 45,
            "po_processing_time": 15
        },
        "inventory_thresholds": {
            "min_units_per_carton": 1,
            "default_units_per_carton": 10
        },
        "marketplace_mappings": {
            "Pan-EU": "EU",
            "DE": "EU",
            "GB": "UK",
            "North America": "US"
        },
        "signed_stages": [
            "12. Ready for Batching Pending",
            "13. Batch Creation Pending",
            "14. SM Sign-Off Pending",
            "15. CI Approval Pending",
            "16. CI Payment Pending",
            "17. QC Schedule Pending",
            "18. FFW Booking Missing",
            "19. Supplier Pickup Date Pending",
            "20. Pre Pickup Check",
            "21. FOB Pickup Pending",
            "22. Non FOB Pickup Pending",
            "23. INB Creation Pending"
        ],
        "unsigned_stages": [
            "01. PO Approval Pending",
            "02. Supplier Confirmation Pending",
            "03. PI Upload Pending",
            "04. PI Approval Pending",
            "05. PI Payment Pending",
            "06. Packaging Pending",
            "07. Transperancy Label Pending",
            "08. PRD Pending",
            "09. Under Production",
            "10. PRD Confirmation Pending",
            "11. IM Sign-Off Pending",
            "A. Anti PO Line",
            "B. Compliance Blocked"
        ]
    }
    
    with open(file_path, 'w') as f:
        json.dump(rules, f, indent=2)
    print(f"Created business rules: {file_path}")