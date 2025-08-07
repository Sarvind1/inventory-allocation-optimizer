"""
Database loading functions with concurrent execution
Simplified version without classes
"""

import pandas as pd
import redshift_connector
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

def create_connection(conn_params):
    """Create a database connection"""
    try:
        conn = redshift_connector.connect(**conn_params)
        return conn
    except Exception as e:
        print(f"Connection error: {e}")
        return None

def execute_query(conn_params, query_name, query):
    """Execute a single query and return dataframe"""
    conn = None
    try:
        conn = create_connection(conn_params)
        if conn:
            start = datetime.now()
            df = pd.read_sql(query, conn)
            elapsed = (datetime.now() - start).total_seconds()
            print(f"  ✓ {query_name}: {len(df)} rows in {elapsed:.1f}s")
            return query_name, df
    except Exception as e:
        print(f"  ✗ {query_name} failed: {e}")
        return query_name, pd.DataFrame()
    finally:
        if conn:
            conn.close()

def load_all_data_concurrent(conn_params, max_workers=4):
    """
    Load all required data using parallel queries
    This is 5-10x faster than sequential loading
    """
    
    # Define all queries
    queries = get_all_queries()
    
    # Execute queries in parallel
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all queries
        futures = [
            executor.submit(execute_query, conn_params, name, query) 
            for name, query in queries.items()
        ]
        
        # Collect results as they complete
        for future in as_completed(futures):
            query_name, df = future.result()
            results[query_name] = df
    
    # Post-process specific datasets
    results = post_process_data(results)
    
    return results

def get_all_queries():
    """Return all SQL queries as a dictionary"""
    
    queries = {}
    
    # ASIN Vendor Query
    queries['asin_vendor'] = """
    WITH vendor_ranked AS (
        SELECT 
            name AS razin, 
            vendor,
            preferred_vendor,
            ROW_NUMBER() OVER (PARTITION BY name ORDER BY vendor) AS row_num
        FROM razor_db.core.razin_subsidiary_vendor_master_mapping
    ),
    ranked_orders AS (
        SELECT 
            item AS razin, 
            po_vendor, 
            ROW_NUMBER() OVER (PARTITION BY item ORDER BY ordered_at DESC) AS rn
        FROM razor_db.public.rgbit_netsuite_purchase_orders_lineitems_withkey
        WHERE vendor_category <> 'Intercompany'
    ),
    razin_master AS (
        SELECT 
            name AS razin, 
            asin_number AS asin,
            ROW_NUMBER() OVER (PARTITION BY name ORDER BY snapshot_date DESC) AS row_num
        FROM razor_db.core.razin_master_data
    )
    SELECT 
        v.razin, 
        rm.asin, 
        COALESCE(NULLIF(v.preferred_vendor, ''),v.vendor, r.po_vendor) AS final_vendor  
    FROM vendor_ranked v
    LEFT JOIN ranked_orders r ON v.razin = r.razin AND r.rn = 1
    LEFT JOIN razin_master rm ON v.razin = rm.razin AND rm.row_num = 1
    WHERE v.row_num = 1
    ORDER BY v.razin
    """
    
    # Target Sales Price Query
    queries['target_sp'] = """
    WITH filtered_data AS (
        SELECT asin, sales_channel, marketplace, gross_asp_l30
        FROM razor_db.public.rgbit_asp_l30_all_channels_w_fallbacks
        WHERE gross_asp_l30 IS NOT NULL AND gross_asp_l30 > 0
    ),
    aggregated_data AS (
        SELECT asin, marketplace, 
               MAX(gross_asp_l30) AS gross_asp_l30
        FROM filtered_data
        GROUP BY asin, marketplace
    ),
    latest_data AS (
        SELECT asin, market_reporting, asp_benchmark,
               ROW_NUMBER() OVER (PARTITION BY asin, market_reporting ORDER BY date DESC) AS rn
        FROM razor_db.forecast_reporting.system_forecast_reporting
        WHERE asp_benchmark IS NOT NULL
    )
    SELECT 
        l.asin || (CASE WHEN l.market_reporting = 'Pan-EU' THEN 'EU' ELSE l.market_reporting END) AS ref,
        COALESCE(l.asp_benchmark, a.gross_asp_l30, 0) AS final_sales_price
    FROM (
        SELECT asin, market_reporting, asp_benchmark
        FROM latest_data WHERE rn = 1
    ) l
    LEFT JOIN aggregated_data a
    ON l.asin = a.asin AND l.market_reporting = a.marketplace
    """
    
    # Demand Query
    queries['demand'] = """
    WITH RankedCTE AS (
        SELECT 
            CASE WHEN marketplace IN ('DE', 'Pan-EU') THEN 'EU' ELSE marketplace END AS mp,
            razin, asin,
            CAST(date AS DATE) AS date,
            SUM(future_sale) AS quantity
        FROM razor_db.public.rgbit_po_calendar_bm_saleplan_snapshot
        WHERE razin <> '' 
          AND snapshot_date = (
              SELECT MAX(snapshot_date) 
              FROM razor_db.public.rgbit_po_calendar_bm_saleplan_snapshot 
              WHERE review_cycle_version = 'Validated_plan'
          )
          AND review_cycle_version = 'Validated_plan'
          AND CAST(date AS DATE) BETWEEN date_trunc('month', CURRENT_DATE) - interval '2 months'
                                     AND date_trunc('year', CURRENT_DATE + interval '1 year') + interval '1 year' - interval '1 day'
        GROUP BY mp, razin, asin, date
    )
    SELECT mp, razin, asin, quantity, date
    FROM RankedCTE
    ORDER BY razin, mp, quantity
    """
    
    # Master Data Query
    queries['master_data'] = """
    WITH classified AS (
        SELECT DISTINCT 
            name AS razin, 
            asin_number AS asin,
            COALESCE(lead_time_production_days, 45) AS lead_time_production_days,
            parcels_per_master_carton AS units_per_carton, 
            master_carton_weight_kg, 
            master_carton_volume,
            preferred_vendor,
            CASE
                WHEN ea_product_package_weight <= 31.75 THEN 'Standard'
                ELSE 'Oversize'
            END AS size_tier,
            dense_rank() OVER (PARTITION BY name ORDER BY snapshot_date DESC) AS rnk
        FROM razor_db.core.razin_master_data
        WHERE COALESCE(successor_razin, '') = ''
    )
    SELECT razin, asin, lead_time_production_days, units_per_carton, 
           preferred_vendor, master_carton_weight_kg, master_carton_volume, size_tier
    FROM classified
    WHERE rnk = 1
    """
    
    # Add other queries here...
    queries['gfl_list'] = """
    SELECT DISTINCT asin, country_code, brand_name, razin, marketplace
    FROM razor_db.core.amazon_product
    WHERE go_forward = 1
    """
    
    queries['product_market'] = """
    WITH ranked_vendors AS (
        SELECT vendor_id, vendor_name, port_of_departure AS pol,
               supplier_manager, category_manager, country AS shipping_country,
               ROW_NUMBER() OVER (PARTITION BY vendor_id ORDER BY snapshot_date DESC) AS rn
        FROM razor_db.core.vendor_master_data
        WHERE supplier_category IN ('Manufacturers','Manufacturers / LP')
    )
    SELECT vendor_id, vendor_name, pol, supplier_manager, category_manager, shipping_country
    FROM ranked_vendors WHERE rn = 1
    """
    
    # Add remaining queries...
    
    return queries

def post_process_data(results):
    """Post-process loaded data"""
    
    # Process ASIN vendor data
    if 'asin_vendor' in results and not results['asin_vendor'].empty:
        df = results['asin_vendor']
        df['asin'] = df['asin'].replace('', pd.NA).fillna(df['razin'])
        
        # Group by ASIN
        def select_valid_vendor(vendors):
            valid = [v for v in vendors if v and str(v).strip()]
            return valid[0] if valid else None
        
        df = df.groupby('asin', as_index=False).agg({
            'razin': lambda x: ','.join(x),
            'final_vendor': select_valid_vendor
        })
        
        df.rename(columns={'razin': 'concatenated_razins', 'asin': 'asin_razin'}, inplace=True)
        df = df[df['final_vendor'].notna() & (df['final_vendor'] != '')]
        df['vendor_id'] = df['final_vendor'].str[:5]
        results['asin_vendor'] = df[['asin_razin', 'vendor_id']]
    
    # Process GFL list
    if 'gfl_list' in results and not results['gfl_list'].empty:
        df = results['gfl_list']
        df['mp'] = df['marketplace'].replace('Pan-EU', 'EU')
        df['ref'] = df['asin'] + df['mp']
        df['gfl_list'] = 'Yes'
        results['gfl_list'] = df[['ref', 'gfl_list']].drop_duplicates(subset='ref', keep='first')
    
    # Process product market
    if 'product_market' in results and not results['product_market'].empty:
        df = results['product_market']
        df = df[df['vendor_id'].astype(str).str.startswith('7')]
        
        # Map countries to regions
        country_to_region = {
            "China": "CN", "Hong Kong": "CN", "Poland": "EU", "United Kingdom": "UK",
            "Germany": "EU", "United States": "US", "India": "IN", "Canada": "CA",
            "Mexico": "MX", "Brazil": "BR", "Colombia": "CO"
        }
        df['shipping_region'] = df['shipping_country'].map(country_to_region).fillna('CN')
        results['product_market'] = df.drop_duplicates(subset='vendor_id')
    
    return results