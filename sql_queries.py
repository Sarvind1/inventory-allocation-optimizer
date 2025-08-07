"""
Complete SQL queries for inventory allocation
All queries from the notebook are included here
"""

def get_all_sql_queries():
    """Return all SQL queries as a dictionary"""
    
    queries = {}
    
    # 1. ASIN Vendor Mapping Query
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
    
    # 2. Target Sales Price Query
    queries['target_sp'] = """
    WITH filtered_data AS (
        SELECT asin, sales_channel, marketplace, gross_asp_l30
        FROM razor_db.public.rgbit_asp_l30_all_channels_w_fallbacks
        WHERE gross_asp_l30 IS NOT NULL AND gross_asp_l30 > 0
    ),
    aggregated_data AS (
        SELECT asin, marketplace, 
               LISTAGG(sales_channel, ', ') WITHIN GROUP (ORDER BY sales_channel) AS sales_channel_list,
               MAX(gross_asp_l30) AS gross_asp_l30
        FROM filtered_data
        GROUP BY asin, marketplace
    ),
    forecast_filtered AS (
        SELECT *
        FROM razor_db.forecast_reporting.system_forecast_reporting
        WHERE asp_benchmark IS NOT NULL
    ),
    latest_data AS (
        SELECT asin, market_reporting, asp_benchmark, date,
               ROW_NUMBER() OVER (
                   PARTITION BY asin, market_reporting
                   ORDER BY date DESC
               ) AS rn
        FROM forecast_filtered
    )
    SELECT 
        l.asin || (CASE WHEN l.market_reporting = 'Pan-EU' THEN 'EU' ELSE l.market_reporting END) AS ref,
        CASE 
            WHEN l.asp_benchmark IS NULL OR l.asp_benchmark = 0 THEN a.gross_asp_l30
            ELSE l.asp_benchmark
        END AS final_sales_price
    FROM (
        SELECT asin, 
               market_reporting, 
               asp_benchmark, 
               asin || (CASE WHEN market_reporting = 'Pan-EU' THEN 'EU' ELSE market_reporting END) AS ref
        FROM latest_data
        WHERE rn = 1
    ) l
    LEFT JOIN aggregated_data a
    ON l.asin = a.asin AND l.market_reporting = a.marketplace
    """
    
    # 3. Demand Data Query
    queries['demand'] = """
    WITH RankedCTE AS (
        SELECT 
            CASE
                WHEN marketplace IN ('DE', 'Pan-EU') THEN 'EU'
                ELSE marketplace
            END AS mp,
            razin,
            asin,
            CAST(date AS DATE) AS date,
            CAST(snapshot_date AS DATE) AS snapshot_date,
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
        GROUP BY 
            mp,
            razin,
            asin,
            date,
            snapshot_date
    )
    SELECT 
        mp,
        razin,
        asin,
        quantity,
        date
    FROM RankedCTE
    ORDER BY 
        razin, 
        mp, 
        quantity
    """
    
    # 4. Master Data Query
    queries['master_data'] = """
    WITH ranked_data AS (
        SELECT 
            DISTINCT 
            name AS razin, 
            asin_number AS asin,
            COALESCE(lead_time_production_days, 45) AS lead_time_production_days,
            parcels_per_master_carton AS units_per_carton, 
            master_carton_weight_kg, 
            master_carton_volume, 
            ea_product_package_weight AS "Shipping weight",
            preferred_vendor,
            product_package_dimensions_h_cm,
            product_package_dimensions_l_cm,
            product_package_dimensions_w_cm,
            
            -- Compute key values
            GREATEST(product_package_dimensions_l_cm, product_package_dimensions_h_cm, product_package_dimensions_w_cm) AS max_dim,
            product_package_dimensions_l_cm + 2 * (product_package_dimensions_h_cm + product_package_dimensions_w_cm) AS length_plus_girth,
            
            dense_rank() OVER (PARTITION BY name ORDER BY snapshot_date DESC) AS rnk
        FROM razor_db.core.razin_master_data
        WHERE COALESCE(successor_razin, '') = ''
    ),
    classified AS (
        SELECT *,
            CASE
                -- Small standard-size
                WHEN 
                    "Shipping weight" <= 0.45 AND
                    max_dim <= 38.1 AND
                    product_package_dimensions_l_cm <= 33.02 AND
                    product_package_dimensions_w_cm <= 27.94 AND
                    product_package_dimensions_h_cm <= 5.08
                THEN 'Standard'

                -- Large standard-size
                WHEN 
                    "Shipping weight" <= 9.07 AND
                    max_dim <= 45.72 AND
                    length_plus_girth <= 330.2
                THEN 'Standard'

                -- Large bulky
                WHEN 
                    "Shipping weight" <= 22.68 AND
                    max_dim <= 149.86 AND
                    length_plus_girth <= 330.2 AND
                    product_package_dimensions_l_cm <= 149.86 AND
                    product_package_dimensions_w_cm <= 83.82 AND
                    product_package_dimensions_h_cm <= 83.82
                THEN 'Oversize'

                -- Extra-large 0 to 50 lb
                WHEN 
                    "Shipping weight" <= 22.68 AND
                    (max_dim > 149.86 OR length_plus_girth > 330.2)
                THEN 'Oversize'

                -- Extra-large 50+ lb
                WHEN "Shipping weight" > 22.68
                THEN 'Oversize'

                -- Catch-all
                ELSE 'Oversize'
            END AS size_tier
        FROM ranked_data
    )
    SELECT 
        razin, 
        asin, 
        lead_time_production_days,
        units_per_carton, 
        preferred_vendor,
        master_carton_weight_kg, 
        master_carton_volume,
        size_tier,
        product_package_dimensions_h_cm,
        product_package_dimensions_l_cm,
        product_package_dimensions_w_cm
    FROM classified
    WHERE rnk = 1
    """
    
    # 5. GFL List Query
    queries['gfl_list'] = """
    SELECT DISTINCT 
        asin,
        country_code,
        brand_name,
        razin,
        marketplace,
        portfolio_cluster
    FROM razor_db.core.amazon_product
    WHERE go_forward = 1
    """
    
    # 6. Product Market/Vendor Info Query
    queries['product_market'] = """
    WITH ranked_vendors AS (
        SELECT 
            vendor_id,
            vendor_name,
            port_of_departure AS pol,
            supplier_manager,
            category_manager,
            country AS shipping_country,
            snapshot_date,
            ROW_NUMBER() OVER (
                PARTITION BY vendor_id 
                ORDER BY snapshot_date DESC
            ) AS rn
        FROM razor_db.core.vendor_master_data
        WHERE supplier_category in ('Manufacturers','Manufacturers / LP')
    )
    SELECT 
        vendor_id,
        vendor_name,
        pol,
        supplier_manager,
        category_manager,
        shipping_country
    FROM ranked_vendors
    WHERE rn = 1
    """
    
    # 7. Open PO Query (Complex)
    queries['open_po'] = """
    SELECT 
        CONCAT(COALESCE(POData.asin, POData.item), mp) AS ref,
        POData.id as id,
        POData.document_number AS "PO#",
        POData.line_id AS line_id,
        POData.item AS RAZIN,
        POData.asin AS ASIN,
        POData.scm_associated_brands AS Brand_Name,
        mp,
        POData.prd_reconfirmation,
        POData.po