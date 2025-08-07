-- Vendor Master Data Query
-- Fetches vendor information including shipping regions and ports
-- Source: vendor_master_data

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
    WHERE supplier_category IN ('Manufacturers','Manufacturers / LP')
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