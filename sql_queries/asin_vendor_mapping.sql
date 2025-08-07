-- ASIN to Vendor Mapping Query
-- Maps ASINs/RAZINs to their preferred vendors
-- Source: razin_subsidiary_vendor_master_mapping

WITH vendor_ranked AS (
    SELECT 
        name AS razin, 
        vendor,
        preferred_vendor,
        ROW_NUMBER() OVER (PARTITION BY name ORDER BY vendor) AS row_num  -- Pick the first available vendor
    FROM razor_db.core.razin_subsidiary_vendor_master_mapping
),
ranked_orders AS (
    SELECT 
        item AS razin, 
        po_vendor, 
        ROW_NUMBER() OVER (PARTITION BY item ORDER BY ordered_at DESC) AS rn  -- Keep only the latest order per razin
    FROM razor_db.public.rgbit_netsuite_purchase_orders_lineitems_withkey
    WHERE vendor_category <> 'Intercompany'
),
razin_master AS (
    SELECT 
        name AS razin, 
        asin_number AS asin,
        ROW_NUMBER() OVER (PARTITION BY name ORDER BY snapshot_date DESC) AS row_num  -- Pick the most recent ASIN per razin
    FROM razor_db.core.razin_master_data
)
SELECT 
    v.razin, 
    rm.asin, 
    COALESCE(NULLIF(v.preferred_vendor, ''), v.vendor, r.po_vendor) AS final_vendor  
FROM vendor_ranked v
LEFT JOIN ranked_orders r ON v.razin = r.razin AND r.rn = 1  -- Ensure we get only the latest po_vendor
LEFT JOIN razin_master rm ON v.razin = rm.razin AND rm.row_num = 1  -- Ensure we get only the latest ASIN
WHERE v.row_num = 1  -- Pick only one vendor per razin
ORDER BY v.razin