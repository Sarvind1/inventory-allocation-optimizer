-- Master Data Query
-- Fetches product master data including lead times, carton info, and size tier classification
-- Source: razin_master_data

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
        
        -- Compute key values for size tier classification
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

            -- Extra-large 50 to 70 lb
            WHEN "Shipping weight" > 22.68 AND "Shipping weight" <= 31.75
            THEN 'Oversize'

            -- Extra-large 70 to 150 lb
            WHEN "Shipping weight" > 31.75 AND "Shipping weight" <= 68.04
            THEN 'Oversize'

            -- Extra-large more than 150 lb
            WHEN "Shipping weight" > 68.04
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