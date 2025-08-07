-- Inventory SOP (State of Play) Query
-- Fetches current inventory levels across different warehouse locations
-- Source: razin_ibm_country_level_historic

WITH latest_snapshot AS (
    SELECT MAX(snapshot_date) AS max_snapshot_date
    FROM razor_db.inventory.razin_ibm_country_level_historic
),
ranked_data AS (
    SELECT
        snapshot_date,
        COALESCE(asin,razin) as asin,
        razin,
        marketplace AS mp,
        brand_name,
        in_fm,
        in_to_w2w,
        in_nn3pl,
        in_n3pl,
        in_lm,
        in_amz,
        in_wq,
        in_rsvd_cust_ord,
        in_rsd_fc_proc,
        in_walmart,
        in_bol,
        in_meli,
        in_to_osc_l3m,
        units_in_d2amz,
        COALESCE(total_inventory, 0) AS total_inventory,
        units_open_pos_raw,
        RANK() OVER (
            PARTITION BY asin, razin, marketplace 
            ORDER BY snapshot_date DESC
        ) AS rnk
    FROM razor_db.inventory.razin_ibm_country_level_historic
    WHERE snapshot_date = (SELECT max_snapshot_date FROM latest_snapshot)
)
SELECT 
    asin,
    CASE 
        WHEN mp IN ('Pan-EU') THEN 'EU'
        ELSE mp 
    END as mp,
    brand_name,
    MAX(in_fm) AS in_fm,
    MAX(in_to_w2w) AS in_to_w2w,
    MAX(in_nn3pl) AS in_nn3pl,
    MAX(in_n3pl) AS in_n3pl,
    MAX(in_lm) AS in_lm,
    MAX(in_amz) AS in_amz,
    MAX(units_in_d2amz) as units_in_d2amz,
    MAX(in_wq) AS in_wq,
    MAX(in_rsd_fc_proc) as in_rsd_fc_proc,
    MAX(in_rsvd_cust_ord) as in_rsvd_cust_ord,
    Max(in_walmart) as in_walmart,
    Max(in_bol) as in_bol,
    MAX(in_meli) as in_meli,
    MAX(in_to_osc_l3m) as in_to_osc_l3m,
    SUM(total_inventory)-Max(in_walmart)- MAX(in_to_osc_l3m) as total_e2e_inventory,
    SUM(total_inventory)-Max(in_walmart)- MAX(in_to_osc_l3m)-MAX(in_fm)-MAX(units_in_d2amz) as total_inventory,
    MAX(units_open_pos_raw) AS units_open_pos_raw,
    LISTAGG(razin, ', ') WITHIN GROUP (ORDER BY razin) AS razin_list
FROM ranked_data
WHERE rnk = 1
    AND (total_inventory + units_open_pos_raw) > 0
    AND razin IS NOT NULL
GROUP BY asin, mp, brand_name