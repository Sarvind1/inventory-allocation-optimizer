-- Open Purchase Orders Query
-- Fetches open POs that haven't been shipped or inbounded
-- Sources: rgbit_netsuite_purchase_orders_lineitems_withkey, rgbit_netsuite_inbound_shipments

SELECT 
    CONCAT(COALESCE(POData.asin, POData.item), mp) AS ref,
    POData.id as id,
    POData.document_number AS PO#,
    POData.line_id AS line_id,
    POData.item AS RAZIN,
    POData.asin AS ASIN,
    POData.scm_associated_brands AS Brand_Name,
    mp,
    POData.prd_reconfirmation,
    POData.po_vendor AS Vendor_Name,
    COALESCE(POData.scm_po_scm_memo, '') AS MEMO,
    POData.final_status AS Final_Status,
    TRY_CAST(POData.ordered_at AS DATE) AS Order_Date,
    TO_DATE(POData.first_prd, 'DD.MM.YYYY') AS first_prd,
    TRY_CAST(POData.prd AS DATE) AS prd,
    TRY_CAST(POData.planned_prd AS DATE) AS planned_prd,
    TO_DATE(POData.accepted_prd, 'DD.MM.YYYY') AS accepted_prd,
    POData.prd_status AS prd_status,
    TO_DATE(POData.expected_po_arrival_date, 'DD.MM.YYYY') AS expected_po_arrival_date,
    TO_DATE(POData.planned_pickup_date, 'DD.MM.YYYY') AS planned_pickup_date,
    POData.prd_change_reason AS prd_change_reason,
    POData.prd_reconfirmation AS prd_reconfirmation,
    TRY_CAST(POData.confirmed_crd AS DATE) AS crd,
    POData.item_rate AS item_rate,
    POData.item_rate_eur AS item_rate_eur,
    POData.transport_mode AS transport_mode,
    POData.deliver_to_location AS deliver_to_location,
    POData.subsidiary_no_hierarchy AS subsidiary_no_hierarchy,
    POData.production_status AS production_status,
    TO_DATE(POData.quality_control_date, 'DD.MM.YYYY') AS quality_control_date,
    POData.quality_control_status AS quality_control_status,
    POData.supplier_payment_terms AS supplier_payment_terms,
    POData.incoterms AS incoterms,
    POData.batch_id AS batch_id,
    POData.wh_type AS wh_type,
    POData.im_line_signoff AS im_sign_off,
    POData.sm_line_signoff AS sm_sign_off,
    POData."considered_for_anti-po" AS considered_for_anti_po,
    POData.quantity AS Quantity,
    COALESCE(POData."quantity_fulfilled/received"::bigint, 0) AS Quantity_Fulfilled_Received,
    COALESCE(POData.quantity_on_shipments::bigint, 0) AS Quantity_On_Shipment,
    POData.quantity - GREATEST(
        COALESCE(POData."quantity_fulfilled/received"::bigint, 0), 
        COALESCE(POData.quantity_on_shipments::bigint, 0), 
        COALESCE(INBData.quantity_expected::bigint, 0)
    ) AS Leftover_Quantity

FROM (
    SELECT 
        *, 
        DENSE_RANK() OVER(PARTITION BY document_number, line_id ORDER BY snapshot_date DESC) AS PORank,
        CASE
            WHEN marketplace_header IN ('Pan-EU') THEN 'EU'
            ELSE marketplace_header
        END AS mp
    FROM razor_db.public.rgbit_netsuite_purchase_orders_lineitems_withkey
) AS POData

LEFT JOIN (
    SELECT 
        INBL.po_line_unique_key, 
        INBL.po, 
        INBL.item, 
        INBL.quantity_expected
    FROM razor_db.public.rgbit_netsuite_inbound_shipments_header AS INBH
    INNER JOIN razor_db.public.rgbit_netsuite_inbound_shipments_lineitems_withkey AS INBL
        ON INBH.shipment_number = INBL.shipment_number
) AS INBData
    ON POData.document_number = INBData.po
    AND POData.item = INBData.item
    AND POData.po_line_unique_key = INBData.po_line_unique_key

WHERE POData.final_status NOT IN ('Closed', 'Legacy Closed', 'Fully Billed', 'Rejected by Supervisor')
  AND POData.quantity > 0
  AND POData.quantity - GREATEST(
        COALESCE(POData."quantity_fulfilled/received"::bigint, 0), 
        COALESCE(POData.quantity_on_shipments::bigint, 0), 
        COALESCE(INBData.quantity_expected::bigint, 0)
      ) > 0
  AND vendor_category <> 'Intercompany'
  AND PORank = 1
  AND (production_status <> 'Shipped' OR production_status IS NULL)
ORDER BY Brand_Name, Order_Date