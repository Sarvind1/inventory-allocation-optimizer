-- Inbound Shipments Query
-- Fetches inbound shipment data with expected delivery dates
-- Sources: rgbit_netsuite_inbound_shipments_lineitems_withkey, rgbit_netsuite_inbound_shipments_header

SELECT 
    CONCAT(RAZIN, Mkt_Place) AS ref, 
    *
FROM (
    SELECT
        COALESCE(INBData.po, POData.document_number) AS po_number,
        POData.line_id AS po_line_id,
        INBData.shipment_number AS inb_number,
        POData.batch_id AS batch_id,
        TO_DATE(inbdata.date_created,'DD.MM.YYYY') as date_created,
        POData.po_line_unique_key AS po_key,
        POData.scm_associated_brands AS brand_name,
        CASE
            WHEN COALESCE(INBData.market_place, POData.MarketPlace) IN ('Pan-EU', 'DE') THEN 'EU'
            WHEN COALESCE(INBData.market_place, POData.MarketPlace) IN ('North America') THEN 'US'
            WHEN COALESCE(INBData.market_place, POData.MarketPlace) IN ('GB') THEN 'UK'
            ELSE COALESCE(INBData.market_place, POData.MarketPlace)
        END AS mkt_place,
        POData.po_vendor AS vendor_name,
        COALESCE(POData.scm_po_scm_memo, '') AS memo,
        POData.transport_mode AS shipment_method,
        CASE 
            WHEN receiving_warehouse_type LIKE '%Amazon%' THEN 'AMZ' 
            ELSE '3PL' 
        END AS receiving_warehouse_type,
        INBData.receiving_location AS receiving_location,
        INBData.status AS status,
        INBData.substatus AS sub_status,
        DATE(INBData.cargo_ready_date) AS final_crd,
        DATE(INBData.actual_cargo_pick_up_date) AS movement_date,
        DATE(INBData.expected_delivery_date) AS expected_delivery_date,
        DATE(INBData.actual_arrival_date) AS actual_arrival_date,
        COALESCE(INBData.item, POData.item) AS razin,
        POData.asin AS asin,
        COALESCE(INBData.quantity_expected, 0) AS quantity_expected,
        COALESCE(INBData.Quantity_Received, 0) AS quantity_received,
        GREATEST(COALESCE(INBData.quantity_remaining_to_be_received - INBData.Quantity_Received::bigint, POData.Leftover_PO_Quantity::bigint), 0) AS quantity
    FROM (
        SELECT DISTINCT 
            document_number, 
            line_id,
            batch_id,
            po_line_unique_key, 
            scm_associated_brands,
            COALESCE(market_place, SUBSTRING(SPLIT_PART(deliver_to_location, '_', 2), 1, 2)) AS marketplace,
            po_vendor, 
            scm_po_scm_memo, 
            final_status, 
            ordered_at,  
            confirmed_crd,
            item, 
            asin,
            deliver_to_location, 
            transport_mode, 
            transport_type,
            quantity, 
            "quantity_fulfilled/received", 
            quantity_on_shipments, 
            quantity - COALESCE(quantity_on_shipments::bigint, 0) AS leftover_po_quantity
        FROM (
            SELECT *, DENSE_RANK() OVER (PARTITION BY document_number ORDER BY snapshot_date DESC) AS porank 
            FROM razor_db.public.rgbit_netsuite_purchase_orders_lineitems_withkey
        )
        WHERE  
            final_status NOT IN ('Closed', 'Legacy Closed', 'Fully Billed', 'Rejected by Supervisor')
            AND vendor_category <> 'Intercompany'
            AND COALESCE(scm_po_scm_memo, 'A') NOT LIKE '%B2B%'
            AND COALESCE(scm_po_scm_memo, 'A') NOT LIKE '%SHOPIFY%'
            AND porank = 1
    ) AS podata
    LEFT JOIN (
        SELECT 
            inbl.po_line_unique_key, 
            inbl.po, 
            inbh.shipment_number, 
            inbh.external_document_number, 
            inbl.vendor,
            inbh.market_place, 
            inbl.receiving_location, 
            inbh.receiving_warehouse_type,
            inbh.fba_id, 
            inbh.status, 
            inbh.substatus,
            inbh.cargo_ready_date, 
            inbh.expected_shipping_date, 
            inbh.expected_arrival_date, 
            inbh.expected_delivery_date,
            inbh.actual_cargo_pick_up_date, 
            inbh.actual_shipping_date, 
            inbh.actual_arrival_date, 
            inbh.actual_delivery_date,
            inbl.item, 
            inbl.quantity_expected, 
            inbl.quantity_remaining_to_be_received, 
            inbh.date_created,
            CASE 
                WHEN inbh.substatus IS NULL THEN 0 
                ELSE 
                    CASE 
                        WHEN inbl.quantity_received >= 0 THEN inbl.quantity_received 
                        ELSE inbl.quantity_remaining_to_be_received 
                    END 
            END AS quantity_received
        FROM (
            SELECT *, DENSE_RANK() OVER (PARTITION BY shipment_number ORDER BY snapshot_date DESC) AS po_line_rank 
            FROM razor_db.public.rgbit_netsuite_inbound_shipments_header
        ) AS inbh
        INNER JOIN (
            SELECT * 
            FROM razor_db.public.rgbit_netsuite_inbound_shipments_lineitems_withkey
        ) AS inbl
        ON inbh.shipment_number = inbl.shipment_number
        AND po_line_rank = 1
    ) AS inbdata
    ON podata.document_number = inbdata.po 
    AND podata.item = inbdata.item 
    AND podata.po_line_unique_key = inbdata.po_line_unique_key
    WHERE quantity_expected > 0
    AND quantity > 0 
    ORDER BY brand_name, expected_delivery_date
)