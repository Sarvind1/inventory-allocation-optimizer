-- Long Term Demand Forecast Query
-- Fetches demand data from validated sales plan
-- Source: rgbit_po_calendar_bm_saleplan_snapshot

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