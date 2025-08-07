-- Target Sales Price Query
-- Fetches target sales prices for revenue calculations
-- Sources: rgbit_asp_l30_all_channels_w_fallbacks, forecast_reporting.system_forecast_reporting

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