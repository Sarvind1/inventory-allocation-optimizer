-- GFL (Go Forward List) Items Query
-- Fetches list of active products in Amazon catalog
-- Source: amazon_product

SELECT DISTINCT 
    asin,
    country_code,
    brand_name,
    razin,
    marketplace,
    portfolio_cluster
FROM razor_db.core.amazon_product
WHERE go_forward = 1