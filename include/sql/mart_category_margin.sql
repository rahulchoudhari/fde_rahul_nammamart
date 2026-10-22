-- include/sql/mart_category_margin.sql
-- Business question 2: revenue and gross margin by product category.
-- gross_margin = revenue − quantity × cost_price. Orders whose product failed its checks
-- show as 'Unknown' with no margin (cost price can't be trusted).

CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    COALESCE(p.category, 'Unknown')                                     AS category,
    COUNT(*)                                                            AS delivered_orders,
    ROUND(SUM(o.net_revenue), 2)                                        AS net_revenue,
    ROUND(SUM(o.net_revenue - o.quantity * p.cost_price), 2)            AS gross_margin,
    ROUND(100 * SUM(o.net_revenue - o.quantity * p.cost_price) / SUM(o.net_revenue), 1)
                                                                        AS margin_pct
FROM stg_orders o
LEFT JOIN stg_products p ON p.product_id = o.product_id AND p.dq_status = 'PASSED'
WHERE o.dq_status = 'PASSED' AND o.order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY net_revenue DESC;
