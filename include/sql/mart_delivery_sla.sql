-- include/sql/mart_delivery_sla.sql
-- Business question 3: average delivery time per store, and % of delivered orders within 15 minutes.

CREATE OR REPLACE TABLE mart_delivery_sla AS
SELECT
    o.store_id,
    s.store_name,
    COALESCE(s.zone, 'Unknown')                                         AS zone,
    COUNT(*)                                                            AS delivered_orders,
    ROUND(AVG(o.delivery_minutes), 1)                                   AS avg_delivery_minutes,
    COUNT(*) FILTER (WHERE o.within_15_min)                             AS within_15_min,
    ROUND(100.0 * COUNT(*) FILTER (WHERE o.within_15_min) / COUNT(*), 1) AS pct_within_15_min
FROM stg_orders o
LEFT JOIN stg_stores s ON s.store_id = o.store_id AND s.dq_status = 'PASSED'
WHERE o.dq_status = 'PASSED' AND o.order_status = 'DELIVERED'
GROUP BY ALL
ORDER BY pct_within_15_min DESC;
