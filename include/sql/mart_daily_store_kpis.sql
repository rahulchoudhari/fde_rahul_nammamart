-- include/sql/mart_daily_store_kpis.sql
-- Business question 1: daily net revenue by zone and by store (delivered orders, after discount).
-- Only rows that passed every Critical check (dq_status = 'PASSED') are counted.

CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    o.order_date,
    COALESCE(s.zone, 'Unknown')                                         AS zone,
    o.store_id,
    s.store_name,
    COUNT(*)                                                            AS total_orders,
    COUNT(*) FILTER (WHERE o.order_status = 'DELIVERED')                AS delivered_orders,
    COUNT(*) FILTER (WHERE o.order_status = 'CANCELLED')                AS cancelled_orders,
    COUNT(*) FILTER (WHERE o.order_status = 'RETURNED')                 AS returned_orders,
    ROUND(SUM(o.net_revenue), 2)                                        AS net_revenue,
    ROUND(AVG(o.delivery_minutes) FILTER (WHERE o.order_status = 'DELIVERED'), 1)
                                                                        AS avg_delivery_minutes
FROM stg_orders o
LEFT JOIN stg_stores s ON s.store_id = o.store_id AND s.dq_status = 'PASSED'
WHERE o.dq_status = 'PASSED'
GROUP BY ALL
ORDER BY o.order_date, zone, o.store_id;
