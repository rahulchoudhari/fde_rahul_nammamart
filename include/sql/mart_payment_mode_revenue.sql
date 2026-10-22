-- include/sql/mart_payment_mode_revenue.sql
-- Business question 4 (part 1): revenue by payment mode, plus the amount that doesn't reconcile.

CREATE OR REPLACE TABLE mart_payment_mode_revenue AS
WITH revenue AS (
    SELECT payment_mode,
           COUNT(*)                     AS delivered_orders,
           ROUND(SUM(net_revenue), 2)   AS net_revenue
    FROM stg_orders
    WHERE dq_status = 'PASSED' AND order_status = 'DELIVERED'
    GROUP BY payment_mode
),
unreconciled AS (
    SELECT payment_mode,
           COUNT(*)                                 AS mismatches,
           ROUND(SUM(ABS(COALESCE(difference, amount_paid))), 2) AS unreconciled_amount
    FROM mart_payment_reconciliation
    GROUP BY payment_mode
)
SELECT r.payment_mode, r.delivered_orders, r.net_revenue,
       COALESCE(u.mismatches, 0)            AS mismatches,
       COALESCE(u.unreconciled_amount, 0)   AS unreconciled_amount
FROM revenue r LEFT JOIN unreconciled u USING (payment_mode)
ORDER BY r.net_revenue DESC;
