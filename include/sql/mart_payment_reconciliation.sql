-- include/sql/mart_payment_reconciliation.sql
-- Business question 4 (part 2): one row per order/payment mismatch, with a reason.
-- Same four rules as the ETL table etl_payment_reconciliation.

CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH orders AS (SELECT * FROM stg_orders WHERE dq_status = 'PASSED'),
payments AS (SELECT * FROM stg_payments WHERE dq_status = 'PASSED'),
paid AS (
    SELECT order_id,
           COUNT(*) FILTER (WHERE payment_status = 'SUCCESS') AS success_count
    FROM payments GROUP BY order_id
)
-- Delivered orders with no successful payment
SELECT o.order_id, NULL AS payment_id, o.order_status, o.payment_mode,
       o.order_amount, 0.0 AS amount_paid, o.order_amount AS difference,
       'DELIVERED_NOT_PAID' AS reason
FROM orders o LEFT JOIN paid p USING (order_id)
WHERE o.order_status = 'DELIVERED' AND COALESCE(p.success_count, 0) = 0

UNION ALL
-- Successful payment that doesn't equal the order amount
SELECT o.order_id, pay.payment_id, o.order_status, pay.payment_mode,
       o.order_amount, pay.amount_paid, ROUND(o.order_amount - pay.amount_paid, 2),
       'AMOUNT_MISMATCH'
FROM payments pay JOIN orders o USING (order_id)
WHERE pay.payment_status = 'SUCCESS' AND ABS(o.order_amount - pay.amount_paid) > 0.01

UNION ALL
-- Cancelled order that was still charged
SELECT o.order_id, pay.payment_id, o.order_status, pay.payment_mode,
       o.order_amount, pay.amount_paid, ROUND(o.order_amount - pay.amount_paid, 2),
       'CANCELLED_BUT_PAID'
FROM payments pay JOIN orders o USING (order_id)
WHERE pay.payment_status = 'SUCCESS' AND o.order_status = 'CANCELLED'

UNION ALL
-- Payment for an order that isn't in the reporting tables (failed its checks, or unknown)
SELECT pay.order_id, pay.payment_id, NULL, pay.payment_mode,
       NULL, pay.amount_paid, NULL, 'ORDER_NOT_IN_FACT'
FROM payments pay
WHERE pay.order_id NOT IN (SELECT order_id FROM orders);
