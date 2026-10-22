-- include/sql/mart_top_customers.sql
-- Business question 5: top 10 customers by spend, with loyalty tier.
-- Only masked contact details are available here; raw email/phone never leave raw_customers.

CREATE OR REPLACE TABLE mart_top_customers AS
SELECT
    RANK() OVER (ORDER BY SUM(o.net_revenue) DESC)  AS spend_rank,
    o.customer_id,
    c.loyalty_tier,
    c.city,
    c.email                                         AS email_masked,
    COUNT(*)                                        AS delivered_orders,
    ROUND(SUM(o.net_revenue), 2)                    AS total_spend
FROM stg_orders o
LEFT JOIN stg_customers c ON c.customer_id = o.customer_id AND c.dq_status = 'PASSED'
WHERE o.dq_status = 'PASSED' AND o.order_status = 'DELIVERED'
GROUP BY o.customer_id, c.loyalty_tier, c.city, c.email
ORDER BY total_spend DESC
LIMIT 10;
