-- include/sql/stg_orders.sql
-- Staging for orders: cast types, standardise, apply quality rules, flag every row.
--
-- v_orders_checked holds the latest load of EVERY orders file (orders.csv and each increment).
-- stg_orders is rebuilt from orders.csv only; stg_orders_merge.sql then upserts the increments.
-- Checks read the raw text (exactly as received); output columns are the cleaned values.
-- Uses session variable: run_now (IST timestamp of this run).

CREATE OR REPLACE VIEW v_orders_checked AS
WITH latest AS (
    SELECT * FROM raw_orders
    QUALIFY _loaded_at = MAX(_loaded_at) OVER (PARTITION BY _source_file)
),
latest_products AS (
    SELECT product_id FROM raw_products QUALIFY _loaded_at = MAX(_loaded_at) OVER ()
),
latest_stores AS (
    SELECT store_id FROM raw_stores QUALIFY _loaded_at = MAX(_loaded_at) OVER ()
),
typed AS (
    SELECT *,
        TRY_STRPTIME(order_ts, '%Y-%m-%d %H:%M:%S')  AS ts,
        TRY_CAST(quantity AS DOUBLE)                 AS qty,
        TRY_CAST(unit_price AS DOUBLE)               AS price,
        TRY_CAST(discount_pct AS DOUBLE)             AS disc,
        TRY_CAST(order_amount AS DOUBLE)             AS amt,
        TRY_CAST(delivery_minutes AS DOUBLE)         AS mins,
        UPPER(TRIM(order_status))                    AS status_clean
    FROM latest
),
checked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY _source_file, order_id ORDER BY _source_row) > 1
                                                                            AS chk__orders_order_id_unique,
        customer_id IS NULL                                                 AS chk__orders_customer_id_not_null,
        order_ts IS NULL                                                    AS chk__orders_order_ts_not_null,
        order_ts IS NOT NULL AND ts IS NULL                                 AS chk__orders_order_ts_valid,
        COALESCE(ts > getvariable('run_now'), FALSE)                        AS chk__orders_order_ts_not_future,
        NOT COALESCE(status_clean IN ('DELIVERED', 'CANCELLED', 'RETURNED'), FALSE)
                                                                            AS chk__orders_status_allowed,
        NOT COALESCE(payment_mode IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet'), FALSE)
                                                                            AS chk__orders_payment_mode_allowed,
        COALESCE(qty <= 0, FALSE)                                           AS chk__orders_quantity_positive,
        NOT COALESCE(disc BETWEEN 0 AND 100, FALSE)                         AS chk__orders_discount_range,
        COALESCE(ABS(amt - ROUND(qty * price * (1 - disc / 100), 2)) > 0.01, FALSE)
                                                                            AS chk__orders_amount_formula,
        COALESCE(status_clean = 'DELIVERED' AND delivery_minutes IS NULL, FALSE)
                                                                            AS chk__orders_delivered_has_minutes,
        COALESCE(mins >= 120, FALSE)                                        AS chk__orders_delivery_minutes_max,
        NOT COALESCE(product_id IN (SELECT product_id FROM latest_products), FALSE)
                                                                            AS chk__orders_product_exists,
        NOT COALESCE(store_id IN (SELECT store_id FROM latest_stores), FALSE)
                                                                            AS chk__orders_store_exists
    FROM typed
),
flagged AS (
    SELECT *,
        -- Critical checks only: these decide whether the row reaches the marts
        NULLIF(concat_ws('; ',
            CASE WHEN chk__orders_order_id_unique       THEN 'orders_order_id_unique' END,
            CASE WHEN chk__orders_customer_id_not_null  THEN 'orders_customer_id_not_null' END,
            CASE WHEN chk__orders_order_ts_not_null     THEN 'orders_order_ts_not_null' END,
            CASE WHEN chk__orders_order_ts_valid        THEN 'orders_order_ts_valid' END,
            CASE WHEN chk__orders_order_ts_not_future   THEN 'orders_order_ts_not_future' END,
            CASE WHEN chk__orders_status_allowed        THEN 'orders_status_allowed' END,
            CASE WHEN chk__orders_quantity_positive     THEN 'orders_quantity_positive' END,
            CASE WHEN chk__orders_discount_range        THEN 'orders_discount_range' END,
            CASE WHEN chk__orders_amount_formula        THEN 'orders_amount_formula' END,
            CASE WHEN chk__orders_delivered_has_minutes THEN 'orders_delivered_has_minutes' END,
            CASE WHEN chk__orders_delivery_minutes_max  THEN 'orders_delivery_minutes_max' END,
            CASE WHEN chk__orders_product_exists        THEN 'orders_product_exists' END,
            CASE WHEN chk__orders_store_exists          THEN 'orders_store_exists' END
        ), '') AS failure_reason,
        NULLIF(concat_ws('; ',
            CASE WHEN chk__orders_payment_mode_allowed  THEN 'orders_payment_mode_allowed' END
        ), '') AS dq_warnings
    FROM checked
)
SELECT
    TRIM(order_id)                                                  AS order_id,
    ts                                                              AS order_ts,
    CAST(ts AS DATE)                                                AS order_date,
    TRIM(customer_id)                                               AS customer_id,
    TRIM(store_id)                                                  AS store_id,
    TRIM(product_id)                                                AS product_id,
    TRY_CAST(qty AS INTEGER)                                        AS quantity,
    price                                                           AS unit_price,
    disc                                                            AS discount_pct,
    amt                                                             AS order_amount,
    clean_payment_mode(payment_mode)                                AS payment_mode,
    status_clean                                                    AS order_status,
    TRIM(channel)                                                   AS channel,
    mins                                                            AS delivery_minutes,
    CASE WHEN status_clean = 'DELIVERED' THEN amt ELSE 0 END        AS net_revenue,
    CASE WHEN status_clean = 'DELIVERED' THEN mins <= 15 END        AS within_15_min,
    CASE WHEN failure_reason IS NULL THEN 'PASSED' ELSE 'FAILED' END AS dq_status,
    failure_reason,
    dq_warnings,
    COLUMNS('^chk__'),
    _source_file, _source_row, _run_id, _loaded_at
FROM flagged;

CREATE OR REPLACE TABLE stg_orders AS
SELECT * FROM v_orders_checked WHERE _source_file = 'orders.csv';
