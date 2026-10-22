-- include/sql/stg_payments.sql
-- Staging for payments: cast types, standardise payment_mode, apply quality rules.

CREATE OR REPLACE TABLE stg_payments AS
WITH latest AS (
    SELECT * FROM raw_payments QUALIFY _loaded_at = MAX(_loaded_at) OVER ()
),
-- Every order we have received (all orders files), to check that payments point at a real order
all_orders AS (
    SELECT order_id, TRY_CAST(order_amount AS DOUBLE) AS amt, _source_file, _source_row
    FROM raw_orders
    QUALIFY _loaded_at = MAX(_loaded_at) OVER (PARTITION BY _source_file)
),
-- One amount per order: an increment correction wins over orders.csv, otherwise the first row
order_amount AS (
    SELECT order_id, amt FROM all_orders
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _source_file = 'orders.csv', _source_row) = 1
),
typed AS (
    SELECT l.*,
        TRY_CAST(l.amount_paid AS DOUBLE)  AS paid,
        UPPER(TRIM(l.payment_status))      AS status_clean,
        o.amt                              AS order_amt
    FROM latest l LEFT JOIN order_amount o ON o.order_id = l.order_id
),
checked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY _source_row) > 1
                                                                    AS chk__payments_payment_id_unique,
        NOT COALESCE(order_id IN (SELECT order_id FROM all_orders), FALSE)
                                                                    AS chk__payments_order_exists,
        COALESCE(paid <= 0, FALSE)                                  AS chk__payments_amount_positive,
        COALESCE(status_clean = 'SUCCESS' AND ABS(paid - order_amt) > 0.01, FALSE)
                                                                    AS chk__payments_amount_matches_order,
        NOT COALESCE(payment_mode IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet'), FALSE)
                                                                    AS chk__payments_mode_allowed,
        COALESCE(status_clean = 'SUCCESS' AND gateway_ref IS NULL, FALSE)
                                                                    AS chk__payments_gateway_ref_present
    FROM typed
),
flagged AS (
    SELECT *,
        NULLIF(concat_ws('; ',
            CASE WHEN chk__payments_payment_id_unique THEN 'payments_payment_id_unique' END,
            CASE WHEN chk__payments_order_exists      THEN 'payments_order_exists' END,
            CASE WHEN chk__payments_amount_positive   THEN 'payments_amount_positive' END
        ), '') AS failure_reason,
        NULLIF(concat_ws('; ',
            CASE WHEN chk__payments_amount_matches_order THEN 'payments_amount_matches_order' END,
            CASE WHEN chk__payments_mode_allowed         THEN 'payments_mode_allowed' END,
            CASE WHEN chk__payments_gateway_ref_present  THEN 'payments_gateway_ref_present' END
        ), '') AS dq_warnings
    FROM checked
)
SELECT
    TRIM(payment_id)                                                AS payment_id,
    TRIM(order_id)                                                  AS order_id,
    TRY_STRPTIME(payment_ts, '%Y-%m-%d %H:%M:%S')                   AS payment_ts,
    clean_payment_mode(payment_mode)                                AS payment_mode,
    paid                                                            AS amount_paid,
    status_clean                                                    AS payment_status,
    TRIM(gateway_ref)                                               AS gateway_ref,
    CASE WHEN failure_reason IS NULL THEN 'PASSED' ELSE 'FAILED' END AS dq_status,
    failure_reason,
    dq_warnings,
    COLUMNS('^chk__'),
    _source_file, _source_row, _run_id, _loaded_at
FROM flagged;
