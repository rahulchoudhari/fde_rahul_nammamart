-- include/sql/elt_setup.sql
-- Shared tables, the DQ check catalog and helper macros for the ELT pipeline.
-- Safe to run on every run.

CREATE TABLE IF NOT EXISTS dq_results (
    run_id        VARCHAR,
    dag_id        VARCHAR,
    check_name    VARCHAR,
    dimension     VARCHAR,
    severity      VARCHAR,
    rows_checked  BIGINT,
    rows_failed   BIGINT,
    status        VARCHAR,
    checked_at    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id            VARCHAR,
    dag_id            VARCHAR,
    start_time        TIMESTAMPTZ,
    end_time          TIMESTAMPTZ,
    rows_extracted    BIGINT,
    rows_loaded       BIGINT,
    rows_quarantined  BIGINT,
    status            VARCHAR
);

-- Same 34 checks as include/nammamart_dq.py (SUBMISSION.md Part D)
CREATE OR REPLACE TABLE elt_dq_catalog (check_name VARCHAR, file VARCHAR, dimension VARCHAR, severity VARCHAR);
INSERT INTO elt_dq_catalog VALUES
    ('orders_order_id_unique',         'orders',    'Uniqueness',   'Critical'),
    ('orders_customer_id_not_null',    'orders',    'Completeness', 'Critical'),
    ('orders_order_ts_not_null',       'orders',    'Completeness', 'Critical'),
    ('orders_order_ts_valid',          'orders',    'Validity',     'Critical'),
    ('orders_order_ts_not_future',     'orders',    'Timeliness',   'Critical'),
    ('orders_status_allowed',          'orders',    'Validity',     'Critical'),
    ('orders_payment_mode_allowed',    'orders',    'Validity',     'Warning'),
    ('orders_quantity_positive',       'orders',    'Accuracy',     'Critical'),
    ('orders_discount_range',          'orders',    'Validity',     'Critical'),
    ('orders_amount_formula',          'orders',    'Accuracy',     'Critical'),
    ('orders_delivered_has_minutes',   'orders',    'Completeness', 'Critical'),
    ('orders_delivery_minutes_max',    'orders',    'Accuracy',     'Critical'),
    ('orders_product_exists',          'orders',    'Consistency',  'Critical'),
    ('orders_store_exists',            'orders',    'Consistency',  'Critical'),
    ('orders_freshness',               'orders',    'Timeliness',   'Warning'),
    ('payments_payment_id_unique',     'payments',  'Uniqueness',   'Critical'),
    ('payments_order_exists',          'payments',  'Consistency',  'Critical'),
    ('payments_amount_positive',       'payments',  'Accuracy',     'Critical'),
    ('payments_amount_matches_order',  'payments',  'Consistency',  'Warning'),
    ('payments_mode_allowed',          'payments',  'Validity',     'Warning'),
    ('payments_gateway_ref_present',   'payments',  'Completeness', 'Warning'),
    ('customers_id_unique',            'customers', 'Uniqueness',   'Critical'),
    ('customers_email_format',         'customers', 'Validity',     'Warning'),
    ('customers_phone_10_digits',      'customers', 'Validity',     'Warning'),
    ('customers_pincode_6_digits',     'customers', 'Validity',     'Warning'),
    ('customers_city_not_null',        'customers', 'Completeness', 'Warning'),
    ('customers_signup_not_future',    'customers', 'Timeliness',   'Critical'),
    ('customers_tier_casing',          'customers', 'Consistency',  'Warning'),
    ('products_id_unique',             'products',  'Uniqueness',   'Critical'),
    ('products_category_not_null',     'products',  'Completeness', 'Critical'),
    ('products_mrp_positive',          'products',  'Accuracy',     'Critical'),
    ('products_cost_le_mrp',           'products',  'Accuracy',     'Critical'),
    ('stores_zone_not_null',           'stores',    'Completeness', 'Critical'),
    ('stores_capacity_positive',       'stores',    'Accuracy',     'Warning');

-- upi / 'UPI ' / CARD / cod / Cashh → the 4 allowed payment modes
CREATE OR REPLACE MACRO clean_payment_mode(m) AS
    CASE lower(trim(m))
        WHEN 'upi'              THEN 'UPI'
        WHEN 'card'             THEN 'Card'
        WHEN 'wallet'           THEN 'Wallet'
        WHEN 'cash on delivery' THEN 'Cash on Delivery'
        WHEN 'cod'              THEN 'Cash on Delivery'
        WHEN 'cashh'            THEN 'Cash on Delivery'
        ELSE trim(m)
    END;

-- priya.rao@example.com → pr***@example.com; malformed emails are fully hidden
CREATE OR REPLACE MACRO mask_email(e) AS
    CASE
        WHEN e IS NULL THEN NULL
        WHEN length(e) - length(replace(e, '@', '')) <> 1 THEN '***'
        ELSE left(split_part(e, '@', 1), 2) || '***@' || split_part(e, '@', 2)
    END;

-- 9876543221 → 98******21
CREATE OR REPLACE MACRO mask_phone(p) AS
    CASE
        WHEN p IS NULL THEN NULL
        WHEN length(regexp_replace(p, '\D', '', 'g')) < 4 THEN repeat('*', length(regexp_replace(p, '\D', '', 'g')))
        ELSE left(regexp_replace(p, '\D', '', 'g'), 2)
             || repeat('*', length(regexp_replace(p, '\D', '', 'g')) - 4)
             || right(regexp_replace(p, '\D', '', 'g'), 2)
    END;
