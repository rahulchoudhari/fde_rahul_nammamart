-- include/sql/stg_customers.sql
-- Staging for customers: dedupe, standardise loyalty_tier, MASK PII, apply quality rules.
-- full_name is dropped; email and phone are masked. Raw values stay only in raw_customers.
-- Uses session variable: run_now.

CREATE OR REPLACE TABLE stg_customers AS
WITH latest AS (
    SELECT * FROM raw_customers QUALIFY _loaded_at = MAX(_loaded_at) OVER ()
),
checked AS (
    SELECT *,
        -- Keep the newest copy of each customer (latest signup_date); older copies fail
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY TRY_STRPTIME(signup_date, '%Y-%m-%d') DESC NULLS LAST, _source_row DESC
        ) > 1                                                       AS chk__customers_id_unique,
        NOT regexp_matches(COALESCE(email, ''), '^[^@\s]+@[^@\s]+\.[a-z]{2,}$')
                                                                    AS chk__customers_email_format,
        NOT regexp_full_match(COALESCE(phone, ''), '\d{10}')        AS chk__customers_phone_10_digits,
        NOT regexp_full_match(COALESCE(pincode, ''), '\d{6}')       AS chk__customers_pincode_6_digits,
        city IS NULL                                                AS chk__customers_city_not_null,
        COALESCE(TRY_STRPTIME(signup_date, '%Y-%m-%d') > getvariable('run_now'), FALSE)
                                                                    AS chk__customers_signup_not_future,
        NOT COALESCE(loyalty_tier IN ('Bronze', 'Silver', 'Gold', 'Platinum'), FALSE)
                                                                    AS chk__customers_tier_casing
    FROM latest
),
flagged AS (
    SELECT *,
        NULLIF(concat_ws('; ',
            CASE WHEN chk__customers_id_unique         THEN 'customers_id_unique' END,
            CASE WHEN chk__customers_signup_not_future THEN 'customers_signup_not_future' END
        ), '') AS failure_reason,
        NULLIF(concat_ws('; ',
            CASE WHEN chk__customers_email_format      THEN 'customers_email_format' END,
            CASE WHEN chk__customers_phone_10_digits   THEN 'customers_phone_10_digits' END,
            CASE WHEN chk__customers_pincode_6_digits  THEN 'customers_pincode_6_digits' END,
            CASE WHEN chk__customers_city_not_null     THEN 'customers_city_not_null' END,
            CASE WHEN chk__customers_tier_casing       THEN 'customers_tier_casing' END
        ), '') AS dq_warnings
    FROM checked
)
SELECT
    TRIM(customer_id)                                               AS customer_id,
    mask_email(TRIM(email))                                         AS email,
    mask_phone(TRIM(phone))                                         AS phone,
    TRIM(city)                                                      AS city,
    TRIM(area)                                                      AS area,
    TRIM(pincode)                                                   AS pincode,
    CAST(TRY_STRPTIME(signup_date, '%Y-%m-%d') AS DATE)             AS signup_date,
    upper(left(TRIM(loyalty_tier), 1)) || lower(substr(TRIM(loyalty_tier), 2))
                                                                    AS loyalty_tier,
    upper(TRIM(is_active)) = 'Y'                                    AS is_active,
    CASE WHEN failure_reason IS NULL THEN 'PASSED' ELSE 'FAILED' END AS dq_status,
    failure_reason,
    dq_warnings,
    COLUMNS('^chk__'),
    _source_file, _source_row, _run_id, _loaded_at
FROM flagged;
