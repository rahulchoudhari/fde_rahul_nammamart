-- include/sql/stg_products.sql
-- Staging for products: cast types, dedupe, apply quality rules.

CREATE OR REPLACE TABLE stg_products AS
WITH latest AS (
    SELECT * FROM raw_products QUALIFY _loaded_at = MAX(_loaded_at) OVER ()
),
typed AS (
    SELECT *,
        TRY_CAST(mrp AS DOUBLE)         AS mrp_num,
        TRY_CAST(cost_price AS DOUBLE)  AS cost_num
    FROM latest
),
checked AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _source_row) > 1
                                                    AS chk__products_id_unique,
        category IS NULL                            AS chk__products_category_not_null,
        COALESCE(mrp_num <= 0, FALSE)               AS chk__products_mrp_positive,
        COALESCE(cost_num > mrp_num, FALSE)         AS chk__products_cost_le_mrp
    FROM typed
),
flagged AS (
    SELECT *,
        NULLIF(concat_ws('; ',
            CASE WHEN chk__products_id_unique         THEN 'products_id_unique' END,
            CASE WHEN chk__products_category_not_null THEN 'products_category_not_null' END,
            CASE WHEN chk__products_mrp_positive      THEN 'products_mrp_positive' END,
            CASE WHEN chk__products_cost_le_mrp       THEN 'products_cost_le_mrp' END
        ), '') AS failure_reason
    FROM checked
)
SELECT
    TRIM(product_id)                                                AS product_id,
    TRIM(product_name)                                              AS product_name,
    TRIM(category)                                                  AS category,
    TRIM(sub_category)                                              AS sub_category,
    TRIM(brand)                                                     AS brand,
    mrp_num                                                         AS mrp,
    cost_num                                                        AS cost_price,
    TRIM(unit)                                                      AS unit,
    lower(TRIM(is_perishable)) = 'true'                             AS is_perishable,
    CAST(TRY_STRPTIME(launch_date, '%Y-%m-%d') AS DATE)             AS launch_date,
    CASE WHEN failure_reason IS NULL THEN 'PASSED' ELSE 'FAILED' END AS dq_status,
    failure_reason,
    NULL::VARCHAR                                                   AS dq_warnings,
    COLUMNS('^chk__'),
    _source_file, _source_row, _run_id, _loaded_at
FROM flagged;
