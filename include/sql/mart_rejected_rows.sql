-- include/sql/mart_rejected_rows.sql
-- Business question 6: rows rejected in this run, by file and by reason.
-- A row that broke two rules is counted once per reason.
-- Uses session variable: run_id.

CREATE OR REPLACE TABLE mart_rejected_rows AS
WITH failed AS (
    SELECT _source_file, failure_reason FROM v_orders_checked
    WHERE dq_status = 'FAILED' AND _run_id = getvariable('run_id')
    UNION ALL SELECT _source_file, failure_reason FROM stg_payments  WHERE dq_status = 'FAILED' AND _run_id = getvariable('run_id')
    UNION ALL SELECT _source_file, failure_reason FROM stg_customers WHERE dq_status = 'FAILED' AND _run_id = getvariable('run_id')
    UNION ALL SELECT _source_file, failure_reason FROM stg_products  WHERE dq_status = 'FAILED' AND _run_id = getvariable('run_id')
    UNION ALL SELECT _source_file, failure_reason FROM stg_stores    WHERE dq_status = 'FAILED' AND _run_id = getvariable('run_id')
),
reasons AS (
    SELECT _source_file, unnest(string_split(failure_reason, '; ')) AS reason FROM failed
)
SELECT
    getvariable('run_id')   AS run_id,
    _source_file            AS file,
    reason,
    COUNT(*)                AS rows_rejected
FROM reasons
GROUP BY ALL
ORDER BY file, rows_rejected DESC;
