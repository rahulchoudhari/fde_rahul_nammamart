-- include/sql/dq_results.sql
-- Record one row per check for this run in dq_results (the quality scorecard).
-- Each staging table has one boolean column per check, named chk__<check_name>.
-- Only rows loaded in this run are counted, same as the ETL pipeline.
-- Uses session variables: run_id, dag_id, run_now.

INSERT INTO dq_results BY NAME
WITH flags AS (
    UNPIVOT (SELECT COLUMNS('^chk__')::INTEGER FROM v_orders_checked WHERE _run_id = getvariable('run_id'))
        ON COLUMNS('^chk__') INTO NAME check_name VALUE failed
    UNION ALL
    UNPIVOT (SELECT COLUMNS('^chk__')::INTEGER FROM stg_payments WHERE _run_id = getvariable('run_id'))
        ON COLUMNS('^chk__') INTO NAME check_name VALUE failed
    UNION ALL
    UNPIVOT (SELECT COLUMNS('^chk__')::INTEGER FROM stg_customers WHERE _run_id = getvariable('run_id'))
        ON COLUMNS('^chk__') INTO NAME check_name VALUE failed
    UNION ALL
    UNPIVOT (SELECT COLUMNS('^chk__')::INTEGER FROM stg_products WHERE _run_id = getvariable('run_id'))
        ON COLUMNS('^chk__') INTO NAME check_name VALUE failed
    UNION ALL
    UNPIVOT (SELECT COLUMNS('^chk__')::INTEGER FROM stg_stores WHERE _run_id = getvariable('run_id'))
        ON COLUMNS('^chk__') INTO NAME check_name VALUE failed
),
row_checks AS (
    SELECT replace(check_name, 'chk__', '') AS check_name,
           COUNT(*)    AS rows_checked,
           SUM(failed) AS rows_failed
    FROM flags GROUP BY 1
),
-- Table-level check: is the newest (non-future) order more than 1 day old?
freshness AS (
    SELECT 'orders_freshness' AS check_name,
           1 AS rows_checked,
           CASE WHEN MAX(order_ts) FILTER (WHERE order_ts <= getvariable('run_now'))
                     < getvariable('run_now') - INTERVAL 1 DAY THEN 1 ELSE 0 END AS rows_failed
    FROM v_orders_checked WHERE _run_id = getvariable('run_id')
),
-- Table-level check: does this orders file have under 10% of its usual rows
-- (median of the last 5 loads of the same file name)?
loads AS (
    SELECT _run_id, _source_file, COUNT(*) AS n, MAX(_loaded_at) AS loaded_at
    FROM raw_orders GROUP BY ALL
),
this_load AS (
    SELECT * FROM loads WHERE _run_id = getvariable('run_id')
),
history AS (
    SELECT l.n FROM loads l JOIN this_load t USING (_source_file)
    WHERE l._run_id <> t._run_id
    ORDER BY l.loaded_at DESC LIMIT 5
),
volume AS (
    SELECT 'orders_volume' AS check_name,
           1 AS rows_checked,
           CASE WHEN (SELECT n FROM this_load) < 0.1 * (SELECT MEDIAN(n) FROM history) THEN 1 ELSE 0 END AS rows_failed
)
SELECT
    getvariable('run_id')   AS run_id,
    getvariable('dag_id')   AS dag_id,
    c.check_name, c.dimension, c.severity,
    r.rows_checked, r.rows_failed,
    CASE WHEN r.rows_failed = 0 THEN 'PASS'
         WHEN c.severity = 'Critical' THEN 'FAIL' ELSE 'WARN' END AS status,
    now()                   AS checked_at
FROM (SELECT * FROM row_checks UNION ALL SELECT * FROM freshness UNION ALL SELECT * FROM volume) r
JOIN elt_dq_catalog c USING (check_name);
