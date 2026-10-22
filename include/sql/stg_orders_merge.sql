-- include/sql/stg_orders_merge.sql
-- Incremental load: upsert every increment file (e.g. orders_2026-10-01_increment.csv)
-- into stg_orders on order_id.
--   * order already in stg_orders (and PASSED) → row is overwritten (e.g. DELIVERED → RETURNED)
--   * new order                                → row is inserted
-- A correction that fails a Critical check never overwrites a good row.
-- Runs on every DAG run after stg_orders.sql rebuilds September, so the result is the
-- same however many times it runs.

MERGE INTO stg_orders AS t
USING (
    SELECT * FROM v_orders_checked
    WHERE _source_file <> 'orders.csv'
    -- if an order appears in several increment files, the newest file wins
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _source_file DESC, _source_row DESC) = 1
) AS s
ON t.order_id = s.order_id AND t.dq_status = 'PASSED'
WHEN MATCHED AND s.dq_status = 'PASSED' THEN UPDATE
WHEN NOT MATCHED THEN INSERT;
