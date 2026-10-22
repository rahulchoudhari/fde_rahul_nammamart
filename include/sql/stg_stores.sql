-- include/sql/stg_stores.sql
-- Staging for stores: cast types, apply quality rules.

CREATE OR REPLACE TABLE stg_stores AS
WITH latest AS (
    SELECT * FROM raw_stores QUALIFY _loaded_at = MAX(_loaded_at) OVER ()
),
checked AS (
    SELECT *,
        zone IS NULL                                                        AS chk__stores_zone_not_null,
        COALESCE(TRY_CAST(capacity_orders_per_day AS INTEGER) <= 0, FALSE)  AS chk__stores_capacity_positive
    FROM latest
)
SELECT
    TRIM(store_id)                                                  AS store_id,
    TRIM(store_name)                                                AS store_name,
    TRIM(zone)                                                      AS zone,
    TRIM(area)                                                      AS area,
    TRIM(pincode)                                                   AS pincode,
    CAST(TRY_STRPTIME(open_date, '%Y-%m-%d') AS DATE)               AS open_date,
    TRIM(manager_name)                                              AS manager_name,
    TRY_CAST(capacity_orders_per_day AS INTEGER)                    AS capacity_orders_per_day,
    CASE WHEN chk__stores_zone_not_null THEN 'FAILED' ELSE 'PASSED' END AS dq_status,
    CASE WHEN chk__stores_zone_not_null THEN 'stores_zone_not_null' END AS failure_reason,
    CASE WHEN chk__stores_capacity_positive THEN 'stores_capacity_positive' END AS dq_warnings,
    COLUMNS('^chk__'),
    _source_file, _source_row, _run_id, _loaded_at
FROM checked;
