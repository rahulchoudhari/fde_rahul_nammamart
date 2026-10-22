"""
dags/nammamart_elt_rahul.py
----------------------------
NammaMart ELT pipeline: Extract → Load raw → Transform in the warehouse with SQL.
Every raw row is kept; bad rows are flagged in staging and filtered out of the marts.

  extract[orders]    ──┐
  extract[payments]  ──┤
  extract[customers] ──┼──► load_raw ──► build_staging ──► merge_increments ──► record_dq ──► build_marts ──► report ──► audit
  extract[products]  ──┤                                                                                      (all_done)
  extract[stores]    ──┘

Layers in include/warehouse_rahul.duckdb:
  raw_*   exact copy of each CSV, all text, + _loaded_at, _source_file, _run_id, _source_row (append only)
  stg_*   SQL only: typed, cleaned, deduped, PII masked, dq_status + failure_reason   (include/sql/stg_*.sql)
  mart_*  SQL only: business tables for the six questions                             (include/sql/mart_*.sql)

Incremental load: trigger with params {"orders_file": "orders_2026-10-01_increment.csv"}.
The file is appended to raw_orders and MERGEd into stg_orders on order_id.
"""

import sys
from datetime import timedelta

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

sys.path.insert(0, "/usr/local/airflow/include")

DATA_DIR = "/usr/local/airflow/include/data/nammamart"
SQL_DIR = "/usr/local/airflow/include/sql"
DAG_ID = "nammamart_elt_rahul"
FILES = ["orders", "payments", "customers", "products", "stores"]

# The columns each file must have, in order. A different header fails extract loudly.
EXPECTED_COLUMNS = {
    "orders": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity",
               "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status",
               "channel", "delivery_minutes"],
    "payments": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid",
                 "payment_status", "gateway_ref"],
    "customers": ["customer_id", "full_name", "email", "phone", "city", "area", "pincode",
                  "signup_date", "loyalty_tier", "is_active"],
    "products": ["product_id", "product_name", "category", "sub_category", "brand", "mrp",
                 "cost_price", "unit", "is_perishable", "launch_date"],
    "stores": ["store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name",
               "capacity_orders_per_day"],
}

STAGING_SQL = ["elt_setup.sql", "stg_stores.sql", "stg_products.sql", "stg_customers.sql",
               "stg_orders.sql", "stg_payments.sql"]
MART_SQL = ["mart_daily_store_kpis.sql", "mart_category_margin.sql", "mart_delivery_sla.sql",
            "mart_payment_reconciliation.sql", "mart_payment_mode_revenue.sql",
            "mart_top_customers.sql", "mart_rejected_rows.sql"]

default_args = {
    "owner": "rahul",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}


def _connect():
    """Open the warehouse and set the variables the SQL files read with getvariable()."""
    from rahul_warehouse import connect

    context = get_current_context()
    con = connect()
    now_ist = pendulum.now("Asia/Kolkata").naive()     # source timestamps are IST without a zone
    con.execute(f"SET VARIABLE run_id = '{context['run_id']}'")
    con.execute(f"SET VARIABLE dag_id = '{DAG_ID}'")
    con.execute(f"SET VARIABLE run_now = TIMESTAMP '{now_ist:%Y-%m-%d %H:%M:%S}'")
    return con


def _run_sql(con, filename: str):
    with open(f"{SQL_DIR}/{filename}") as f:
        con.execute(f.read())
    print(f"[SQL] ran {filename}")


@dag(
    dag_id=DAG_ID,
    schedule="0 9 * * 1-6",                                          # 9:00 AM, Monday–Saturday
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),    # cron is read in IST, not UTC
    catchup=False,                                                   # don't backfill every missed day
    default_args=default_args,
    params={"orders_file": Param("orders.csv", type="string",
                                 description="Orders file to load, e.g. orders_2026-10-01_increment.csv")},
    tags=["nammamart", "elt", "rahul"],
)
def nammamart_elt_rahul():

    # ── EXTRACT: check each file exists and has the expected columns (parallel) ──

    @task
    def extract(name: str) -> dict:
        import csv

        filename = get_current_context()["params"]["orders_file"] if name == "orders" else f"{name}.csv"
        path = f"{DATA_DIR}/{filename}"
        with open(path, newline="") as f:
            reader = csv.reader(f)
            header = next(reader)
            rows = sum(1 for _ in reader)

        expected = EXPECTED_COLUMNS[name]
        if header != expected:
            missing = [c for c in expected if c not in header]
            unexpected = [c for c in header if c not in expected]
            raise ValueError(f"Schema change in {filename}: missing {missing}, unexpected {unexpected}")

        print(f"[Extract] {filename}: {rows} rows, schema OK")
        return {"name": name, "file": filename, "path": path, "rows": rows}

    # ── LOAD RAW: append every row exactly as received, all columns as text ──

    @task
    def load_raw(extracted: list[dict]) -> int:
        con = _connect()
        total = 0
        for e in extracted:
            table = f"raw_{e['name']}"
            source = f"read_csv('{e['path']}', header = true, all_varchar = true)"
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} AS
                SELECT *, NULL::TIMESTAMPTZ AS _loaded_at, NULL::VARCHAR AS _source_file,
                       NULL::VARCHAR AS _run_id, NULL::BIGINT AS _source_row
                FROM {source} LIMIT 0
            """)
            con.execute(f"""
                INSERT INTO {table} BY NAME
                SELECT *, now() AS _loaded_at, '{e['file']}' AS _source_file,
                       getvariable('run_id') AS _run_id, ROW_NUMBER() OVER () AS _source_row
                FROM {source}
            """)
            all_rows = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"[Load raw] {table}: +{e['rows']} rows from {e['file']} (table now {all_rows} rows)")
            total += e["rows"]
        con.close()
        return total

    # ── STAGING: SQL only ─────────────────────────────────────────────────────

    @task
    def build_staging(_raw_rows: int) -> dict:
        con = _connect()
        for sql in STAGING_SQL:
            _run_sql(con, sql)

        counts = {}
        for table in ["stg_orders", "stg_payments", "stg_customers", "stg_products", "stg_stores"]:
            passed, failed = con.execute(f"""
                SELECT COUNT(*) FILTER (WHERE dq_status = 'PASSED'), COUNT(*) FILTER (WHERE dq_status = 'FAILED')
                FROM {table}
            """).fetchone()
            counts[table] = {"passed": passed, "failed": failed}
            print(f"[Staging] {table:14} passed={passed:5} failed={failed:4}")
        con.close()
        return counts

    @task
    def merge_increments(_staged: dict) -> dict:
        con = _connect()
        increments = con.execute("""
            SELECT DISTINCT _source_file FROM raw_orders WHERE _source_file <> 'orders.csv' ORDER BY 1
        """).fetchall()
        if not increments:
            print("[Merge] No increment files loaded yet. Nothing to merge.")
            con.close()
            return {"inserted": 0, "updated": 0}

        before = con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
        updated = con.execute("""
            SELECT COUNT(DISTINCT s.order_id)
            FROM v_orders_checked s JOIN stg_orders t ON t.order_id = s.order_id AND t.dq_status = 'PASSED'
            WHERE s._source_file <> 'orders.csv' AND s.dq_status = 'PASSED'
        """).fetchone()[0]
        _run_sql(con, "stg_orders_merge.sql")
        after = con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
        con.close()

        print(f"[Merge] files: {[f[0] for f in increments]}")
        print(f"[Merge] stg_orders: {before} → {after} rows ({after - before} inserted, {updated} updated)")
        return {"inserted": after - before, "updated": updated}

    @task
    def record_dq(_merged: dict) -> int:
        con = _connect()
        _run_sql(con, "dq_results.sql")
        results = con.execute("""
            SELECT status, severity, check_name, rows_failed FROM dq_results
            WHERE run_id = getvariable('run_id') AND dag_id = getvariable('dag_id') AND status <> 'PASS'
            ORDER BY check_name
        """).fetchall()
        quarantined = con.execute("""
            SELECT (SELECT COUNT(*) FROM v_orders_checked WHERE dq_status = 'FAILED' AND _run_id = getvariable('run_id'))
                 + (SELECT COUNT(*) FROM stg_payments  WHERE dq_status = 'FAILED' AND _run_id = getvariable('run_id'))
                 + (SELECT COUNT(*) FROM stg_customers WHERE dq_status = 'FAILED' AND _run_id = getvariable('run_id'))
                 + (SELECT COUNT(*) FROM stg_products  WHERE dq_status = 'FAILED' AND _run_id = getvariable('run_id'))
                 + (SELECT COUNT(*) FROM stg_stores    WHERE dq_status = 'FAILED' AND _run_id = getvariable('run_id'))
        """).fetchone()[0]
        con.close()
        for status, severity, name, failed in results:
            print(f"[DQ] {status:4} {severity:8} {name}: {failed} rows")
        print(f"[DQ] rows flagged FAILED this run: {quarantined}")
        return quarantined

    # ── MARTS: SQL only ───────────────────────────────────────────────────────

    @task
    def build_marts(_quarantined: int):
        con = _connect()
        for sql in MART_SQL:
            _run_sql(con, sql)
        for table in [s.removesuffix(".sql") for s in MART_SQL]:
            print(f"[Mart] {table}: {con.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]} rows")
        con.close()

    # ── REPORT: business answers + ETL comparison, printed to the log ─────────

    @task
    def report() -> str:
        con = _connect()

        def show(title, sql):
            print(f"\n[Report] {title}\n{con.execute(sql).df().to_string(index=False)}")

        show("Q1 Net revenue by zone", """
            SELECT zone, SUM(delivered_orders)::INTEGER AS delivered_orders, ROUND(SUM(net_revenue), 2) AS net_revenue
            FROM mart_daily_store_kpis GROUP BY zone ORDER BY net_revenue DESC""")
        show("Q2 Margin by category", "SELECT * FROM mart_category_margin")
        show("Q3 Delivery SLA (worst 5 stores)",
             "SELECT * FROM mart_delivery_sla ORDER BY pct_within_15_min LIMIT 5")
        show("Q4 Revenue by payment mode", "SELECT * FROM mart_payment_mode_revenue")
        show("Q5 Top 10 customers", "SELECT * FROM mart_top_customers")
        show("Q6 Rows rejected this run", "SELECT file, reason, rows_rejected FROM mart_rejected_rows")

        # Part B.4: September totals must match the ETL pipeline
        elt = con.execute("""
            SELECT ROUND(SUM(net_revenue), 2) FROM stg_orders
            WHERE dq_status = 'PASSED' AND order_date < DATE '2026-10-01'""").fetchone()[0]
        has_etl = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'etl_fact_orders'").fetchone()[0]
        if has_etl:
            etl = con.execute("""
                SELECT ROUND(SUM(net_revenue), 2) FROM etl_fact_orders
                WHERE order_date < DATE '2026-10-01'""").fetchone()[0]
            verdict = "MATCH" if abs(elt - etl) < 0.01 else f"DIFFERENT by ₹{elt - etl:,.2f}"
            print(f"\n[Report] September net revenue: ELT ₹{elt:,.2f} | ETL ₹{etl:,.2f} → {verdict}")
        else:
            print(f"\n[Report] September net revenue: ELT ₹{elt:,.2f} (ETL has not run yet)")
        con.close()
        return "SUCCESS"

    # ── AUDIT: one row per run in pipeline_runs, even when something failed ───

    @task(trigger_rule="all_done", retries=0)    # retrying would write duplicate audit rows
    def audit():
        import pandas as pd

        context = get_current_context()
        ti = context["ti"]
        extracted = ti.xcom_pull(task_ids="extract") or []
        status = "SUCCESS" if ti.xcom_pull(task_ids="report") == "SUCCESS" else "FAILED"
        start = context["dag_run"].start_date

        run = pd.DataFrame([{
            "run_id": context["run_id"],
            "dag_id": DAG_ID,
            "start_time": pd.Timestamp(start) if start else None,
            "end_time": pd.Timestamp.now(tz="UTC"),
            "rows_extracted": sum(e["rows"] for e in extracted if e),
            "rows_loaded": ti.xcom_pull(task_ids="load_raw") or 0,          # rows appended to raw_*
            "rows_quarantined": ti.xcom_pull(task_ids="record_dq") or 0,    # rows flagged FAILED in staging
            "status": status,
        }])
        con = _connect()
        _run_sql(con, "elt_setup.sql")          # makes sure pipeline_runs exists even if staging never ran
        con.execute("INSERT INTO pipeline_runs BY NAME SELECT * FROM run")
        con.close()
        print(f"[Audit]\n{run.T.to_string(header=False)}")

        if status == "FAILED":
            raise RuntimeError("Pipeline failed: see the red tasks in this run")

    # ── Wiring ────────────────────────────────────────────────────────────────

    raw_rows = load_raw(extract.expand(name=FILES))
    marts = build_marts(record_dq(merge_increments(build_staging(raw_rows))))
    marts >> report() >> audit()


nammamart_elt_rahul()
