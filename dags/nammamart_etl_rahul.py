"""
dags/nammamart_etl_rahul.py
----------------------------
NammaMart ETL pipeline: Extract → Validate → Transform → Load.
Bad rows are quarantined before they reach the warehouse.

  extract[orders]    ──┐
  extract[payments]  ──┤
  extract[customers] ──┼──► validate ──► transform ──► load ──► reconcile ──► report ──► audit
  extract[products]  ──┤      │                                                          ▲
  extract[stores]    ──┘      └─► quarantine CSVs + dq_results               (all_done) ─┘

Warehouse:  include/warehouse_rahul.duckdb
Quarantine: include/quarantine/rahul/<file>_failed_<run_id>.csv

Incremental load: trigger with params {"orders_file": "orders_2026-10-01_increment.csv"}.
The fact load deletes and re-inserts by order_id, so corrected orders overwrite the
old row and re-runs never double-count.
"""

import os
import re
import sys
from datetime import timedelta

import pendulum
from airflow.sdk import Param, dag, get_current_context, task

sys.path.insert(0, "/usr/local/airflow/include")

DATA_DIR = "/usr/local/airflow/include/data/nammamart"
QUARANTINE_DIR = "/usr/local/airflow/include/quarantine/rahul"
WORK_DIR = "/tmp/nammamart_etl_rahul"      # intermediate files passed between tasks
DAG_ID = "nammamart_etl_rahul"
FILES = ["orders", "payments", "customers", "products", "stores"]

default_args = {
    "owner": "rahul",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}


def _safe_run_id() -> str:
    """run_id contains ':' and '+', which don't belong in file names."""
    return re.sub(r"[^\w-]", "_", get_current_context()["run_id"])


def _work_path(name: str) -> str:
    path = f"{WORK_DIR}/{_safe_run_id()}"
    os.makedirs(path, exist_ok=True)
    return f"{path}/{name}.pkl"


@dag(
    dag_id=DAG_ID,
    schedule="0 9 * * 1-6",                                          # 9:00 AM, Monday–Saturday
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),    # cron is read in IST, not UTC
    catchup=False,                                                   # don't backfill every missed day
    default_args=default_args,
    params={"orders_file": Param("orders.csv", type="string",
                                 description="Orders file to load, e.g. orders_2026-10-01_increment.csv")},
    tags=["nammamart", "etl", "rahul"],
)
def nammamart_etl_rahul():

    # ── EXTRACT: one task per file, all five run in parallel ──────────────────

    @task
    def extract(name: str) -> dict:
        import pandas as pd

        filename = get_current_context()["params"]["orders_file"] if name == "orders" else f"{name}.csv"
        # Read everything as text so checks see the values exactly as received
        df = pd.read_csv(f"{DATA_DIR}/{filename}", dtype=str, keep_default_na=False, na_values=[""])
        path = _work_path(f"raw_{name}")
        df.to_pickle(path)
        print(f"[Extract] {filename}: {len(df)} rows, {len(df.columns)} columns")
        return {"name": name, "file": filename, "rows": len(df), "path": path}

    # ── VALIDATE: run every check, quarantine Critical failures ───────────────

    @task
    def validate(extracted: list[dict]) -> dict:
        from rahul_warehouse import connect
        import pandas as pd
        from nammamart_dq import mask_email, mask_phone, run_checks

        context = get_current_context()
        raw = {e["name"]: pd.read_pickle(e["path"]) for e in extracted}
        files = {e["name"]: e["file"] for e in extracted}
        now = pendulum.now("Asia/Kolkata").naive()     # source timestamps are IST without a zone
        ctx = {"now": pd.Timestamp(now), **raw}

        # Payment checks look up orders. On an increment run, orders holds only the new file,
        # so add the full history from orders.csv (this run's rows first, so corrections win).
        if files["orders"] != "orders.csv":
            history = pd.read_csv(f"{DATA_DIR}/orders.csv", dtype=str, keep_default_na=False, na_values=[""])
            ctx["orders"] = pd.concat([raw["orders"], history], ignore_index=True)

        # Volume baseline: row counts of the last 5 loads of this same orders file
        con = connect()
        con.execute("""CREATE TABLE IF NOT EXISTS etl_load_history
                       (run_id VARCHAR, file VARCHAR, rows BIGINT, loaded_at TIMESTAMPTZ)""")
        ctx["orders_history_rows"] = [r[0] for r in con.execute(
            "SELECT rows FROM etl_load_history WHERE file = ? ORDER BY loaded_at DESC LIMIT 5",
            [files["orders"]]).fetchall()]
        con.close()

        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        all_results, passed_paths, counts = [], {}, {}

        for name in FILES:
            passed, failed, results = run_checks(name, raw[name], ctx, context["run_id"], DAG_ID)
            all_results += results

            passed_paths[name] = _work_path(f"passed_{name}")
            passed.to_pickle(passed_paths[name])

            if len(failed):
                if name == "customers":
                    # Quarantine files are shared for debugging, so they never hold raw PII.
                    # The as-received row is still in the source file, traceable by customer_id.
                    failed["email"] = failed.email.map(mask_email)
                    failed["phone"] = failed.phone.map(mask_phone)
                    failed["full_name"] = "***"
                stem = files[name].removesuffix(".csv")
                q_path = f"{QUARANTINE_DIR}/{stem}_failed_{_safe_run_id()}.csv"
                failed.to_csv(q_path, index=False)
                print(f"[Quarantine] {len(failed)} rows → {q_path}")

            counts[name] = {"in": len(raw[name]), "passed": len(passed), "quarantined": len(failed)}
            print(f"[Validate] {name:10} in={len(raw[name]):5} passed={len(passed):5} quarantined={len(failed):4}")

        for r in all_results:
            if r["status"] != "PASS":
                print(f"[DQ] {r['status']:4} {r['severity']:8} {r['check_name']}: {r['rows_failed']} rows")

        dq = pd.DataFrame(all_results)
        con = connect()
        con.execute("CREATE TABLE IF NOT EXISTS dq_results AS SELECT * FROM dq LIMIT 0")
        con.execute("INSERT INTO dq_results SELECT * FROM dq")
        for e in extracted:
            con.execute("INSERT INTO etl_load_history VALUES (?, ?, ?, now())",
                        [context["run_id"], e["file"], e["rows"]])
        con.close()

        # Order IDs whose fact rows this run replaces. A full file replaces every order in it,
        # so an order that now fails its checks is removed from the fact instead of going stale.
        # An increment only replaces orders that passed, so a bad correction never deletes a good row.
        is_full = files["orders"] == "orders.csv"
        ids = raw["orders"] if is_full else pd.read_pickle(passed_paths["orders"])
        replace_ids_path = _work_path("replace_ids")
        ids[["order_id"]].dropna().drop_duplicates().to_pickle(replace_ids_path)

        return {"passed": passed_paths, "counts": counts, "replace_ids": replace_ids_path}

    # ── TRANSFORM: clean, mask PII, build the star schema ─────────────────────

    @task
    def transform(validated: dict) -> dict:
        import pandas as pd
        from nammamart_dq import (clean_customers, clean_orders, clean_payments,
                                  clean_products, clean_stores)

        p = {name: pd.read_pickle(path) for name, path in validated["passed"].items()}
        orders = clean_orders(p["orders"])
        customers = clean_customers(p["customers"])
        products = clean_products(p["products"])
        stores = clean_stores(p["stores"])
        payments = clean_payments(p["payments"])

        # Join to products for cost_price; customer and store attributes stay in their dimensions
        fact = orders.merge(products[["product_id", "cost_price"]], on="product_id", how="left")
        delivered = fact.order_status == "DELIVERED"
        fact["order_date"] = fact.order_ts.dt.date
        fact["net_revenue"] = fact.order_amount.where(delivered, 0.0)
        fact["gross_margin"] = (fact.order_amount - fact.quantity * fact.cost_price).where(delivered, 0.0)
        fact["within_15_min"] = (fact.delivery_minutes <= 15).where(delivered)
        fact["_run_id"] = get_current_context()["run_id"]
        fact["_loaded_at"] = pd.Timestamp.now(tz="UTC")
        fact = fact.drop(columns=["cost_price"])

        missing_cost = int((delivered & fact.gross_margin.isna()).sum())
        print(f"[Transform] fact rows: {len(fact)} | delivered: {int(delivered.sum())} | "
              f"delivered orders with no product cost (product quarantined): {missing_cost}")

        out = {}
        for name, df in {"fact_orders": fact, "dim_customer": customers, "dim_product": products,
                         "dim_store": stores, "payments": payments}.items():
            out[name] = _work_path(f"clean_{name}")
            df.to_pickle(out[name])
        out["replace_ids"] = validated["replace_ids"]
        return out

    # ── LOAD: idempotent writes to DuckDB ─────────────────────────────────────

    @task
    def load(transformed: dict) -> int:
        from rahul_warehouse import connect
        import pandas as pd

        con = connect()

        # Dimensions and payments are full snapshots: replace them each run
        for name in ["dim_customer", "dim_product", "dim_store", "payments"]:
            df = pd.read_pickle(transformed[name])
            con.execute(f"CREATE OR REPLACE TABLE etl_{name} AS SELECT * FROM df")
            print(f"[Load] etl_{name}: {len(df)} rows")

        # Fact: delete-then-insert by order_id in one transaction.
        # Re-runs replace the same rows; increment corrections overwrite the old version;
        # orders in a full file that now fail their checks are removed (see validate).
        fact = pd.read_pickle(transformed["fact_orders"])
        replace_ids = pd.read_pickle(transformed["replace_ids"])
        con.execute("CREATE TABLE IF NOT EXISTS etl_fact_orders AS SELECT * FROM fact LIMIT 0")
        before = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]
        con.execute("BEGIN")
        replaced = con.execute(
            "SELECT COUNT(*) FROM etl_fact_orders WHERE order_id IN (SELECT order_id FROM fact)"
        ).fetchone()[0]
        removed = con.execute("""
            SELECT COUNT(*) FROM etl_fact_orders
            WHERE order_id IN (SELECT order_id FROM replace_ids) AND order_id NOT IN (SELECT order_id FROM fact)
        """).fetchone()[0]
        con.execute("DELETE FROM etl_fact_orders WHERE order_id IN (SELECT order_id FROM replace_ids)")
        con.execute("INSERT INTO etl_fact_orders BY NAME SELECT * FROM fact")
        con.execute("COMMIT")
        after = con.execute("SELECT COUNT(*) FROM etl_fact_orders").fetchone()[0]
        con.close()

        print(f"[Load] etl_fact_orders: {before} → {after} rows ({len(fact) - replaced} inserted, "
              f"{replaced} replaced/updated, {removed} removed because they now fail a check)")
        return len(fact)

    # ── RECONCILE: every order/payment mismatch, with a reason ────────────────

    @task
    def reconcile(_loaded: int) -> int:
        from rahul_warehouse import connect

        con = connect()
        con.execute("""
            CREATE OR REPLACE TABLE etl_payment_reconciliation AS
            WITH paid AS (
                SELECT order_id,
                       SUM(amount_paid) FILTER (WHERE payment_status = 'SUCCESS') AS success_amount,
                       COUNT(*)         FILTER (WHERE payment_status = 'SUCCESS') AS success_count
                FROM etl_payments GROUP BY order_id
            )
            -- Delivered orders with no successful payment
            SELECT o.order_id, NULL AS payment_id, o.order_status, o.payment_mode,
                   o.order_amount, 0.0 AS amount_paid, o.order_amount AS difference,
                   'DELIVERED_NOT_PAID' AS reason
            FROM etl_fact_orders o LEFT JOIN paid p USING (order_id)
            WHERE o.order_status = 'DELIVERED' AND COALESCE(p.success_count, 0) = 0

            UNION ALL
            -- Successful payment that doesn't equal the order amount
            SELECT o.order_id, pay.payment_id, o.order_status, pay.payment_mode,
                   o.order_amount, pay.amount_paid, ROUND(o.order_amount - pay.amount_paid, 2),
                   'AMOUNT_MISMATCH'
            FROM etl_payments pay JOIN etl_fact_orders o USING (order_id)
            WHERE pay.payment_status = 'SUCCESS' AND ABS(o.order_amount - pay.amount_paid) > 0.01

            UNION ALL
            -- Cancelled order that was still charged
            SELECT o.order_id, pay.payment_id, o.order_status, pay.payment_mode,
                   o.order_amount, pay.amount_paid, ROUND(o.order_amount - pay.amount_paid, 2),
                   'CANCELLED_BUT_PAID'
            FROM etl_payments pay JOIN etl_fact_orders o USING (order_id)
            WHERE pay.payment_status = 'SUCCESS' AND o.order_status = 'CANCELLED'

            UNION ALL
            -- Payment for an order that isn't in the finance table (quarantined or unknown)
            SELECT pay.order_id, pay.payment_id, NULL, pay.payment_mode,
                   NULL, pay.amount_paid, NULL, 'ORDER_NOT_IN_FACT'
            FROM etl_payments pay
            WHERE pay.order_id NOT IN (SELECT order_id FROM etl_fact_orders)
        """)
        summary = con.execute("""
            SELECT reason, COUNT(*) AS rows, ROUND(SUM(ABS(difference)), 2) AS amount
            FROM etl_payment_reconciliation GROUP BY reason ORDER BY rows DESC
        """).df()
        total = con.execute("SELECT COUNT(*) FROM etl_payment_reconciliation").fetchone()[0]
        con.close()
        print(f"[Reconcile] {total} mismatches\n{summary.to_string(index=False)}")
        return total

    # ── REPORT: business tables, printed to the log ───────────────────────────

    @task
    def report(_mismatches: int) -> str:
        from rahul_warehouse import connect

        con = connect()
        con.execute("""
            CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT f.order_date,
                   COALESCE(s.zone, 'Unknown')       AS zone,
                   COUNT(*)                          AS delivered_orders,
                   ROUND(SUM(f.net_revenue), 2)      AS net_revenue
            FROM etl_fact_orders f LEFT JOIN etl_dim_store s USING (store_id)
            WHERE f.order_status = 'DELIVERED'
            GROUP BY ALL ORDER BY f.order_date, zone
        """)
        con.execute("""
            CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT COALESCE(p.category, 'Unknown')   AS category,
                   COUNT(*)                          AS delivered_orders,
                   ROUND(SUM(f.net_revenue), 2)      AS net_revenue,
                   ROUND(SUM(f.gross_margin), 2)     AS gross_margin,
                   ROUND(100 * SUM(f.gross_margin) / SUM(f.net_revenue), 1) AS margin_pct
            FROM etl_fact_orders f LEFT JOIN etl_dim_product p USING (product_id)
            WHERE f.order_status = 'DELIVERED'
            GROUP BY ALL ORDER BY net_revenue DESC
        """)
        by_zone = con.execute("""
            SELECT zone, SUM(delivered_orders)::INTEGER AS delivered_orders, ROUND(SUM(net_revenue), 2) AS net_revenue
            FROM etl_daily_zone_revenue GROUP BY zone ORDER BY net_revenue DESC
        """).df()
        margin = con.execute("SELECT * FROM etl_category_margin").df()
        total = con.execute("SELECT ROUND(SUM(net_revenue), 2) FROM etl_fact_orders").fetchone()[0]
        con.close()

        print(f"[Report] Net revenue by zone (all days):\n{by_zone.to_string(index=False)}")
        print(f"[Report] Margin by category:\n{margin.to_string(index=False)}")
        print(f"[Report] Total net revenue: ₹{total:,.2f}")
        return "SUCCESS"

    # ── AUDIT: one row per run in pipeline_runs, even when something failed ───

    @task(trigger_rule="all_done", retries=0)    # retrying would write duplicate audit rows
    def audit():
        from rahul_warehouse import connect
        import pandas as pd

        context = get_current_context()
        ti = context["ti"]
        extracted = ti.xcom_pull(task_ids="extract") or []
        validated = ti.xcom_pull(task_ids="validate") or {}
        loaded = ti.xcom_pull(task_ids="load")
        status = "SUCCESS" if ti.xcom_pull(task_ids="report") == "SUCCESS" else "FAILED"

        start = context["dag_run"].start_date
        run = pd.DataFrame([{
            "run_id": context["run_id"],
            "dag_id": DAG_ID,
            "start_time": pd.Timestamp(start) if start else None,
            "end_time": pd.Timestamp.now(tz="UTC"),
            "rows_extracted": sum(e["rows"] for e in extracted if e),
            "rows_loaded": loaded or 0,
            "rows_quarantined": sum(c["quarantined"] for c in validated.get("counts", {}).values()),
            "status": status,
        }])
        con = connect()
        con.execute("CREATE TABLE IF NOT EXISTS pipeline_runs AS SELECT * FROM run LIMIT 0")
        con.execute("INSERT INTO pipeline_runs SELECT * FROM run")
        con.close()
        print(f"[Audit]\n{run.T.to_string(header=False)}")

        if status == "FAILED":
            raise RuntimeError("Pipeline failed: see the red tasks in this run")

    # ── Wiring ────────────────────────────────────────────────────────────────

    extracted = extract.expand(name=FILES)
    validated = validate(extracted)
    loaded = load(transform(validated))
    reported = report(reconcile(loaded))
    reported >> audit()


nammamart_etl_rahul()
