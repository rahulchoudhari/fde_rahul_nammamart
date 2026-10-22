# NammaMart ETL & ELT Pipeline — Submission


## Part 0: Discovery and data profiling

### Data Issues Log

Profiled on 2026-10-21. All 6 files have at least one issue. Some rows break more than one rule (for example ORD002103 has a discount of 120% **and** a wrong `order_amount`), so they appear more than once.

#### orders.csv (2,412 rows)

| File | Row key | Column | Problem | Dimension | Rule | Severity |
|---|---|---|---|---|---|---|
| orders.csv | ORD000539, ORD000783, ORD001110, ORD001239, ORD001326, ORD001434, ORD001441, ORD001462, ORD001549, ORD001692, ORD001870, ORD002129 (12) | order_id | Whole row appears twice | Uniqueness | order_id must be unique | Critical |
| orders.csv | ORD000022, ORD000580, ORD000583, ORD000701, ORD000764, ORD000790, ORD000997, ORD001332, ORD001373, ORD001532, ORD001918, ORD002019, ORD002149, ORD002156, ORD002219, ORD002255, ORD002262, ORD002367 (18) | customer_id | Customer is empty | Completeness | customer_id not null | Critical |
| orders.csv | ORD000362 | order_ts | Timestamp is empty | Completeness | order_ts not null | Critical |
| orders.csv | ORD000403, ORD000534, ORD000946, ORD001610, ORD002012 (5) | order_ts | Not a real date: `2026-13-05`, `31/09/2026`, `N/A` | Validity | order_ts parses as `YYYY-MM-DD HH:MM:SS` | Critical |
| orders.csv | ORD001089, ORD001121, ORD001693, ORD001725, ORD001765, ORD001965, ORD002119 (7) | order_ts | Dated after the September export, up to `2027-01-01` | Timeliness | order_ts not in the future | Critical |
| orders.csv | ORD000153, ORD000500, ORD001066, ORD001637, ORD001970 (5) | order_status | `Pending??` (not allowed) and `delivered` (wrong casing) | Validity | status in DELIVERED / CANCELLED / RETURNED, after uppercasing | Critical |
| orders.csv | ORD000159, ORD000433, ORD000691, ORD000934, ORD001364, ORD001414, ORD001640, ORD001812, ORD001874, ORD002356 (10) | payment_mode | `upi`, `UPI ` (space), `CARD`, `cod`, `Cashh` | Validity | Map to UPI / Card / Cash on Delivery / Wallet; anything unmappable fails | Warning |
| orders.csv | ORD000099, ORD000462, ORD000469, ORD000867, ORD001124, ORD001296, ORD001384, ORD001459, ORD001724, ORD002360 (10) | quantity | Quantity is 0 or negative (-1 to -3) | Accuracy | quantity > 0 | Critical |
| orders.csv | ORD001237, ORD001468, ORD002103 (3) | discount_pct | Discount of 120%, 150%, 250% | Validity | discount_pct between 0 and 100 | Critical |
| orders.csv | ORD000677, ORD000801, ORD000845, ORD000880, ORD000964, ORD001237, ORD001274, ORD001468, ORD001624, ORD001755, ORD002004, ORD002031, ORD002103, ORD002128, ORD002223, ORD002244, ORD002298, ORD002305 (18) | order_amount | Doesn't equal quantity × unit_price × (1 − discount/100) | Accuracy | order_amount matches the formula within ₹0.01 | Critical |
| orders.csv | ORD000326, ORD000731, ORD000878, ORD001277, ORD001515, ORD001554, ORD001800, ORD002229 (8) | delivery_minutes | DELIVERED but no delivery time | Completeness | DELIVERED orders have delivery_minutes | Critical |
| orders.csv | ORD000020, ORD000424, ORD000455, ORD001037, ORD001104, ORD002101 (6) | delivery_minutes | 480, 900, 1440 minutes (hours, not minutes) | Accuracy | delivery_minutes < 120 | Critical |
| orders.csv | ORD000070, ORD000134, ORD000596, ORD000789, ORD000965, ORD000986, ORD001058, ORD001685, ORD002179 (9) | product_id | Product doesn't exist (P9212, P9399, …) | Consistency | product_id exists in products.csv | Critical |
| orders.csv | ORD001046, ORD001275, ORD001492, ORD001799, ORD002126 (5) | store_id | Store doesn't exist (S00, S17, S99) | Consistency | store_id exists in stores.csv | Critical |

#### payments.csv (2,228 rows)

| File | Row key | Column | Problem | Dimension | Rule | Severity |
|---|---|---|---|---|---|---|
| payments.csv | PAY0000509, PAY0000755, PAY0001651, PAY0002000 (4) | payment_id | Whole row appears twice | Uniqueness | payment_id must be unique | Critical |
| payments.csv | PAY0002217 – PAY0002224 (8) | order_id | Payment for an order that doesn't exist (ORD941071, …) | Consistency | order_id exists in orders.csv | Critical |
| payments.csv | PAY0000116, PAY0000737, PAY0000751, PAY0000791, PAY0000944, PAY0000954, PAY0001111, PAY0001322, PAY0001568, PAY0002175 (10) | amount_paid | Amount is 0 or negative | Accuracy | amount_paid > 0 | Critical |
| payments.csv | PAY0000044, PAY0000161, PAY0000323, PAY0000354, PAY0000429, PAY0000586, PAY0000779, PAY0000909, PAY0001139, PAY0001167, PAY0001499, PAY0001813, PAY0002183 (13) | amount_paid | SUCCESS payment doesn't equal the order's order_amount | Consistency | SUCCESS amount_paid = order_amount | Warning (listed in the reconciliation table, not quarantined) |
| payments.csv | PAY0000044, PAY0000522, PAY0000817, PAY0001359, PAY0001850, PAY0001862, PAY0001954, PAY0002113, PAY0002188, PAY0002208 (10) | payment_mode | `upi`, `UPI `, `CARD`, `cod`, `Cashh` | Validity | Map to the 4 allowed modes | Warning |
| payments.csv | 301 rows (285 of them SUCCESS) | gateway_ref | Gateway reference is empty | Completeness | SUCCESS payments have a gateway_ref | Warning |

#### customers.csv (405 rows)

| File | Row key | Column | Problem | Dimension | Rule | Severity |
|---|---|---|---|---|---|---|
| customers.csv | C00036, C00103, C00226, C00252, C00313 (5) | customer_id | Same ID appears twice with different details | Uniqueness | one row per customer_id | Critical |
| customers.csv | C00045, C00062, C00063, C00067, C00112, C00193, C00307, C00310, C00327, C00337, C00363, C00392 (12) | email | Missing `@` or domain, or `N/A` | Validity | email matches `name@domain.tld` | Warning |
| customers.csv | C00095, C00130, C00151, C00240, C00290, C00339, C00357 (7) | phone | 7–9 digits instead of 10 | Validity | phone is exactly 10 digits | Warning |
| customers.csv | C00083, C00132, C00166 (3) | pincode | `5600XX` | Validity | pincode is 6 digits | Warning |
| customers.csv | C00142, C00219, C00233, C00328, C00355, C00394 (6) | city | City is empty | Completeness | city not null | Warning |
| customers.csv | C00038, C00398, C00400 (3) | signup_date | Signed up in the future (up to 2027-01-05) | Timeliness | signup_date not in the future | Critical |
| customers.csv | C00031, C00032, C00092, C00133, C00136, C00171, C00196, C00199 (8) | loyalty_tier | `BRONZE`, `bronze`, `SILVER` | Consistency | Standardise to Bronze / Silver / Gold / Platinum | Warning |

#### products.csv (152 rows)

| File | Row key | Column | Problem | Dimension | Rule | Severity |
|---|---|---|---|---|---|---|
| products.csv | P0030, P0140 (2) | product_id | Same ID appears twice | Uniqueness | one row per product_id | Critical |
| products.csv | P0051, P0060, P0068 (3) | category | Category is empty | Completeness | category not null | Critical |
| products.csv | P0109 | mrp | MRP is -203 | Accuracy | mrp > 0 | Critical |
| products.csv | P0019, P0073, P0074, P0109, P0117 (5) | cost_price | Cost is higher than MRP (sold at a loss) | Accuracy | cost_price <= mrp | Critical |

#### stores.csv (15 rows)

| File | Row key | Column | Problem | Dimension | Rule | Severity |
|---|---|---|---|---|---|---|
| stores.csv | S07 | zone | Zone is empty | Completeness | zone not null | Critical |
| stores.csv | S12 | capacity_orders_per_day | Capacity is 0 | Accuracy | capacity_orders_per_day > 0 | Warning |

#### orders_2026-10-01_increment.csv (160 rows)

| File | Row key | Column | Problem | Dimension | Rule | Severity |
|---|---|---|---|---|---|---|
| orders_2026-10-01_increment.csv | 10 order_ids already in orders.csv | order_status | Existing orders changed to RETURNED | Consistency | Upsert on order_id: update the old row, don't add a duplicate | — (expected; tests the incremental load) |
| orders_2026-10-01_increment.csv | 150 new order_ids | — | New orders for 1 October | Timeliness | Append; latest order_ts not older than 1 day | — |

### Questions for the client

Nobody will answer these, so each question has the assumption my pipeline uses.

1. **Are the 12 duplicate orders (e.g. ORD000539) double exports or real repeat orders?**
   Assumption: double exports, because the whole row is identical. Keep one copy; the extra copy is quarantined.
2. **Is a delivery time like 1440 minutes a real delivery, or hours/seconds typed into a minutes field?**
   Assumption: data error. Anything 120 minutes or more is quarantined so it doesn't distort the 10-minute promise KPI.
3. **Are small spelling differences like `upi`, `cod` and `delivered` errors, or different values?**
   Assumption: same value with bad formatting. Clean and keep them (Warning). Only values that can't be mapped, like `Pending??`, are rejected.

## Part A: ETL pipeline

**DAG:** [`dags/nammamart_etl_rahul.py`](dags/nammamart_etl_rahul.py)
**Helper code:** [`include/nammamart_dq.py`](include/nammamart_dq.py) (checks, cleaning, masking) and [`include/rahul_warehouse.py`](include/rahul_warehouse.py) (warehouse connection)
**Warehouse:** `include/warehouse_rahul.duckdb`

```
extract[orders]    ──┐
extract[payments]  ──┤
extract[customers] ──┼──► validate ──► transform ──► load ──► reconcile ──► report ──► audit
extract[products]  ──┤      │                                                          ▲
extract[stores]    ──┘      └─► quarantine CSVs + dq_results               (all_done) ─┘
```

| Task | What it does |
|---|---|
| `extract` ×5 | One mapped task per file, all 5 running in parallel. Reads the CSV with every column as text (`dtype=str`), so the checks see values exactly as received. |
| `validate` | Runs the 36 checks from Part D with `run_checks()`. Rows that fail any Critical check are written to `include/quarantine/rahul/<file>_failed_<run_id>.csv` with a `failure_reason` column. Customer emails and phones are masked even in quarantine. All results go to `dq_results`. |
| `transform` | Cleans the passed rows: trims text, uppercases statuses, maps `upi`/`cod`/`Cashh` to the allowed modes, fixes tier casing. Masks email (`pr***@example.com`) and phone (`98******21`) and drops `full_name`. Joins orders to products for `cost_price`, then derives `order_date`, `net_revenue` (DELIVERED only), `gross_margin` and `within_15_min`. |
| `load` | Writes a star schema. The dimensions (`etl_dim_customer`, `etl_dim_product`, `etl_dim_store`) and `etl_payments` are full snapshots, replaced each run. The fact `etl_fact_orders` uses **delete-then-insert by `order_id` in one transaction**, so a re-run replaces the same rows instead of adding them, and an increment correction overwrites the old row. On a full-file load, orders that now fail a check are also removed, so they can't go stale in the fact table (bug found in Part F, Exp 9). |
| `reconcile` | Builds `etl_payment_reconciliation`: one row per mismatch, with one of 4 reasons (`DELIVERED_NOT_PAID`, `AMOUNT_MISMATCH`, `CANCELLED_BUT_PAID`, `ORDER_NOT_IN_FACT`). |
| `report` | Builds `etl_daily_zone_revenue` and `etl_category_margin` and prints them in the log. |
| `audit` | `trigger_rule="all_done"`, so it runs even after a failure. It writes one row to `pipeline_runs`, then fails on purpose if the run failed, so the run stays red. |

**Incremental load:** trigger with config `{"orders_file": "orders_2026-10-01_increment.csv"}`.

**Results (September data):** 5,212 rows extracted, 141 quarantined (orders 100, payments 22, customers 8, products 10, stores 1), 2,312 orders in `etl_fact_orders`, September net revenue ₹11,35,628.60.

## Part B: ELT pipeline

**DAG:** [`dags/nammamart_elt_rahul.py`](dags/nammamart_elt_rahul.py)
**SQL:** [`include/sql/`](include/sql/), one file per staging table, per mart, plus setup, MERGE and dq_results.

```
extract ×5 ──► load_raw ──► build_staging ──► merge_increments ──► record_dq ──► build_marts ──► report ──► audit (all_done)
```

| Layer | Tables | How it's built |
|---|---|---|
| Raw | `raw_orders`, `raw_payments`, `raw_customers`, `raw_products`, `raw_stores` | DuckDB `read_csv(..., all_varchar = true)`: every column as text, no cleaning. Adds `_loaded_at`, `_source_file`, `_run_id` and `_source_row` (the row number in the file). **Append only**, never updated or deleted. |
| Staging | `stg_orders`, `stg_payments`, `stg_customers`, `stg_products`, `stg_stores` | SQL only ([stg_*.sql](include/sql/)). Reads the latest load of each file, casts types with `TRY_CAST`/`TRY_STRPTIME`, standardises, dedupes, masks PII with the SQL macros `mask_email()`/`mask_phone()`, and drops `full_name`. Every row keeps one true/false column per check (`chk__<check_name>`), plus `dq_status` (PASSED/FAILED), `failure_reason` (Critical checks) and `dq_warnings`. |
| Mart | `mart_daily_store_kpis`, `mart_category_margin`, `mart_delivery_sla`, `mart_payment_reconciliation`, `mart_top_customers`, plus `mart_payment_mode_revenue` (Q4) and `mart_rejected_rows` (Q6) | SQL only ([mart_*.sql](include/sql/)). Reads only `dq_status = 'PASSED'` rows. |

`extract` doesn't read the data. It checks that each file exists and that its header matches the expected columns, so a schema change fails loudly before anything is loaded.

**Incremental load** ([stg_orders_merge.sql](include/sql/stg_orders_merge.sql)):

1. Raw: the increment file is appended to `raw_orders`, with `_source_file = 'orders_2026-10-01_increment.csv'`.
2. Staging: `stg_orders` is rebuilt from `orders.csv`, then every increment file is merged into it:
   ```sql
   MERGE INTO stg_orders AS t USING (<checked increment rows>) AS s
   ON t.order_id = s.order_id AND t.dq_status = 'PASSED'
   WHEN MATCHED AND s.dq_status = 'PASSED' THEN UPDATE
   WHEN NOT MATCHED THEN INSERT;
   ```
   Result: **150 orders inserted, 10 updated** (DELIVERED → RETURNED), no duplicates. Because the MERGE runs again after every staging rebuild, the corrections survive any later re-run.

**Do ELT totals match ETL?** Yes. September net revenue is **₹11,35,628.60 in both**, with the same 141 rejected rows and the same zone and category numbers. The `report` task compares the two on every run and prints `MATCH`.

The totals differed only once, by **₹4,193.70**, when the ELT had loaded the increment and the ETL hadn't yet. That's exactly the revenue of the 10 orders that became RETURNED. After the ETL loaded the increment too, they matched again.

Matching needs identical rules in both pipelines. The same "now" time is used for the future-date checks, both pipelines keep the first copy of a duplicate, and both check product and store keys against the master file as received.

## Part C: Scheduling

Both DAGs use the same schedule settings:

```python
@dag(
    schedule="0 9 * * 1-6",                                          # 9:00 AM, Monday–Saturday
    start_date=pendulum.datetime(2026, 10, 1, tz="Asia/Kolkata"),    # cron is read in IST, not UTC
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=10),
    },
)
```

![Schedule of both DAGs in the Airflow UI](ScheduleScreenShot.png)

**Cron expression:** `0 9 * * 1-6`

It reads as: minute `0`, hour `9`, any day of the month, any month, days of the week `1-6` (Monday to Saturday; Sunday is `0`). Because the `start_date` has `tz="Asia/Kolkata"`, Airflow reads the cron in IST, so the run fires at **9:00 AM IST**.

**Time shown in UTC in the Airflow UI:** **03:30 UTC**, because IST is UTC+5:30.

My screenshot shows the next run as `2026-10-21 23:30:00`. That's not UTC: the Airflow UI shows times in the browser's timezone, and my laptop is set to US Eastern time (EDT, UTC−4). It's the same moment in three timezones:

| Timezone | Next run |
|---|---|
| IST (Asia/Kolkata) | Thu 22 Oct 2026, 09:00 |
| UTC (what Airflow stores) | Thu 22 Oct 2026, 03:30 |
| EDT (my browser) | Wed 21 Oct 2026, 23:30 |

So "the time in the UI" depends on who is looking. NammaMart's team should set the UI timezone to Asia/Kolkata (user menu → timezone) so nobody misreads it.

**What would happen at 9 AM if `start_date` were in UTC:** Airflow would read `0 9 * * 1-6` as 9:00 AM **UTC**, which is **2:30 PM IST**. The report would arrive five hours after Kavya's 9:30 AM stand-up, which is worse than today's manual 10:30 AM email. Nothing would error or warn: the DAG would run successfully, just at the wrong time. That's why the timezone must be explicit.

**What happens if the laptop or Docker is off at 9 AM:** Nothing runs. The Airflow scheduler runs inside Docker on my laptop, so if Docker is off at 9:00, no process exists to start the run. With `catchup=False`, Airflow does not go back and run the missed slot when Docker comes back. It waits for the next scheduled time. I saw this myself: I unpaused `nammamart_etl_rahul` after the 21 Oct 9:00 slot had passed, and Airflow didn't create a run for 21 Oct. The next run in the screenshot is 22 Oct. For NammaMart that means no report that morning, and nobody is told. Someone would have to notice and click **Trigger** by hand.

The fix is to run Airflow on a machine that's always on (a server, or Astronomer's managed service), not a laptop. Also add an alert when the 9:00 run hasn't succeeded by 9:15.

**Why `catchup=False`:** With `catchup=True`, Airflow creates a run for every missed 9:00 slot since `start_date` (1 Oct 2026). That's about 18 runs the moment the DAG is unpaused. For this pipeline, those runs would be useless:

- **Each run loads the same files.** The CSVs in `include/data/nammamart/` are a snapshot, not one file per day, so 18 backfill runs would reprocess identical data 18 times.
- **They would fight over the warehouse.** Every run writes to the same single-writer DuckDB file, so the runs would queue behind each other.
- **The ELT raw tables would grow by 18 copies** of every file, and `dq_results` and `pipeline_runs` would fill with meaningless history.

The client needs **today's** report at 9 AM, not a replay of past days. Missed days can be run on purpose with **Trigger** if ever needed.

**Retries and timeouts:**

| Setting | Value | Why |
|---|---|---|
| `retries` | 2 | Handles brief problems, like a file still being copied or the warehouse locked by the other DAG. Two retries are enough without hiding a real failure. |
| `retry_delay` | 1 minute | Short enough that all retries finish by about 9:05, well before the 9:30 stand-up. |
| `execution_timeout` | 10 minutes | A full run takes about 5 seconds. A task stuck for 10 minutes is broken, so it fails loudly instead of hanging past 9:30. |

## Part D: Data quality checks

Every check runs in both pipelines and writes one row per run to `dq_results`.

- **Critical:** the failing row goes to quarantine (ETL) or gets `dq_status = 'FAILED'` (ELT).
- **Warning:** the row still loads, but the check is logged as `WARN`.

In the code columns, `ts`, `qty`, `price`, `disc`, `amt`, `mins`, `paid`, `mrp` and `cost` are the parsed (numeric or date) versions of the text columns. `now` is the run time.

#### orders.csv

| check_name | Dimension | Severity | ETL (pandas): row fails when… | ELT (SQL): row fails when… |
|---|---|---|---|---|
| `orders_order_id_unique` | Uniqueness | Critical | `df.order_id.duplicated(keep="first")` | `ROW_NUMBER() OVER (PARTITION BY _source_file, order_id ORDER BY _source_row) > 1` |
| `orders_customer_id_not_null` | Completeness | Critical | `df.customer_id.isna()` | `customer_id IS NULL` |
| `orders_order_ts_not_null` | Completeness | Critical | `df.order_ts.isna()` | `order_ts IS NULL` |
| `orders_order_ts_valid` | Validity | Critical | `df.order_ts.notna() & ts.isna()` with `format="%Y-%m-%d %H:%M:%S", errors="coerce"` | `order_ts IS NOT NULL AND TRY_STRPTIME(order_ts, '%Y-%m-%d %H:%M:%S') IS NULL` |
| `orders_order_ts_not_future` | Timeliness | Critical | `ts > now` | `ts > getvariable('run_now')` |
| `orders_status_allowed` | Validity | Critical | `~df.order_status.str.strip().str.upper().isin(["DELIVERED", "CANCELLED", "RETURNED"])` | `UPPER(TRIM(order_status)) NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED')` |
| `orders_payment_mode_allowed` | Validity | Warning | Mode is not already one of the 4 allowed values (it gets mapped, then loads) | `payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet')` |
| `orders_quantity_positive` | Accuracy | Critical | `qty <= 0` | `TRY_CAST(quantity AS INTEGER) <= 0` |
| `orders_discount_range` | Validity | Critical | `~disc.between(0, 100)` | `TRY_CAST(discount_pct AS DOUBLE) NOT BETWEEN 0 AND 100` |
| `orders_amount_formula` | Accuracy | Critical | `(amt - (qty * price * (1 - disc / 100)).round(2)).abs() > 0.01` | `ABS(amt - ROUND(qty * price * (1 - disc / 100), 2)) > 0.01` |
| `orders_delivered_has_minutes` | Completeness | Critical | `(status == "DELIVERED") & mins.isna()` | `status = 'DELIVERED' AND delivery_minutes IS NULL` |
| `orders_delivery_minutes_max` | Accuracy | Critical | `mins >= 120` | `TRY_CAST(delivery_minutes AS DOUBLE) >= 120` |
| `orders_product_exists` | Consistency | Critical | `~df.product_id.isin(products.product_id)` | `product_id NOT IN (SELECT product_id FROM raw_products)` |
| `orders_store_exists` | Consistency | Critical | `~df.store_id.isin(stores.store_id)` | `store_id NOT IN (SELECT store_id FROM raw_stores)` |
| `orders_unit_price_le_mrp` | Accuracy (distribution) | Warning | `(mrp > 0) & (price > mrp + 0.01)`, using the product's MRP | `product_mrp > 0 AND price > product_mrp + 0.01` |
| `orders_freshness` | Timeliness | Warning | Whole file: newest non-future `ts < now - 1 day` | `MAX(order_ts) FILTER (WHERE order_ts <= run_now) < run_now - INTERVAL 1 DAY` |
| `orders_volume` | Completeness (volume) | Warning | Whole file: rows < 10% of the median of the last 5 loads of the same file (`etl_load_history`) | Same rule, using the per-run counts in `raw_orders` |

#### payments.csv

| check_name | Dimension | Severity | ETL (pandas): row fails when… | ELT (SQL): row fails when… |
|---|---|---|---|---|
| `payments_payment_id_unique` | Uniqueness | Critical | `df.payment_id.duplicated(keep="first")` | `ROW_NUMBER() OVER (PARTITION BY payment_id ORDER BY _loaded_at) > 1` |
| `payments_order_exists` | Consistency | Critical | `~df.order_id.isin(orders.order_id)` | `order_id NOT IN (SELECT order_id FROM raw_orders)` |
| `payments_amount_positive` | Accuracy | Critical | `paid <= 0` | `TRY_CAST(amount_paid AS DOUBLE) <= 0` |
| `payments_amount_matches_order` | Consistency | Warning | SUCCESS and `abs(paid - order_amount) > 0.01`; listed in `etl_payment_reconciliation` | Same condition; listed in `mart_payment_reconciliation` |
| `payments_mode_allowed` | Validity | Warning | Same mapping as orders | Same as orders |
| `payments_gateway_ref_present` | Completeness | Warning | `(df.payment_status == "SUCCESS") & df.gateway_ref.isna()` | `payment_status = 'SUCCESS' AND gateway_ref IS NULL` |

#### customers.csv

| check_name | Dimension | Severity | ETL (pandas): row fails when… | ELT (SQL): row fails when… |
|---|---|---|---|---|
| `customers_id_unique` | Uniqueness | Critical | Sort by `signup_date`, then `df.customer_id.duplicated(keep="last")` (keeps the newest copy) | `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY signup_date DESC) > 1` |
| `customers_email_format` | Validity | Warning | `~df.email.fillna("").str.match(r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$")` | `NOT regexp_matches(COALESCE(email, ''), '^[^@\s]+@[^@\s]+\.[a-z]{2,}$')` |
| `customers_phone_10_digits` | Validity | Warning | `~df.phone.fillna("").str.fullmatch(r"\d{10}")` | `NOT regexp_full_match(COALESCE(phone, ''), '\d{10}')` |
| `customers_pincode_6_digits` | Validity | Warning | `~df.pincode.fillna("").str.fullmatch(r"\d{6}")` | `NOT regexp_full_match(COALESCE(pincode, ''), '\d{6}')` |
| `customers_city_not_null` | Completeness | Warning | `df.city.isna()` | `city IS NULL` |
| `customers_signup_not_future` | Timeliness | Critical | `signup > now` | `TRY_STRPTIME(signup_date, '%Y-%m-%d') > getvariable('run_now')` |
| `customers_tier_casing` | Consistency | Warning | `~df.loyalty_tier.isin(["Bronze", "Silver", "Gold", "Platinum"])` (then fixed with `.str.title()`) | `loyalty_tier NOT IN ('Bronze', 'Silver', 'Gold', 'Platinum')` |

#### products.csv

| check_name | Dimension | Severity | ETL (pandas): row fails when… | ELT (SQL): row fails when… |
|---|---|---|---|---|
| `products_id_unique` | Uniqueness | Critical | `df.product_id.duplicated(keep="first")` | `ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY _loaded_at) > 1` |
| `products_category_not_null` | Completeness | Critical | `df.category.isna()` | `category IS NULL` |
| `products_mrp_positive` | Accuracy | Critical | `mrp <= 0` | `TRY_CAST(mrp AS DOUBLE) <= 0` |
| `products_cost_le_mrp` | Accuracy | Critical | `cost > mrp` | `TRY_CAST(cost_price AS DOUBLE) > TRY_CAST(mrp AS DOUBLE)` |

#### stores.csv

| check_name | Dimension | Severity | ETL (pandas): row fails when… | ELT (SQL): row fails when… |
|---|---|---|---|---|
| `stores_zone_not_null` | Completeness | Critical | `df.zone.isna()` | `zone IS NULL` |
| `stores_capacity_positive` | Accuracy | Warning | `cap <= 0` | `TRY_CAST(capacity_orders_per_day AS INTEGER) <= 0` |

#### Coverage: every dimension is checked

| Dimension | orders | payments | customers | products | stores |
|---|---|---|---|---|---|
| Completeness | ✅ | ✅ | ✅ | ✅ | ✅ |
| Uniqueness | ✅ | ✅ | ✅ | ✅ | — |
| Validity | ✅ | ✅ | ✅ | — | — |
| Accuracy | ✅ | ✅ | — | ✅ | ✅ |
| Consistency | ✅ | ✅ | ✅ | — | — |
| Timeliness | ✅ | — | ✅ | — | — |

**Totals:** 36 checks (25 Critical, 11 Warning).

I added `orders_unit_price_le_mrp` and `orders_volume` for observability Experiments 7 and 8. In clean data, `unit_price` always equals the product's MRP (all 2,196 matched orders have a ratio of exactly 1.0), and selling above MRP is illegal in India. So any price above MRP is a strong drift signal.

#### Design decisions

1. **Clean first, then check.** Text is trimmed, `delivered` is uppercased, and payment modes are mapped (`upi`/`UPI ` → UPI, `CARD` → Card, `cod`/`Cashh` → Cash on Delivery) *before* the allowed-value checks. Only values that can't be mapped, like `Pending??`, fail.
2. **Duplicates keep one copy.** The first copy loads (for customers, the newest by `signup_date`). Only the extra copies are quarantined, so a real order is never lost.
3. **Key checks use the full master file.** `orders_product_exists` checks the product IDs in `products.csv` as received, not just the clean ones. So if a product is quarantined (for example P0109, negative MRP), its real orders still count toward revenue. Only the product's own details are missing.
4. **Payment mismatches are reported, not rejected.** Finance needs to see them, so they go into the reconciliation table instead of quarantine.
5. **`orders_freshness` ignores future-dated rows,** otherwise one 2027 typo would make stale data look fresh. Even so, Experiment 9 showed that two typo-dated orders (`2026-10-21 10:15`) make the September export look fresh once that date has passed. See Part F.

#### dq_results columns

Each check writes one row per run:

| Column | Value |
|---|---|
| `run_id` | Airflow run ID |
| `dag_id` | ETL or ELT DAG ID |
| `check_name` | From the tables above |
| `dimension` | From the tables above |
| `severity` | `Critical` or `Warning` |
| `rows_checked` | Rows in the file |
| `rows_failed` | Rows that failed this check |
| `status` | `PASS` if `rows_failed = 0`; otherwise `FAIL` (Critical) or `WARN` (Warning) |
| `checked_at` | Timestamp of the check |

## Part E: Data governance

### 1. Ownership

| Dataset | Owner (accountable for its content) | Why |
|---|---|---|
| `orders.csv` | Head of Operations (Kavya, COO's team) | Created by the order management system that ops runs. |
| `payments.csv` | Finance Controller | Comes from the payment gateway; finance reconciles it. |
| `customers.csv` | CRM / Marketing lead, with the Data Protection Officer for PII | Holds personal data, so privacy sign-off is needed. |
| `products.csv` | Category / Merchandising manager | Sets MRP and cost price. |
| `stores.csv` | Head of Store Operations | Opens stores and assigns zones and capacity. |
| Warehouse tables (`etl_*`, `raw_*`, `stg_*`, `mart_*`, `dq_results`, `pipeline_runs`) | Data engineering (FDE team, then NammaMart's data owner) | Responsible for the pipeline, not the source content. |

**Changing a quality rule:** the business owner of the data approves it, and data engineering implements it. For example, the 120-minute delivery limit (`orders_delivery_minutes_max`) changes the "10-minute promise" KPI, so the COO approves it. The change goes through a pull request that edits [`nammamart_dq.py`](include/nammamart_dq.py) and [`stg_orders.sql`](include/sql/stg_orders.sql) together, so both pipelines stay identical.

### 2. Data dictionary

Types are as stored in DuckDB. The ELT tables `stg_orders`, `stg_customers`, `stg_products` and `stg_stores` have the same business columns, plus `dq_status`, `failure_reason`, `dq_warnings`, the `chk__*` flags and the lineage columns `_source_file` / `_source_row`.

**`etl_fact_orders`**: one row per order that passed every Critical check.

| Column | Type | Meaning | Source | Allowed values | PII |
|---|---|---|---|---|---|
| `order_id` | VARCHAR | Order key (unique) | orders.order_id | `ORD` + 6 digits | No |
| `order_ts` | TIMESTAMP | When the order was placed (IST) | orders.order_ts | Parsed `YYYY-MM-DD HH:MM:SS`, not in the future | No |
| `order_date` | DATE | Date part of `order_ts` | Derived | — | No |
| `customer_id` | VARCHAR | Customer key → `etl_dim_customer` | orders.customer_id | Not null | Pseudonymous |
| `store_id` | VARCHAR | Store key → `etl_dim_store` | orders.store_id | Exists in stores.csv | No |
| `product_id` | VARCHAR | Product key → `etl_dim_product` | orders.product_id | Exists in products.csv | No |
| `quantity` | BIGINT | Units ordered | orders.quantity | > 0 | No |
| `unit_price` | DOUBLE | Price per unit, ₹ | orders.unit_price | ≤ product MRP (warning) | No |
| `discount_pct` | BIGINT | Discount percentage | orders.discount_pct | 0–100 | No |
| `order_amount` | DOUBLE | Amount charged, ₹ | orders.order_amount | = quantity × unit_price × (1 − discount/100), ±₹0.01 | No |
| `payment_mode` | VARCHAR | How the customer paid | orders.payment_mode, standardised | UPI, Card, Cash on Delivery, Wallet | No |
| `order_status` | VARCHAR | Latest order status | orders.order_status, uppercased; updated by increments | DELIVERED, CANCELLED, RETURNED | No |
| `channel` | VARCHAR | Where the order was placed | orders.channel | App, Web | No |
| `delivery_minutes` | DOUBLE | Minutes from order to delivery | orders.delivery_minutes | < 120; required when DELIVERED | No |
| `net_revenue` | DOUBLE | Revenue counted for reporting, ₹ | Derived: `order_amount` if DELIVERED, else 0 | ≥ 0 | No |
| `gross_margin` | DOUBLE | `net_revenue − quantity × cost_price`, ₹ | Derived, joined to products | Empty if the product failed its checks | No |
| `within_15_min` | BOOLEAN | Delivered within 15 minutes | Derived from `delivery_minutes` | true/false; empty if not delivered | No |
| `_run_id` | VARCHAR | Airflow run that loaded the row | Pipeline | — | No |
| `_loaded_at` | TIMESTAMPTZ | When the row was loaded (UTC) | Pipeline | — | No |

**`etl_dim_customer`**: one row per customer (newest copy kept).

| Column | Type | Meaning | Source | Allowed values | PII |
|---|---|---|---|---|---|
| `customer_id` | VARCHAR | Customer key (unique) | customers.customer_id | `C` + 5 digits | Pseudonymous |
| `email` | VARCHAR | **Masked** email | customers.email | `xx***@domain` or `***` | Yes (masked) |
| `phone` | VARCHAR | **Masked** phone | customers.phone | `98******21` | Yes (masked) |
| `city` | VARCHAR | City | customers.city | Bengaluru (may be empty: warning) | Indirect |
| `area` | VARCHAR | Neighbourhood | customers.area | — | Indirect |
| `pincode` | VARCHAR | Postal code | customers.pincode | 6 digits (warning) | Indirect |
| `signup_date` | TIMESTAMP | Date the customer signed up | customers.signup_date | Not in the future | No |
| `loyalty_tier` | VARCHAR | Loyalty tier | customers.loyalty_tier, title-cased | Bronze, Silver, Gold, Platinum | No |
| `is_active` | BOOLEAN | Account is active | customers.is_active (`Y`/`N`) | true/false | No |

`full_name` is deliberately not stored.

**`etl_dim_product`**: one row per product.

| Column | Type | Meaning | Source | Allowed values | PII |
|---|---|---|---|---|---|
| `product_id` | VARCHAR | Product key (unique) | products.product_id | `P` + 4 digits | No |
| `product_name`, `brand`, `unit` | VARCHAR | Descriptive attributes | products.* | — | No |
| `category` | VARCHAR | Category (6 values) | products.category | Not null | No |
| `sub_category` | VARCHAR | Sub-category | products.sub_category | — | No |
| `mrp` | DOUBLE | Maximum retail price, ₹ | products.mrp | > 0 | No |
| `cost_price` | DOUBLE | NammaMart's cost, ₹ | products.cost_price | ≤ mrp | No |
| `is_perishable` | BOOLEAN | Perishable item | products.is_perishable | true/false | No |
| `launch_date` | TIMESTAMP | Launch date | products.launch_date | — | No |

**`etl_dim_store`**: one row per dark store.

| Column | Type | Meaning | Source | Allowed values | PII |
|---|---|---|---|---|---|
| `store_id` | VARCHAR | Store key (unique) | stores.store_id | `S01`–`S15` | No |
| `store_name`, `area`, `pincode` | VARCHAR | Location details | stores.* | — | No |
| `zone` | VARCHAR | Sales zone | stores.zone | North, South, East, West (not null) | No |
| `open_date` | TIMESTAMP | Opening date | stores.open_date | — | No |
| `manager_name` | VARCHAR | Store manager | stores.manager_name | — | **Yes** (employee) |
| `capacity_orders_per_day` | BIGINT | Daily order capacity | stores.capacity_orders_per_day | > 0 (warning) | No |

### 3. Lineage

**The number:** `mart_daily_store_kpis` says store **S01 (Hebbal) earned ₹4,583.05 on 15 Sep 2026** from 6 delivered orders.

**Tracing it in ELT** takes three queries and gives the exact CSV line:

```sql
-- 1. The mart row
SELECT order_date, store_id, delivered_orders, net_revenue FROM mart_daily_store_kpis
WHERE order_date = DATE '2026-09-15' AND store_id = 'S01';                 -- 6 orders, ₹4,583.05

-- 2. The staging rows behind it, with where each one came from
SELECT order_id, order_status, net_revenue, _source_file, _source_row, _run_id
FROM stg_orders WHERE order_date = DATE '2026-09-15' AND store_id = 'S01' AND dq_status = 'PASSED';

-- 3. Any row exactly as received
SELECT * FROM raw_orders WHERE _run_id = 'final_full' AND _source_row = 1723;
```

| order_id | status | net_revenue | _source_file | _source_row (CSV line) |
|---|---|---|---|---|
| ORD001127 | DELIVERED | 597.55 | orders.csv | 88 (line 89) |
| ORD001105 | DELIVERED | 360.00 | orders.csv | 390 (line 391) |
| ORD001130 | DELIVERED | 447.30 | orders.csv | 750 (line 751) |
| ORD001144 | DELIVERED | 354.00 | orders.csv | 1498 (line 1499) |
| ORD001133 | DELIVERED | 2,325.60 | orders.csv | 1723 (line 1724) |
| ORD001132 | DELIVERED | 498.60 | orders.csv | 2191 (line 2192) |
| ORD001109 | RETURNED | 0.00 | orders.csv | 2358 (line 2359) |

The 6 delivered amounts add up to ₹4,583.05. Each row tells me the file, the line (`_source_row` + 1 for the header) and the Airflow run that loaded it.

**In ETL** I can only get part of the way. `etl_fact_orders` has `order_id` and `_run_id`, so I can list the 6 orders. But the fact table doesn't record which file the row came from (`orders.csv` or an increment) or which line. I have to search the CSV for each `order_id` by hand. If a row was overwritten by an increment, the old version is gone from the warehouse.

**ELT is easier**, because raw keeps every row as received, staging keeps `_source_file` / `_source_row` / `_run_id` on every row, and the marts are plain SQL over staging. Lineage is just a few joins. The ETL throws that information away in pandas before loading.

### 4. Personal data

**Personal data in the dataset:**

| File | Column | Personal data? | How my pipeline handles it |
|---|---|---|---|
| customers.csv | `full_name` | Yes, direct identifier | **Dropped** in staging and the dimension; no report needs it. `***` in quarantine. |
| customers.csv | `email` | Yes, direct identifier | **Masked**: `priya.rao@example.com` → `pr***@example.com`. Malformed emails → `***`. |
| customers.csv | `phone` | Yes, direct identifier | **Masked**: `9876543221` → `98******21`. |
| customers.csv | `area`, `pincode`, `city` | Yes, indirectly (location narrows down a person) | Kept, since zone and area analytics need them; restricted to analysts. |
| customers.csv | `customer_id`, `signup_date`, `loyalty_tier` | Pseudonymous (personal data once linked to the CRM) | Kept; it's the join key. |
| stores.csv | `manager_name` | Yes: an employee's name | Kept in `etl_dim_store` / `stg_stores`, since ops needs it; not shown in any mart. |
| orders.csv | `customer_id` + `order_ts` + `area` | Yes, combined (buying habits of a person) | Only aggregated in marts, except `mart_top_customers`, which shows masked email and ID only. |

**Why mask instead of delete:** `customer_id` must stay to join orders to customers (top customers, loyalty tier). Partial email and phone let support staff confirm "is this the right customer?" without exposing the full value. Deleting the columns would also remove the ability to spot duplicate accounts (the same masked email twice). The full values are still available, under tighter control, in the source CRM.

**Law:** India's **Digital Personal Data Protection Act, 2023 (DPDP Act)**. NammaMart is a *data fiduciary* and must:
- Process customer data only for the purpose the customer consented to (purpose limitation).
- Collect and keep only what's needed (data minimisation), which is why `full_name` is dropped.
- Delete data once the purpose is served (storage limitation).
- Protect it with reasonable security safeguards (masking, access control).
- Report breaches to the Data Protection Board and to the affected customers.
- Honour customers' rights to access, correct and erase their data.

Penalties go up to ₹250 crore for failing to prevent a breach.

### 5. Access control

| Layer | Who can read it | Why |
|---|---|---|
| Raw (`raw_*`, source CSVs) | Data engineering only (2–3 named people) | Holds unmasked PII and is the legal audit copy. |
| Staging (`stg_*`, `etl_dim_*`) | Data engineering and analysts | Masked, row-level detail, needed to debug and answer new questions. |
| Mart (`mart_*`, `etl_*` reports) | Finance, ops, store managers, leadership | Aggregated and masked; the business-facing layer. |
| Quarantine files, `dq_results` | Data engineering and the dataset's owner | To fix bad source data. My quarantine files are masked too. |

**Unmasked phone numbers in `raw_customers`:** nobody should read them in the warehouse during normal work. Support staff who need to call a customer use the CRM, which has its own access controls and audit log. Only the data engineering owner can open the raw layer, for audit or to fix a bug, and that access should be logged.

**Gap in my setup:** DuckDB is a single file with no users or roles, so anyone who can open the file can read every table, raw included. In production, I would move the warehouse to PostgreSQL (or a cloud warehouse) and give each layer its own schema and role (`GRANT SELECT ON SCHEMA mart TO finance`). Until then, I excluded the file from git (see `.gitignore`).

### 6. Retention

| Data | Keep for | Why | After that |
|---|---|---|---|
| Raw orders and payments | 8 years | Financial records: the Companies Act, 2013 requires books of account to be kept for 8 years. | Archive to cold storage, then delete. |
| Raw customers (unmasked PII) | While the customer is active + 1 year | DPDP storage limitation: delete once the purpose is served. | Delete the row, or replace the PII columns with `NULL` and keep the `customer_id` so old orders still join. |
| Masked customer data (`stg_customers`, `etl_dim_customer`) | Same as raw customers | It's still personal data (pseudonymous). | Rebuilt from raw, so it disappears automatically after the raw purge. |
| Quarantine files | 90 days | Long enough for the data owner to fix the source; after that they're stale. | Automatic delete job. |
| `dq_results`, `pipeline_runs` | 2 years | Useful as a quality trend. No PII inside. | Delete or summarise to monthly. |

Today nothing is deleted automatically. A weekly retention DAG that runs these deletes would be the next step.

### 7. Secrets

**Where they live:** my two DAGs need no credentials: the data is local CSVs, and the warehouse is a local DuckDB file. Any secret, such as `SLACK_WEBHOOK_URL` for alerts, goes in an **Airflow Variable** (or a Connection), read at run time inside a task:

```python
from airflow.sdk import Variable
webhook = Variable.get("SLACK_WEBHOOK_URL", default=None)
```

The value is set in the Airflow UI (Admin → Variables) or in `airflow_settings.yaml` / `.env` for local development. Both of those files are listed in `.gitignore`, so they never reach GitHub. Airflow stores Variables in its metadata database and hides values whose names contain words like `secret`, `password` or `token` in the UI. In production they would come from a secrets backend such as AWS Secrets Manager or HashiCorp Vault.

**Why never in the DAG file or on GitHub:**
- The `dags/` folder is in the git repo, so anyone with repo access, every fork, and the whole git history would hold the secret forever. Deleting it later doesn't remove it from history.
- Bots scan public GitHub for webhook URLs and keys within minutes. A leaked Slack webhook lets anyone post fake "pipeline OK" messages to finance.
- Rotating the secret would mean a code change and a redeploy, instead of one edit in the UI.

### 8. Audit

**The rejected order:** `ORD000469`, quantity 0 on a DELIVERED order.

**ELT**: three queries, all inside the warehouse:

```sql
-- As received (raw keeps every load; this order has been loaded 9 times, all identical)
SELECT * FROM raw_orders WHERE order_id = 'ORD000469' ORDER BY _loaded_at LIMIT 1;
-- order_ts 2026-09-07 11:52:46 | C00076 | S02 | P0041 | quantity 0 | unit_price 444.0 | order_amount 0.0 | DELIVERED | orders.csv row 126

-- The rule it broke
SELECT order_id, dq_status, failure_reason FROM stg_orders WHERE order_id = 'ORD000469';
-- FAILED | orders_quantity_positive

-- Proof it's not in the CFO's revenue: marts only read PASSED rows
SELECT COUNT(*) FROM stg_orders WHERE order_id = 'ORD000469' AND dq_status = 'PASSED';   -- 0
```

**ETL**: the evidence is spread across three places:

1. *As received:* the source file `include/data/nammamart/orders.csv`, line 127. The warehouse doesn't keep raw rows.
2. *Rule broken:* the quarantine file `include/quarantine/rahul/orders_failed_final2_full.csv` has the row with `failure_reason = orders_quantity_positive`.
3. *Proof:* `SELECT COUNT(*) FROM etl_fact_orders WHERE order_id = 'ORD000469'` returns **0**. The fact table is the only source of `etl_daily_zone_revenue`.

Both pipelines can prove it, but **ELT does it in one place with SQL**, and its raw copy can't be edited after the fact. The ETL's "as received" evidence is a CSV on disk that someone could change or delete, and the quarantine file is a separate file per run.

### 9. Change history

**Example from the data:** `ORD000271` was DELIVERED on **4 Sep** (₹445) and is RETURNED in the 1 Oct increment. (No corrected order is dated exactly 3 Sep; the 10 corrections are dated 1–25 Sep.)

**What each pipeline shows for September after the increment:**

| | ETL | ELT |
|---|---|---|
| ORD000271 | RETURNED, net_revenue 0 | RETURNED, net_revenue 0 |
| September net revenue | ₹11,31,435.10 | ₹11,31,435.10 |
| Before the increment | ₹11,35,628.60 | ₹11,35,628.60 |

Both pipelines **restate September**, lowering it by ₹4,193.70 (the 10 returns). The old DELIVERED version is gone from `etl_fact_orders`; the ETL simply overwrote it.

I found a worse problem in testing. If the ETL re-runs `orders.csv` after the increment, the 10 orders go back to DELIVERED and September goes back up. During my experiments the ETL total was ₹4,193.70 higher than the ELT for exactly this reason. The ELT doesn't do this, because it re-applies the MERGE after every staging rebuild.

**What finance wants:** books for September are closed. Finance needs September **as reported** (₹11,35,628.60) to stay fixed, and the return booked as a **negative ₹445 in October**, the month it happened. Ops may still want the restated view, "which September orders were eventually returned?"

**Keeping both views:**
- **ELT already has the data.** `raw_orders` keeps both versions of ORD000271: the `orders.csv` load (DELIVERED) and the increment load (RETURNED), each with `_loaded_at`. "As reported" is a query over the latest `orders.csv` load:
  ```sql
  SELECT SUM(net_revenue) FROM v_orders_checked
  WHERE _source_file = 'orders.csv' AND dq_status = 'PASSED' AND order_date < DATE '2026-10-01';   -- ₹11,35,628.60
  ```
  "Restated" is `stg_orders` (₹11,31,435.10).
- **The proper fix** is an order status history table (SCD Type 2): one row per order per status, with `valid_from` / `valid_to`. Plus a returns fact that books each return as a negative amount on its return date. Then September's report stays fixed, October shows −₹4,193.70, and any past date can be queried "as of".
- **ETL can't do this** without new tables, because it keeps only the latest version of each order.

## Part F: Observability tests

**Run audit:** both DAGs end with an `audit` task (`trigger_rule="all_done"`, `retries=0`) that writes one row per run to `pipeline_runs`: `run_id`, `dag_id`, `start_time`, `end_time`, `rows_extracted`, `rows_loaded`, `rows_quarantined` and `status`.

**How I ran the experiments:** each experiment triggered **both DAGs at the same time** through the scheduler, with a readable run ID (`exp01_happy` … `exp09_freshness`), so every run is in the Airflow UI history. Test files were made from a backup of the originals, and every source file was restored and checksum-verified after each experiment. Run date: 22 Oct 2026, around 07:40 IST.

| # | Experiment | Expected | Observed (ETL / ELT) | Where I saw it | What I would change |
|---|---|---|---|---|---|
| 1 | Happy path | Both green; 5 extracts in parallel | Both green. ETL 11.3 s, ELT 7.5 s. All 5 `extract` tasks started within 0.03 s of each other in both DAGs. Same counts in both: orders 2,412 in / 2,312 passed / 100 quarantined, payments 2,228 / 2,206 / 22, customers 405 / 397 / 8, products 152 / 142 / 10, stores 15 / 14 / 1. ETL `validate` took 5.4 s because it **waited for the ELT's lock** on the shared DuckDB file. | Grid view (start times); `validate` log: `[Warehouse] locked by another run, retry 1/60 in 5s`; `pipeline_runs` | Stagger the schedules (ETL 9:00, ELT 9:02) or give each pipeline its own warehouse file, so neither waits. |
| 2 | Re-run (idempotency) | Fact counts unchanged; ELT raw grows | `etl_fact_orders` 2,312 → 2,312; `stg_orders` 2,412 → 2,412; revenue unchanged. `raw_orders` 7,236 → 9,648 and `raw_payments` 6,684 → 8,912: **raw grew by one full copy. That's correct:** raw is an append-only record of every load, and staging only reads the latest load of each file. | Row-count query before/after; `load` log `0 inserted, 2312 replaced` | Skip loading a file into raw if the same file (same checksum) was already loaded, so raw doesn't grow on every re-run. |
| 3 | Incremental load | 150 inserted, 10 updated, no duplicates | ETL: `2312 → 2462 rows (150 inserted, 10 replaced/updated)`. ELT MERGE: `2412 → 2562 rows (150 inserted, 10 updated)`. All 10 corrected orders are RETURNED in both, 1 row each. | `load` / `merge_increments` logs; query on the 10 IDs | ETL: a later full re-run of `orders.csv` **reverts the 10 returns** (observed: after Exp 5 the ETL total was ₹4,193.70 above ELT). Needs an order-history table, like the ELT's raw layer. |
| 4 | Missing source file | Extract fails after retries; nothing loaded | Both: `extract[products]` failed with `FileNotFoundError: … products.csv`. 3 tries about a minute apart (02:11, 02:12, 02:13 UTC; 1-minute retry delay), then `failed`. The other 4 extracts succeeded; every downstream task `upstream_failed`. `audit` still ran and wrote `FAILED`, `rows_extracted` 5,060, `rows_loaded` 0. **Nothing alerted me**: the run just turned red in the UI. **Bug found:** `audit` raises on purpose to keep the run red, so it was retried twice and wrote **3 duplicate rows**. | Grid view (orange `up_for_retry`, then red); task log; `pipeline_runs` | Fixed: `audit` now has `retries=0`. Add an `on_failure_callback` that posts to Slack, with the webhook in an Airflow Variable. |
| 5 | New bad data | Each of the 5 rows quarantined with the right reason | Both pipelines, identical: ORD900001 → `orders_customer_id_not_null`, ORD900002 → `orders_quantity_positive`, ORD900003 → `orders_store_exists`, ORD900004 (`SHIPPED`) → `orders_status_allowed`, ORD900005 (2027) → `orders_order_ts_not_future`. `dq_results` counted +1 on each check (18→19, 10→11, 5→6, 3→4, 5→6). 0 of them reached `etl_fact_orders` or a PASSED staging row. Runs stayed green. | `orders_failed_exp05_bad_rows.csv`; `stg_orders.failure_reason`; `dq_results` | Alert when a Critical check fails more rows than usual (e.g. above the average of the last 5 runs), not only on a crash. |
| 6 | Schema change (`order_amount` → `total`) | Fail loudly, load nothing | **ETL:** extract succeeded (it reads any columns). `validate` failed with `AttributeError: 'DataFrame' object has no attribute 'order_amount'` after 3 tries (~2 min): loud, but the message is confusing. **ELT:** `extract[orders]` failed with `ValueError: Schema change in orders.csv: missing ['order_amount'], unexpected ['total']`, before anything was loaded. Neither loaded wrong data. | Grid view; task logs | Add the same header check to the ETL `extract`. Raise `AirflowFailException` for schema errors so they fail at once instead of retrying a problem that can't fix itself. |
| 7 | Volume anomaly (50 orders) | Something warns about a >90% drop | Both: new check `orders_volume` = **WARN** (50 < 10% of the median 2,412). **Both runs stayed green.** ETL: the fact table kept its old rows, so the **report didn't change at all**: the drop was invisible. ELT: staging was rebuilt from the 50-row file, so **mart revenue fell from ₹12,12,267 to ₹1,03,134 (−91%)**. Side effect: ~2,150 payments failed `payments_order_exists` (their orders were missing), so `rows_quarantined` jumped to 2,192 / 2,201. | `dq_results`; `pipeline_runs`; mart totals | Make `orders_volume` **Critical**: stop the run before the marts are rebuilt, and alert. A 91% revenue drop must never reach the 9:30 stand-up. |
| 8 | Distribution drift (`unit_price` × 100 for S08) | A check catches it; store metric moves | Both: `orders_amount_formula` FAIL on 194 rows (18 normal + all 176 S08 rows), and the new `orders_unit_price_le_mrp` WARN on 175. **ETL:** S08 revenue stayed at ₹79,130.50: the fact still held the previous good S08 rows, so the report was silently stale. **ELT:** S08 revenue fell **₹78,980.50 → ₹6,569.90** (only its 1-Oct increment orders left), and East zone fell ₹3,14,056.20 → ₹2,41,645.60 (−23%). | `dq_results`; `mart_daily_store_kpis` vs `etl_fact_orders` | Fixed the ETL stale rows: a full-file load now also deletes orders that fail their checks (found the same bug in Exp 9). Add a per-store revenue check against its own 7-day average. |
| 9 | Freshness (latest order 30 days old) | Timeliness check warns | Both: `orders_freshness` = **WARN**; runs green. Surprise: on the **normal** data it said **PASS**. ORD001089 and ORD001693 are dated `2026-10-21 10:15` inside a September export. On 21 Oct they were "future" and quarantined. From 22 Oct they pass every check, count as October revenue and make the data look fresh. **Bug found:** the shift made 2 future-dated orders valid; when the original file came back they failed again, but the ETL **kept them in the fact table** (2,464 vs ELT 2,462, +₹637.40). | `dq_results`; staging query; ETL vs ELT row counts | Fixed the stale rows (see 8). Add a rule that `order_ts` must fall inside the export's period. Measure freshness from the file's arrival time, not the newest timestamp in it. |
| 10 | Recovery (Clear after Exp 4) | Only the failed task and its downstream re-run | **ETL:** restored `products.csv`, ran `airflow tasks clear -f -d` (only failed + downstream). Only `extract[products]` (try 4) and the tasks after it re-ran; the other 4 extracts stayed at try 1. Run → success, and `audit` added a SUCCESS row. **ELT:** cleared from the UI on the whole `extract` task, so **all 5** extracts re-ran (try 2). Also succeeded. | Grid view try numbers; `task_instance` table; `pipeline_runs` | Write a runbook: fix the cause, then **Clear → "Only failed"** on the failed task, not on the whole task or the whole run. |

After the experiments I ran both DAGs on the original files (full, then increment). The ETL and ELT match exactly: **2,462 orders, ₹12,12,267.10 total, ₹11,31,435.10 for September.**

### Five pillars of observability

| Pillar | How my pipeline covers it | Strength |
|---|---|---|
| Freshness | `orders_freshness` (newest valid order < 1 day old) in `dq_results`; `pipeline_runs.end_time` shows when data last landed. | **Weakest.** Two typo-dated orders fooled it (Exp 9); it's only a Warning, and nobody is alerted if the 9:00 run never happens (Part C). |
| Volume | `orders_volume` (< 10% of the usual rows); `rows_extracted` / `rows_loaded` / `rows_quarantined` per run in `pipeline_runs`. | Medium. Caught the 50-row file, but only as a Warning, so a −91% report still went out (Exp 7). Only `orders` is checked. |
| Schema | ELT `extract` compares each header with the expected columns and fails before loading; ETL fails later on a missing column. | **Best (ELT).** Loud, early and with a clear message (Exp 6). The ETL is loud but unclear. |
| Distribution | `orders_amount_formula`, `orders_unit_price_le_mrp`, delivery-time and discount ranges. | Medium. Caught the ×100 price drift (Exp 8), but there's no statistical check per store or day. |
| Lineage | ELT: `_source_file`, `_source_row` and `_run_id` on every raw and staging row, traceable to the exact CSV line (Part E.3). ETL: `_run_id` and `order_id` only. | Strong in ELT, weak in ETL. |

**Overall:** schema is the pillar my pipelines cover best, and freshness the weakest. The biggest gap across all pillars is **alerting**. Every warning lands in `dq_results`, but a person has to go and look. The first thing I would add is a Slack alert for any FAIL, any WARN on volume, freshness or distribution, and any failed run.

## Business questions

Results are from the final runs: September plus the 1 October increment. **Every answer is identical in the ETL and ELT pipelines**, so each table below is both pipelines' result. Store S07 shows as `Unknown` because its zone is empty in `stores.csv`, so the store failed `stores_zone_not_null`. Its orders still count.

### 1. Daily net revenue by zone and by store

```sql
-- ETL
SELECT order_date, zone, delivered_orders, net_revenue FROM etl_daily_zone_revenue ORDER BY order_date, zone;
SELECT f.order_date, f.store_id, s.store_name, SUM(f.net_revenue) AS net_revenue
FROM etl_fact_orders f LEFT JOIN etl_dim_store s USING (store_id)
WHERE f.order_status = 'DELIVERED' GROUP BY ALL ORDER BY 1, 2;

-- ELT (one table answers both)
SELECT order_date, zone, store_id, store_name, delivered_orders, net_revenue
FROM mart_daily_store_kpis ORDER BY order_date, zone, store_id;
```

The daily tables have 450 rows (30 days × 15 stores), so here are the totals by zone, plus one example day:

| Zone | Delivered orders | Net revenue (₹) |
|---|---|---|
| East | 611 | 3,14,056.20 |
| West | 592 | 3,12,413.00 |
| South | 433 | 2,56,214.55 |
| North | 443 | 2,44,008.40 |
| Unknown (S07) | 151 | 85,574.95 |
| **Total** | **2,230** | **12,12,267.10** |

Top stores: S06 Banashankari ₹94,256.85, S13 Malleshwaram ₹89,650.30, S05 JP Nagar ₹86,163.65, S07 BTM Layout ₹85,574.95, S01 Hebbal ₹85,510.70.

Example day, 15 Sep 2026: S01 ₹4,583.05 (6 orders), S09 ₹3,549.00 (6), S11 ₹2,470.80 (6), S02 ₹1,630.30 (3), S10 ₹1,578.30 (4), S08 ₹984.50 (3).

### 2. Revenue and gross margin by product category

```sql
SELECT * FROM etl_category_margin;      -- ETL
SELECT * FROM mart_category_margin;     -- ELT
```

| Category | Delivered orders | Net revenue (₹) | Gross margin (₹) | Margin % |
|---|---|---|---|---|
| Staples | 302 | 2,68,583.30 | 67,143.14 | 25.0 |
| Household | 363 | 2,12,105.05 | 54,466.15 | 25.7 |
| Personal Care | 323 | 2,10,889.10 | 47,054.58 | 22.3 |
| Dairy & Bakery | 406 | 1,80,555.80 | 47,746.13 | 26.4 |
| Snacks & Beverages | 342 | 1,75,181.55 | 41,484.71 | 23.7 |
| Fruits & Vegetables | 374 | 93,604.20 | 22,205.38 | 23.7 |
| Unknown | 120 | 71,348.10 | — | — |

"Unknown" is orders whose product failed its checks (empty category, negative MRP or cost above MRP), so their margin can't be trusted. Their revenue still counts.

### 3. Average delivery time per store, and % delivered within 15 minutes

```sql
-- ETL
SELECT store_id, ROUND(AVG(delivery_minutes), 1) AS avg_min,
       ROUND(100.0 * COUNT(*) FILTER (WHERE within_15_min) / COUNT(*), 1) AS pct_within_15
FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1 ORDER BY 3 DESC;
-- ELT
SELECT * FROM mart_delivery_sla ORDER BY pct_within_15_min DESC;
```

| Store | Avg minutes | % within 15 min | | Store | Avg minutes | % within 15 min |
|---|---|---|---|---|---|---|
| S08 Whitefield | 20.6 | 35.4 | | S04 Jayanagar | 21.3 | 27.5 |
| S07 BTM Layout | 21.1 | 32.5 | | S13 Malleshwaram | 21.9 | 27.3 |
| S01 Hebbal | 20.5 | 32.2 | | S09 Indiranagar | 21.1 | 26.5 |
| S02 Yelahanka | 20.9 | 31.3 | | S10 Marathahalli | 22.1 | 26.0 |
| S14 Vijayanagar | 21.5 | 31.3 | | S12 Rajajinagar | 21.8 | 24.4 |
| S05 JP Nagar | 21.2 | 29.6 | | S15 Basaveshwaranagar | 22.0 | 22.9 |
| S03 RT Nagar | 20.6 | 29.2 | | S06 Banashankari | 22.5 | 21.6 |
| S11 HSR Layout | 20.8 | 28.6 | | **All stores** | **21.3** | **28.5** |

On average a delivery takes 21 minutes, and **only 28.5% of orders arrive within 15 minutes**, far from the "10-minute promise". This is the number Kavya most needs to see.

### 4. Revenue by payment mode, and the amount that doesn't reconcile

```sql
-- ETL
SELECT payment_mode, COUNT(*) AS delivered_orders, SUM(net_revenue) AS net_revenue
FROM etl_fact_orders WHERE order_status = 'DELIVERED' GROUP BY 1;
SELECT reason, COUNT(*), SUM(ABS(COALESCE(difference, amount_paid))) FROM etl_payment_reconciliation GROUP BY 1;
-- ELT (one table)
SELECT * FROM mart_payment_mode_revenue;
```

| Payment mode | Delivered orders | Net revenue (₹) | Mismatches | Unreconciled (₹) |
|---|---|---|---|---|
| UPI | 1,207 | 6,39,931.95 | 123 | 80,995.73 |
| Card | 457 | 2,51,882.30 | 81 | 41,303.50 |
| Cash on Delivery | 320 | 1,82,334.40 | 63 | 57,533.55 |
| Wallet | 246 | 1,38,118.45 | 43 | 21,984.15 |

| Reconciliation reason | Rows | Amount (₹) |
|---|---|---|
| `DELIVERED_NOT_PAID`: delivered, no successful payment | 220 (70 in Sep, 150 Oct orders with no payment file yet) | 1,22,095.75 |
| `ORDER_NOT_IN_FACT`: payment for an order that was rejected | 77 | 79,036.18 |
| `AMOUNT_MISMATCH`: paid amount ≠ order amount | 13 | 685.00 |
| **Total that doesn't reconcile** | **310** | **₹2,01,816.93** |

Of the 70 September orders, **42 were delivered although the payment FAILED**, and 28 have no payment record at all.

### 5. Top 10 customers by spend, with loyalty tier

```sql
-- ETL
SELECT f.customer_id, c.loyalty_tier, c.email, COUNT(*) AS orders, SUM(f.net_revenue) AS total_spend
FROM etl_fact_orders f LEFT JOIN etl_dim_customer c USING (customer_id)
WHERE f.order_status = 'DELIVERED' GROUP BY ALL ORDER BY total_spend DESC LIMIT 10;
-- ELT
SELECT * FROM mart_top_customers;
```

| Rank | Customer | Tier | Email (masked) | Orders | Spend (₹) |
|---|---|---|---|---|---|
| 1 | C00136 | Bronze | sa***@example.com | 9 | 8,738.10 |
| 2 | C00115 | Bronze | sa***@example.com | 8 | 8,037.70 |
| 3 | C00283 | Bronze | ni***@example.com | 8 | 7,934.60 |
| 4 | C00236 | Platinum | sn***@example.com | 12 | 7,831.80 |
| 5 | C00310 | Bronze | ki***@example | 8 | 7,258.95 |
| 6 | C00265 | Gold | vi***@example.com | 8 | 7,151.30 |
| 7 | C00071 | Silver | ma***@example.com | 11 | 6,933.30 |
| 8 | C00338 | Silver | bh***@example.com | 9 | 6,848.55 |
| 9 | C00357 | Bronze | ar***@example.com | 11 | 6,637.90 |
| 10 | C00209 | Platinum | ka***@example.com | 8 | 6,519.40 |

No raw email or phone appears. Insight: **4 of the top 5 spenders are only Bronze**, so the loyalty tiers don't reflect spend.

### 6. Rows rejected today, by file and by reason

```sql
-- ETL: from the quality scorecard for the latest run
SELECT split_part(check_name, '_', 1) || '.csv' AS file, check_name AS reason, rows_failed
FROM dq_results
WHERE run_id = (SELECT run_id FROM pipeline_runs WHERE dag_id = 'nammamart_etl_rahul' ORDER BY end_time DESC LIMIT 1)
  AND dag_id = 'nammamart_etl_rahul' AND severity = 'Critical' AND rows_failed > 0;
-- ELT
SELECT file, reason, rows_rejected FROM mart_rejected_rows;
```

Result for the full September run (identical in both pipelines). A row that broke two rules counts under both reasons, so these add up to more than the 141 rows rejected:

| File | Reason | Rows |
|---|---|---|
| orders.csv | orders_customer_id_not_null | 18 |
| orders.csv | orders_amount_formula | 18 |
| orders.csv | orders_order_id_unique | 12 |
| orders.csv | orders_quantity_positive | 10 |
| orders.csv | orders_product_exists | 9 |
| orders.csv | orders_delivered_has_minutes | 8 |
| orders.csv | orders_delivery_minutes_max | 6 |
| orders.csv | orders_order_ts_valid | 5 |
| orders.csv | orders_order_ts_not_future | 5 |
| orders.csv | orders_store_exists | 5 |
| orders.csv | orders_status_allowed | 3 |
| orders.csv | orders_discount_range | 3 |
| orders.csv | orders_order_ts_not_null | 1 |
| payments.csv | payments_amount_positive | 10 |
| payments.csv | payments_order_exists | 8 |
| payments.csv | payments_payment_id_unique | 4 |
| customers.csv | customers_id_unique | 5 |
| customers.csv | customers_signup_not_future | 3 |
| products.csv | products_cost_le_mrp | 5 |
| products.csv | products_category_not_null | 3 |
| products.csv | products_id_unique | 2 |
| products.csv | products_mrp_positive | 1 |
| stores.csv | stores_zone_not_null | 1 |

On the increment run, the 160 increment orders were all clean, so `mart_rejected_rows` shows only the master files for that run.

## Part G: Recommendation to the CTO

**To:** Sameer, CTO, NammaMart Quick Commerce
**From:** Rahul, FDE team
**Re:** ETL or ELT for the 9:00 AM operations report

**Recommendation: adopt ELT, with an ETL-style quality gate in front of the reports.**

I ran both pipelines side by side on the same September data, the 1 October file and 10 deliberate failure tests. Both produce identical numbers (₹11,31,435.10 net revenue for September after returns), and both keep every bad row out of the reporting tables. The difference is what happens around those numbers.

**Auditability: ELT clearly wins.** ELT keeps every row exactly as received (`raw_*`), and every reported number traces to a file, line and run in three SQL queries. When finance asks "show me rejected order ORD000469 as received, the rule it broke, and prove it's not in revenue", ELT answers in one place. ETL needs the source CSV, a quarantine file and a warehouse query, and the "as received" copy is a file anyone could edit.

**Data quality guarantee: equal, by design.** Finance's requirement, "no bad row on the CFO's dashboard", holds in both. In ELT the marts read only `dq_status = 'PASSED'` rows, the same gate the ETL applies before loading. Both use the same 36 checks, and all 5 bad rows I injected were rejected for the right reason by both pipelines.

**Correctness over time: ELT was right in situations where ETL was wrong.** In testing, the ETL:
- **Reverted the 10 returns** when `orders.csv` was re-run after the 1 Oct file, putting September back up by ₹4,193.70.
- **Kept 2 orders that later failed their checks**, worth ₹637.40, until I fixed it.
- **Kept showing yesterday's figures for store S08** after its prices were corrupted.

ELT rebuilds staging from raw every run, so it can't drift like this. The ETL needs extra code for each of these cases, and I fixed one of them.

**Ease of change: ELT.** Every transformation is a short SQL file in `include/sql/`. A new question means a new mart, and because raw is kept, it can be answered for past data immediately. In the ETL, a new rule or column means changing Python and re-sending files, and old raw data is gone.

**Performance at 1000× (about 2.4 million orders a month): ELT.** The ETL loads all five files into pandas in memory and passes them between tasks; at 1000× that means gigabytes of memory per run. ELT pushes the work into the database engine. DuckDB handles tens of millions of rows on a laptop, and the same SQL moves to PostgreSQL, BigQuery or Snowflake later. Two changes will be needed at scale: load only new files (today a re-run appends a full copy to raw), and build staging incrementally instead of rebuilding it.

**Cost: similar today, ELT costs more storage later.** Both run on free, open-source tools on one machine. ELT stores every raw load, so storage grows over time, but storage is the cheapest part of a data platform, and it's what buys the audit trail compliance asked for. With file de-duplication and a retention policy (8 years for financial raw data, Part E.6), it stays small.

**When ETL would be the better choice:** if raw data must not be stored at all, for example raw customer PII, ETL's "clean before storing" is safer. That's why my ELT masks PII in staging and keeps raw access to data engineering only.

**Top 3 risks of the recommended ELT design:**

1. **Raw holds unmasked personal data.** `raw_customers` has real emails and phone numbers. DuckDB has no user permissions, so anyone with the file can read them. This is a DPDP Act exposure. *Mitigation:* move to a warehouse with role-based access (raw readable only by data engineering), keep the file out of git (done), and add a retention job that deletes PII after the customer leaves.
2. **Bad data can still reach the 9:30 meeting with a green run.** In Experiment 7, a file with only 50 orders produced a −91% revenue report, and the run stayed green with only a warning. *Mitigation:* make volume, freshness and per-store drift checks Critical so the marts aren't rebuilt, and alert the on-call person on Slack.
3. **It runs on one laptop.** If Docker or the laptop is off at 9:00, there's simply no report, and nobody is told (Part C). *Mitigation:* run Airflow on an always-on server or Astronomer, and add a "report not ready by 9:15" alert.

**Next step:** I suggest a two-week pilot. ELT runs daily at 9:00 IST alongside the current Excel report, and finance signs off when the numbers match for 10 working days in a row.

## Screenshots

| What | File |
|---|---|
| Schedule showing 9:00 AM IST and the next-run time | [ScheduleScreenShot.png](ScheduleScreenShot.png) |
| ETL DAG, Graph view, green run (`final2_full`) | `screenshots/Results.png` |
| ELT DAG, Graph view, green run (`final_full`) | `screenshots/Results.png` |

