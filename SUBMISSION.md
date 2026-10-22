# NammaMart ETL & ELT Pipeline — Submission

**Name:** <your name>
**Repository:** <GitHub link>

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

## Part B: ELT pipeline

## Part C: Scheduling

**Cron expression:**

**Time shown in UTC in the Airflow UI:**

**What would happen at 9 AM if `start_date` were in UTC:**

**What happens if the laptop or Docker is off at 9 AM:**

**Why `catchup=False`:**

## Part D: Data quality checks

Every check runs in both pipelines and writes one row per run to `dq_results`.

- **Critical:** the failing row goes to quarantine (ETL) or gets `dq_status = 'FAILED'` (ELT).
- **Warning:** the row still loads, but the check is logged as `WARN`.

In the code columns, `ts`, `qty`, `price`, `disc`, `amt`, `mins`, `paid`, `mrp` and `cost` are the parsed (numeric or date) versions of the text columns. `now` is the run time.

#### orders.csv

| check_name | Dimension | Severity | ETL (pandas): row fails when… | ELT (SQL): row fails when… |
|---|---|---|---|---|
| `orders_order_id_unique` | Uniqueness | Critical | `df.order_id.duplicated(keep="first")` | `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at) > 1` |
| `orders_customer_id_not_null` | Completeness | Critical | `df.customer_id.isna()` | `customer_id IS NULL` |
| `orders_order_ts_not_null` | Completeness | Critical | `df.order_ts.isna()` | `order_ts IS NULL` |
| `orders_order_ts_valid` | Validity | Critical | `df.order_ts.notna() & ts.isna()` with `format="%Y-%m-%d %H:%M:%S", errors="coerce"` | `order_ts IS NOT NULL AND TRY_STRPTIME(order_ts, '%Y-%m-%d %H:%M:%S') IS NULL` |
| `orders_order_ts_not_future` | Timeliness | Critical | `ts > now` | `ts > now()` |
| `orders_status_allowed` | Validity | Critical | `~df.order_status.str.strip().str.upper().isin(["DELIVERED", "CANCELLED", "RETURNED"])` | `UPPER(TRIM(order_status)) NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED')` |
| `orders_payment_mode_allowed` | Validity | Warning | Mode is not already one of the 4 allowed values (it gets mapped, then loads) | `payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet')` |
| `orders_quantity_positive` | Accuracy | Critical | `qty <= 0` | `TRY_CAST(quantity AS INTEGER) <= 0` |
| `orders_discount_range` | Validity | Critical | `~disc.between(0, 100)` | `TRY_CAST(discount_pct AS DOUBLE) NOT BETWEEN 0 AND 100` |
| `orders_amount_formula` | Accuracy | Critical | `(amt - (qty * price * (1 - disc / 100)).round(2)).abs() > 0.01` | `ABS(amt - ROUND(qty * price * (1 - disc / 100), 2)) > 0.01` |
| `orders_delivered_has_minutes` | Completeness | Critical | `(status == "DELIVERED") & mins.isna()` | `status = 'DELIVERED' AND delivery_minutes IS NULL` |
| `orders_delivery_minutes_max` | Accuracy | Critical | `mins >= 120` | `TRY_CAST(delivery_minutes AS DOUBLE) >= 120` |
| `orders_product_exists` | Consistency | Critical | `~df.product_id.isin(products.product_id)` | `product_id NOT IN (SELECT product_id FROM raw_products)` |
| `orders_store_exists` | Consistency | Critical | `~df.store_id.isin(stores.store_id)` | `store_id NOT IN (SELECT store_id FROM raw_stores)` |
| `orders_freshness` | Timeliness | Warning | Whole file: `ts.max() < now - 1 day` | `MAX(ts) < now() - INTERVAL 1 DAY` |

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
| `customers_signup_not_future` | Timeliness | Critical | `signup > now` | `TRY_CAST(signup_date AS DATE) > current_date` |
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

**Totals:** 34 checks (25 Critical, 9 Warning).

#### Design decisions

1. **Clean first, then check.** Text is trimmed, `delivered` is uppercased, and payment modes are mapped (`upi`/`UPI ` → UPI, `CARD` → Card, `cod`/`Cashh` → Cash on Delivery) *before* the allowed-value checks. Only values that can't be mapped, like `Pending??`, fail.
2. **Duplicates keep one copy.** The first copy loads (for customers, the newest by `signup_date`). Only the extra copies are quarantined, so a real order is never lost.
3. **Key checks use the full master file.** `orders_product_exists` checks the product IDs in `products.csv` as received, not just the clean ones. So if a product is quarantined (for example P0109, negative MRP), its real orders still count toward revenue. Only the product's own details are missing.
4. **Payment mismatches are reported, not rejected.** Finance needs to see them, so they go into the reconciliation table instead of quarantine.
5. **`orders_freshness` will warn on every run of this dataset.** The data ends on 30 September, so the latest order is always more than 1 day old. This is the correct result, and it's useful evidence for observability Experiment 9.

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

### 2. Data dictionary

### 3. Lineage

### 4. Personal data

### 5. Access control

### 6. Retention

### 7. Secrets

### 8. Audit

### 9. Change history

## Part F: Observability tests

| # | Experiment | Expected | Observed | Where I saw it | What I would change |
|---|---|---|---|---|---|
| 1 | Happy path | | | | |
| 2 | Re-run (idempotency) | | | | |
| 3 | Incremental load | | | | |
| 4 | Missing source file | | | | |
| 5 | New bad data | | | | |
| 6 | Schema change | | | | |
| 7 | Volume anomaly | | | | |
| 8 | Distribution drift | | | | |
| 9 | Freshness | | | | |
| 10 | Recovery | | | | |

### Five pillars of observability

| Pillar | How my pipeline covers it | Strength |
|---|---|---|
| Freshness | | |
| Volume | | |
| Schema | | |
| Distribution | | |
| Lineage | | |

## Business questions

1. Daily net revenue by zone and by store
2. Revenue and gross margin by product category
3. Average delivery time per store, and % delivered within 15 minutes
4. Revenue by payment mode, and total amount not reconciled with the gateway
5. Top 10 customers by spend, with loyalty tier
6. Rows rejected today, by file and by reason

## Part G: Recommendation to the CTO

## Screenshots
