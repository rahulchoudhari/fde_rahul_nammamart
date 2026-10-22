"""
include/nammamart_dq.py
------------------------
Data quality checks, cleaning and masking for the NammaMart ETL pipeline.

Every check from SUBMISSION.md Part D lives in CHECKS. Each one is a function that
takes the raw file (all columns as text) and returns a boolean Series: True = row fails.

Usage from a DAG task:

    from nammamart_dq import run_checks
    passed, failed, results = run_checks("orders", df, ctx, run_id, dag_id)

`ctx` holds what checks need beyond the file itself:
    ctx = {"now": pd.Timestamp(...), "orders": raw_orders_df,
           "products": raw_products_df, "stores": raw_stores_df,
           "orders_history_rows": [rows in the last 5 loads of this orders file]}
"""

from dataclasses import dataclass
from typing import Callable

import pandas as pd

# ── Business rules ────────────────────────────────────────────────────────────

ORDER_STATUSES = ["DELIVERED", "CANCELLED", "RETURNED"]
PAYMENT_MODES = ["UPI", "Card", "Cash on Delivery", "Wallet"]
LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
TS_FORMAT = "%Y-%m-%d %H:%M:%S"
MAX_DELIVERY_MINUTES = 120

# Messy payment modes seen in the data → the allowed value (keys are trimmed + lowercased)
PAYMENT_MODE_MAP = {
    "upi": "UPI",
    "card": "Card",
    "wallet": "Wallet",
    "cash on delivery": "Cash on Delivery",
    "cod": "Cash on Delivery",
    "cashh": "Cash on Delivery",
}


# ── Parsing helpers (raw text → typed values; bad values become NaN/NaT) ──────

def num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def ts(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, format=TS_FORMAT, errors="coerce")


def date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")


def status(s: pd.Series) -> pd.Series:
    return s.str.strip().str.upper()


def map_payment_mode(s: pd.Series) -> pd.Series:
    """Map messy modes to the allowed list. Unknown values are kept as-is (trimmed)."""
    cleaned = s.str.strip()
    return cleaned.str.lower().map(PAYMENT_MODE_MAP).fillna(cleaned)


# ── Checks ────────────────────────────────────────────────────────────────────

@dataclass
class Check:
    name: str
    file: str
    dimension: str
    severity: str                                        # "Critical" or "Warning"
    fails: Callable[[pd.DataFrame, dict], pd.Series]     # True = row fails
    table_level: bool = False                            # True = one verdict for the whole file


def _orders_amount_formula(df, ctx):
    expected = (num(df.quantity) * num(df.unit_price) * (1 - num(df.discount_pct) / 100)).round(2)
    return (num(df.order_amount) - expected).abs() > 0.01


def _orders_freshness(df, ctx):
    # Ignore future-dated rows, otherwise a single 2027 typo makes stale data look fresh
    order_ts = ts(df.order_ts)
    stale = order_ts[order_ts <= ctx["now"]].max() < ctx["now"] - pd.Timedelta(days=1)
    return pd.Series(stale, index=df.index)


def _orders_unit_price_le_mrp(df, ctx):
    # Selling above MRP is illegal in India; in clean data unit_price always equals MRP
    mrp = num(df.product_id.map(ctx["products"].drop_duplicates("product_id").set_index("product_id").mrp))
    return (mrp > 0) & (num(df.unit_price) > mrp + 0.01)


def _orders_volume(df, ctx):
    # Fewer than 10% of the usual rows for this file (median of the last 5 loads) → warn
    history = ctx.get("orders_history_rows") or []
    dropped = bool(history) and len(df) < 0.1 * pd.Series(history).median()
    return pd.Series(dropped, index=df.index)


def _payments_amount_matches_order(df, ctx):
    order_amount = ctx["orders"].drop_duplicates("order_id").set_index("order_id").order_amount
    expected = num(df.order_id.map(order_amount))
    return (df.payment_status.str.strip().str.upper() == "SUCCESS") & ((num(df.amount_paid) - expected).abs() > 0.01)


def _customers_id_unique(df, ctx):
    # Keep the newest copy of each customer (latest signup_date); earlier copies fail
    order = date(df.signup_date).sort_values(kind="stable", na_position="first").index
    return df.loc[order].customer_id.duplicated(keep="last").reindex(df.index)


CHECKS = [
    # orders.csv
    Check("orders_order_id_unique", "orders", "Uniqueness", "Critical",
          lambda df, ctx: df.order_id.duplicated(keep="first")),
    Check("orders_customer_id_not_null", "orders", "Completeness", "Critical",
          lambda df, ctx: df.customer_id.isna()),
    Check("orders_order_ts_not_null", "orders", "Completeness", "Critical",
          lambda df, ctx: df.order_ts.isna()),
    Check("orders_order_ts_valid", "orders", "Validity", "Critical",
          lambda df, ctx: df.order_ts.notna() & ts(df.order_ts).isna()),
    Check("orders_order_ts_not_future", "orders", "Timeliness", "Critical",
          lambda df, ctx: ts(df.order_ts) > ctx["now"]),
    Check("orders_status_allowed", "orders", "Validity", "Critical",
          lambda df, ctx: ~status(df.order_status).isin(ORDER_STATUSES)),
    Check("orders_payment_mode_allowed", "orders", "Validity", "Warning",
          lambda df, ctx: ~df.payment_mode.isin(PAYMENT_MODES)),
    Check("orders_quantity_positive", "orders", "Accuracy", "Critical",
          lambda df, ctx: num(df.quantity) <= 0),
    Check("orders_discount_range", "orders", "Validity", "Critical",
          lambda df, ctx: ~num(df.discount_pct).between(0, 100)),
    Check("orders_amount_formula", "orders", "Accuracy", "Critical", _orders_amount_formula),
    Check("orders_delivered_has_minutes", "orders", "Completeness", "Critical",
          lambda df, ctx: (status(df.order_status) == "DELIVERED") & df.delivery_minutes.isna()),
    Check("orders_delivery_minutes_max", "orders", "Accuracy", "Critical",
          lambda df, ctx: num(df.delivery_minutes) >= MAX_DELIVERY_MINUTES),
    Check("orders_product_exists", "orders", "Consistency", "Critical",
          lambda df, ctx: ~df.product_id.isin(ctx["products"].product_id)),
    Check("orders_store_exists", "orders", "Consistency", "Critical",
          lambda df, ctx: ~df.store_id.isin(ctx["stores"].store_id)),
    Check("orders_unit_price_le_mrp", "orders", "Accuracy", "Warning", _orders_unit_price_le_mrp),
    Check("orders_freshness", "orders", "Timeliness", "Warning", _orders_freshness, table_level=True),
    Check("orders_volume", "orders", "Completeness", "Warning", _orders_volume, table_level=True),

    # payments.csv
    Check("payments_payment_id_unique", "payments", "Uniqueness", "Critical",
          lambda df, ctx: df.payment_id.duplicated(keep="first")),
    Check("payments_order_exists", "payments", "Consistency", "Critical",
          lambda df, ctx: ~df.order_id.isin(ctx["orders"].order_id)),
    Check("payments_amount_positive", "payments", "Accuracy", "Critical",
          lambda df, ctx: num(df.amount_paid) <= 0),
    Check("payments_amount_matches_order", "payments", "Consistency", "Warning", _payments_amount_matches_order),
    Check("payments_mode_allowed", "payments", "Validity", "Warning",
          lambda df, ctx: ~df.payment_mode.isin(PAYMENT_MODES)),
    Check("payments_gateway_ref_present", "payments", "Completeness", "Warning",
          lambda df, ctx: (df.payment_status.str.strip().str.upper() == "SUCCESS") & df.gateway_ref.isna()),

    # customers.csv
    Check("customers_id_unique", "customers", "Uniqueness", "Critical", _customers_id_unique),
    Check("customers_email_format", "customers", "Validity", "Warning",
          lambda df, ctx: ~df.email.fillna("").str.match(r"^[^@\s]+@[^@\s]+\.[a-z]{2,}$")),
    Check("customers_phone_10_digits", "customers", "Validity", "Warning",
          lambda df, ctx: ~df.phone.fillna("").str.fullmatch(r"\d{10}")),
    Check("customers_pincode_6_digits", "customers", "Validity", "Warning",
          lambda df, ctx: ~df.pincode.fillna("").str.fullmatch(r"\d{6}")),
    Check("customers_city_not_null", "customers", "Completeness", "Warning",
          lambda df, ctx: df.city.isna()),
    Check("customers_signup_not_future", "customers", "Timeliness", "Critical",
          lambda df, ctx: date(df.signup_date) > ctx["now"]),
    Check("customers_tier_casing", "customers", "Consistency", "Warning",
          lambda df, ctx: ~df.loyalty_tier.isin(LOYALTY_TIERS)),

    # products.csv
    Check("products_id_unique", "products", "Uniqueness", "Critical",
          lambda df, ctx: df.product_id.duplicated(keep="first")),
    Check("products_category_not_null", "products", "Completeness", "Critical",
          lambda df, ctx: df.category.isna()),
    Check("products_mrp_positive", "products", "Accuracy", "Critical",
          lambda df, ctx: num(df.mrp) <= 0),
    Check("products_cost_le_mrp", "products", "Accuracy", "Critical",
          lambda df, ctx: num(df.cost_price) > num(df.mrp)),

    # stores.csv
    Check("stores_zone_not_null", "stores", "Completeness", "Critical",
          lambda df, ctx: df.zone.isna()),
    Check("stores_capacity_positive", "stores", "Accuracy", "Warning",
          lambda df, ctx: num(df.capacity_orders_per_day) <= 0),
]


def run_checks(file: str, df: pd.DataFrame, ctx: dict, run_id: str, dag_id: str):
    """
    Run every check for one file.

    Returns:
        passed  - rows with no Critical failure (Warning rows are included)
        failed  - rows with at least one Critical failure, plus a `failure_reason` column
        results - one dict per check, ready to insert into dq_results
    """
    reasons = pd.Series("", index=df.index)
    critical = pd.Series(False, index=df.index)
    results = []
    checked_at = pd.Timestamp.now(tz="UTC")

    for check in (c for c in CHECKS if c.file == file):
        fails = check.fails(df, ctx).fillna(False).astype(bool)

        if check.table_level:
            rows_checked, rows_failed = 1, int(fails.any())
        else:
            rows_checked, rows_failed = len(df), int(fails.sum())
            if check.severity == "Critical":
                critical |= fails
                reasons[fails] += check.name + "; "

        status_ = "PASS" if rows_failed == 0 else ("FAIL" if check.severity == "Critical" else "WARN")
        results.append({
            "run_id": run_id, "dag_id": dag_id, "check_name": check.name,
            "dimension": check.dimension, "severity": check.severity,
            "rows_checked": rows_checked, "rows_failed": rows_failed,
            "status": status_, "checked_at": checked_at,
        })

    failed = df[critical].copy()
    failed["failure_reason"] = reasons[critical].str.rstrip("; ")
    return df[~critical].copy(), failed, results


# ── Cleaning (run on passed rows, in the Transform step) ──────────────────────

def _strip(df: pd.DataFrame) -> pd.DataFrame:
    return df.apply(lambda col: col.str.strip() if col.dtype == object else col)


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    df = _strip(df)
    df["order_status"] = status(df.order_status)
    df["payment_mode"] = map_payment_mode(df.payment_mode)
    df["order_ts"] = ts(df.order_ts)
    for col in ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"]:
        df[col] = num(df[col])
    df["quantity"] = df.quantity.astype("int64")
    return df


def clean_payments(df: pd.DataFrame) -> pd.DataFrame:
    df = _strip(df)
    df["payment_mode"] = map_payment_mode(df.payment_mode)
    df["payment_status"] = df.payment_status.str.upper()
    df["payment_ts"] = ts(df.payment_ts)
    df["amount_paid"] = num(df.amount_paid)
    return df


def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Standardise and mask PII. full_name is dropped: no report needs it."""
    df = _strip(df)
    df["loyalty_tier"] = df.loyalty_tier.str.title()
    df["signup_date"] = date(df.signup_date)
    df["is_active"] = df.is_active.str.upper().eq("Y")
    df["email"] = df.email.map(mask_email)
    df["phone"] = df.phone.map(mask_phone)
    return df.drop(columns=["full_name"])


def clean_products(df: pd.DataFrame) -> pd.DataFrame:
    df = _strip(df)
    df["mrp"] = num(df.mrp)
    df["cost_price"] = num(df.cost_price)
    df["is_perishable"] = df.is_perishable.str.lower().eq("true")
    df["launch_date"] = date(df.launch_date)
    return df


def clean_stores(df: pd.DataFrame) -> pd.DataFrame:
    df = _strip(df)
    df["capacity_orders_per_day"] = num(df.capacity_orders_per_day).astype("int64")
    df["open_date"] = date(df.open_date)
    return df


# ── PII masking ───────────────────────────────────────────────────────────────

def mask_email(email) -> str | None:
    """priya.rao@example.com → pr***@example.com. Malformed emails are fully hidden."""
    if not isinstance(email, str) or email.count("@") != 1:
        return None if pd.isna(email) else "***"
    local, domain = email.split("@")
    return f"{local[:2]}***@{domain}"


def mask_phone(phone) -> str | None:
    """9876543221 → 98******21. Keeps the first 2 and last 2 digits."""
    if not isinstance(phone, str):
        return None
    digits = "".join(ch for ch in phone if ch.isdigit())
    if len(digits) < 4:
        return "*" * len(digits)
    return digits[:2] + "*" * (len(digits) - 4) + digits[-2:]
