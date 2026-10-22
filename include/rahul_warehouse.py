"""
include/rahul_warehouse.py
---------------------------
Shared connection to include/warehouse_rahul.duckdb for the ETL and ELT DAGs.

DuckDB allows only one writing process at a time. Both DAGs run at 9:00 AM and write
to the same file, so instead of failing on the lock we wait and retry.
"""

import time

WAREHOUSE_DB = "/usr/local/airflow/include/warehouse_rahul.duckdb"


def connect(retries: int = 60, wait_seconds: int = 5):
    import duckdb

    for attempt in range(1, retries + 1):
        try:
            return duckdb.connect(WAREHOUSE_DB)
        except duckdb.IOException as e:
            if "lock" not in str(e).lower() or attempt == retries:
                raise
            print(f"[Warehouse] locked by another run, retry {attempt}/{retries} in {wait_seconds}s")
            time.sleep(wait_seconds)
