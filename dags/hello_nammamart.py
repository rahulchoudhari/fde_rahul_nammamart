from airflow.sdk import dag, task      
import pendulum
import pandas as pd

# Path inside the Airflow container (your project's include/ folder is mounted here)
DATA = "/usr/local/airflow/include/data/nammamart"

@dag(
    dag_id="hello_nammamart",
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 2},      # the project's DAG tests require retries=2
    tags=["Rahul_test"]
)

def hello_nammamart():

    @task
    def count_rows(filename: str) -> int:
        n = len(pd.read_csv(f"{DATA}/{filename}"))
        print(f"{filename}: {n} rows")
        return n

    @task
    def summarise(orders: int, payments: int):
        print(f"Orders={orders}, Payments={payments}, "
              f"orders without payment ≈ {orders - payments}")

    summarise(count_rows("orders.csv"), count_rows("payments.csv"))

hello_nammamart()