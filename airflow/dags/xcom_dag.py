from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import time


def get_date() -> str:
    return str(datetime.now())


def save_date(ti) -> None:
    time.sleep(10)
    dt = ti.xcom_pull(task_ids="get_date")
    if not dt:
        raise ValueError("No value currently stored in XComs.")

    print(dt)


with DAG(
    dag_id="xcom_dag",
    schedule_interval="@daily",
    start_date=datetime(2022, 3, 1),
    catchup=False,
) as dag:

    task_get_date = PythonOperator(
        task_id="get_date", python_callable=get_date, do_xcom_push=True
    )

    task_save_date = PythonOperator(task_id="save_date", python_callable=save_date)

task_get_date >> task_save_date
