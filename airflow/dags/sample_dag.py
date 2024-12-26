from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

from datetime import datetime

import os

with DAG(
    dag_id="first_sample_dag",
    start_date=datetime(2022, 5, 28),
    schedule_interval="00 23 * * *",
    catchup=False,
) as dag:

    start_task = EmptyOperator(task_id="start")

    hello_world = BashOperator(
        task_id="hello_world", bash_command='echo "Hello, World!"'
    )

    end_task = EmptyOperator(task_id="end")

    test = EmptyOperator(task_id="test")

start_task >> hello_world >> end_task
