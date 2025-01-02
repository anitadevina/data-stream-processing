from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Connection

from datetime import datetime

from modules.final_project.create_schema import *
from dags.modules.final_project.insert_dim_fact_data import execute as insert_data
from dags.modules.final_project.insert_stage_data import *
from dags.modules.final_project.create_mart import *

postgres_conn = Connection.get_connection_from_secrets("postgres-server-ftde")
mysql_conn = Connection.get_connection_from_secrets("mysql-server-ftde")
mongo_conn = Connection.get_connection_from_secrets("mongo-server-ftde")


def func_create_schema():
    create_schema(postgres_conn, "kelompok2_dwh")


def func_insert_stage_data():
    insert_structured_data(postgres_conn, mysql_conn, "kelompok2_dwh")
    insert_unstructured_data(
        mongo_conn,
        postgres_conn,
        "kelompok2_dwh",
        "kelompok2_data_recruitment_selection",
    )


def func_insert_dwh_data():
    insert_data(postgres_conn, "kelompok2_dwh")


def create_and_export_marts():
    create_and_export_data_marts(postgres_conn)


with DAG(
    dag_id="run_final_project",
    start_date=datetime(2022, 5, 28),
    schedule_interval="0 * 1 * *",
    catchup=False,
) as dag:

    start_task = EmptyOperator(task_id="start")

    create_database_schema = PythonOperator(
        task_id="create_schema",
        python_callable=func_create_schema,
    )

    insert_stage_data = PythonOperator(
        task_id="insert_stage_data",
        python_callable=func_insert_stage_data,
    )

    insert_dwh_data = PythonOperator(
        task_id="insert_dwh_data",
        python_callable=func_insert_dwh_data,
    )

    create_and_export_task = PythonOperator(
        task_id="create_and_export_data_marts",
        python_callable=create_and_export_marts,
    )

    end_task = EmptyOperator(task_id="end")

(
    start_task
    >> create_database_schema
    >> insert_stage_data
    >> insert_dwh_data
    >> create_and_export_task
    >> end_task
)
