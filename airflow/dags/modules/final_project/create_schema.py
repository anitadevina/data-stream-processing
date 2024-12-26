from airflow import settings
from airflow.exceptions import AirflowException

import os
import sqlparse

from dags.modules.postgres_connection import get_conn as connection


def create_schema(conn_data, schema=None):
    conn, _ = connection(conn_data)
    cursor = conn.cursor()

    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    conn.commit()

    conn_dwh, _ = connection(conn_data, schema)
    cursor_dwh = conn_dwh.cursor()

    path_query = os.path.join(settings.DAGS_FOLDER, "modules", "final_project", "query")

    dwh_stage = sqlparse.format(
        open(os.path.join(path_query, "dwh_intermediate.sql"), "r").read(),
        strip_comments=True,
    ).strip()

    dwh = sqlparse.format(
        open(os.path.join(path_query, "dwh_dim_fact.sql"), "r").read(),
        strip_comments=True,
    ).strip()

    queries = [dwh_stage, dwh]

    try:
        print("Creating schema...")

        for query in queries:
            cursor_dwh.execute(query)

        conn_dwh.commit()

        print("Schema has been created")

    except Exception as e:
        print("Creating schema is failed")
        print(str(e))
        raise AirflowException("Error occurred")
