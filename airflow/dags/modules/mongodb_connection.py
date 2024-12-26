from airflow.exceptions import AirflowException
from pymongo import MongoClient


def get_conn(conn_data):
    try:
        server = (
            MongoClient(
                f"mongodb://{conn_data.login}:{conn_data.password}@{conn_data.host}:{conn_data.port}/"
            )
            if conn_data.login is not ""
            else MongoClient(f"mongodb://{conn_data.host}:{conn_data.port}/")
        )

        db = server.admin
        server_status = db.command("ping")
        print("MongoDB connection successful:", server_status)

        databases = server.list_database_names()
        print("Databases:", databases)

        return server

    except Exception as e:
        print("An error occurred:", e)
        raise AirflowException("Error occurred")
