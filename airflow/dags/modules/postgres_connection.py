import urllib.parse
import psycopg2
from sqlalchemy import create_engine


def get_conn(conn_data, schema=None):
    try:
        conn = psycopg2.connect(
            host=conn_data.host,
            database=conn_data.schema,
            user=conn_data.login,
            password=conn_data.password,
            port=conn_data.port,
            options=f"-c search_path={schema}" if schema else None,
        )

        connect_args = {"options": "-csearch_path={}".format(schema)} if schema else {}

        engine = create_engine(
            "postgresql+psycopg2://{}:{}@{}:{}/{}".format(
                conn_data.login,
                urllib.parse.quote_plus(conn_data.password),
                conn_data.host,
                conn_data.port,
                conn_data.schema,
            ),
            connect_args=connect_args,
        )

        print("Postgres connection successful")

        return conn, engine

    except Exception as e:
        print("Failed to connect to postgres")
        print(str(e))
