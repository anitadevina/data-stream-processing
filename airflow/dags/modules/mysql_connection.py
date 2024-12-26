import urllib.parse
import pymysql
from sqlalchemy import create_engine


def get_conn(conn_data):
    try:
        conn = pymysql.connect(
            host=conn_data.host,
            user=conn_data.login,
            password=conn_data.password,
            database=conn_data.schema,
            port=conn_data.port,
        )

        engine = create_engine(
            "mysql+pymysql://{}:{}@{}:{}/{}".format(
                conn_data.login,
                (
                    urllib.parse.quote_plus(conn_data.password)
                    if conn_data.password is not None
                    else ""
                ),
                conn_data.host,
                conn_data.port,
                conn_data.schema,
            ),
        )

        print("MySQL connection successful")

        return conn, engine

    except Exception as e:
        print("Failed to connect to mysql")
        print(str(e))
