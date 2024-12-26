import pandas as pd
import urllib.parse

from sqlalchemy import create_engine, types
from sqlalchemy.sql import text
from pymongo import MongoClient

from modules.postgres_connection import get_conn as get_postgres_connection
from modules.mysql_connection import get_conn as get_mysql_connection
from modules.mongodb_connection import get_conn as get_mongodb_connection


def insert_structured_data(postgres_conf, mysql_conf, schema="public"):
    _, engine_postgres = get_postgres_connection(postgres_conf)
    _, engine_mysql = get_mysql_connection(mysql_conf)

    df_employee = pd.read_sql(
        "SELECT * FROM kelompok2_data_management_payroll", engine_postgres
    )
    df_employee = df_employee.rename(
        columns={
            "EmployeeID": "employee_id",
            "Name": "name",
            "Gender": "gender",
            "Age": "age",
            "Department": "department",
            "Position": "position",
            "Salary": "salary",
            "OvertimePay": "overtime_pay",
            "PaymentDate": "payment_date",
        }
    )

    df_employee_training = pd.read_sql(
        "SELECT `EmployeeID`, `TrainingProgram`, `StartDate`, `EndDate`, `Status` FROM kelompok2_data_training_development",
        engine_mysql,
    )
    df_employee_training = df_employee_training.rename(
        columns={
            "EmployeeID": "employee_id",
            "TrainingProgram": "training_program",
            "StartDate": "start_date",
            "EndDate": "end_date",
            "Status": "status",
        }
    )

    df_employee_performance = pd.read_sql(
        'SELECT "EmployeeID", "ReviewPeriod", "Rating", "Comments" FROM kelompok2_data_performance_management',
        engine_postgres,
    )
    df_employee_performance = df_employee_performance.rename(
        columns={
            "EmployeeID": "employee_id",
            "ReviewPeriod": "review_period",
            "Rating": "rating",
            "Comments": "comments",
        }
    )

    with engine_postgres.connect() as connection:
        connection.execute(
            text(f"TRUNCATE {schema}.fact_employees RESTART IDENTITY CASCADE;")
        )

        df_employee.to_sql(
            "fact_employees", connection, schema=schema, if_exists="append", index=False
        )
        df_employee_training.to_sql(
            "dim_trainings", connection, schema=schema, if_exists="append", index=False
        )
        df_employee_performance.to_sql(
            "dim_performances",
            connection,
            schema=schema,
            if_exists="append",
            index=False,
        )


def insert_unstructured_data(
    mongo_conf, postgres_conf, schema="public", collection_name=None
):
    _, engine_postgres = get_postgres_connection(postgres_conf, schema)

    mongo_server = get_mongodb_connection(mongo_conf)

    db_mongo = mongo_server[mongo_conf.schema]
    collection = db_mongo[collection_name]

    if collection_name:
        df_recruitments = pd.DataFrame(list(collection.find()))
        df_recruitments = df_recruitments.drop(columns=["_id"], axis=1)
        df_recruitments = df_recruitments.rename(
            columns={
                "CandidateID": "candidate_id",
                "Name": "name",
                "Gender": "gender",
                "Age": "age",
                "Position": "position",
                "ApplicationDate": "application_date",
                "Status": "status",
                "InterviewDate": "interview_date",
                "OfferStatus": "offer_status",
                "Predict": "predict",
            }
        )
    else:
        df_recruitments = pd.DataFrame()

    with engine_postgres.connect() as connection:
        connection.execute(
            text(f"TRUNCATE {schema}.dim_recruitments RESTART IDENTITY CASCADE;")
        )

        if not df_recruitments.empty:
            df_recruitments.to_sql(
                "dim_recruitments",
                connection,
                schema=schema,
                if_exists="append",
                index=False,
            )

            connection.execute(
                text(f"TRUNCATE {schema}.employee_candidate_maps RESTART IDENTITY;")
            )

            employee_candidate_maps = pd.read_sql(
                f"""
                    SELECT fe.employee_id, dr.candidate_id FROM {schema}.fact_employees fe 
                    INNER JOIN {schema}.dim_recruitments dr ON dr."name" = fe."name" AND dr.gender = fe.gender AND dr.age = fe.age AND dr."position" = fe."position";                          
                """,
                connection,
            )

            employee_candidate_maps.to_sql(
                "employee_candidate_maps",
                connection,
                schema=schema,
                if_exists="append",
                index=False,
            )
