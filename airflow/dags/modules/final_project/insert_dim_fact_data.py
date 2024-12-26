import pandas as pd
from sqlalchemy.sql import text

from modules.postgres_connection import get_conn as get_postgres_connection
from modules.mysql_connection import get_conn as get_mysql_connection
from modules.mongodb_connection import get_conn as get_mongodb_connection


def execute(
    postgres_conf,
    postgres_schema="public",
):
    ingest_dimension_table(postgres_conf, postgres_schema)
    ingest_fact_table(postgres_conf, postgres_schema)


def ingest_dimension_table(postgres_conf, schema):
    _, engine_postgres = get_postgres_connection(postgres_conf, schema)

    df_training_programs = pd.read_sql(
        "SELECT DISTINCT(training_program) AS name FROM int_training_developments",
        engine_postgres,
    )

    df_employees = pd.read_sql(
        "SELECT employee_id, name, gender, age, department, position FROM int_employee_managements",
        engine_postgres,
    )

    df_candidates = pd.read_sql(
        "SELECT candidate_id, name, gender, age, position FROM int_recruitment_selections",
        engine_postgres,
    )

    with engine_postgres.connect() as connection:
        connection.execute(
            text(
                f"TRUNCATE {schema}.dim_training_programs RESTART IDENTITY CASCADE;"
                f"TRUNCATE {schema}.dim_employees RESTART IDENTITY CASCADE;"
                f"TRUNCATE {schema}.dim_candidates RESTART IDENTITY CASCADE;"
            )
        )

        df_training_programs.to_sql(
            "dim_training_programs",
            connection,
            schema=schema,
            if_exists="append",
            index=False,
        )

        df_employees.to_sql(
            "dim_employees",
            connection,
            schema=schema,
            if_exists="append",
            index=False,
        )

        df_candidates.to_sql(
            "dim_candidates",
            connection,
            schema=schema,
            if_exists="append",
            index=False,
        )


def ingest_fact_table(postgres_conf, schema):
    _, engine_postgres = get_postgres_connection(postgres_conf, schema)

    df_employee_payrolls = pd.read_sql(
        """
            SELECT de.employee_id AS employee_id, salary, overtime_pay, dt.id AS payment_date_id
            FROM int_employee_managements iem
            INNER JOIN dim_employees de ON de.employee_id = iem.employee_id
            INNER JOIN dim_times dt ON dt.date = iem.payment_date
        """,
        engine_postgres,
    )

    df_employee_performances = pd.read_sql(
        """
            SELECT de.employee_id AS employee_id, dqp.id AS review_period_id, rating, comments
            FROM int_performance_managements ipm
            INNER JOIN dim_employees de ON de.employee_id = ipm.employee_id
            INNER JOIN dim_quarter_periods dqp ON dqp.quarter_period = ipm.review_period
        """,
        engine_postgres,
    )

    df_employee_trainings = pd.read_sql(
        """
            SELECT
                de.employee_id AS employee_id,
                dtp.id AS training_program_id,
                (SELECT dt.id FROM dim_times dt WHERE dt.date = itd.start_date) AS start_date_id,
                (SELECT dt.id FROM dim_times dt WHERE dt.date = itd.end_date) AS end_date_id,
                itd.status
            FROM int_training_developments itd
            INNER JOIN dim_employees de ON de.employee_id = itd.employee_id
            INNER JOIN dim_training_programs dtp ON dtp.name = itd.training_program;
        """,
        engine_postgres,
    )

    df_candidate_recruitments = pd.read_sql(
        """
            SELECT
                dc.candidate_id AS candidate_id,
                (SELECT dt.id FROM dim_times dt WHERE dt.date = irs.application_date) AS application_date_id,
                irs.status,
                (SELECT dt.id FROM dim_times dt WHERE dt.date = irs.interview_date) AS interview_date_id,
                irs.offer_status
            FROM int_recruitment_selections irs
            INNER JOIN dim_candidates dc ON dc.candidate_id = irs.candidate_id
        """,
        engine_postgres,
    )

    with engine_postgres.connect() as connection:
        connection.execute(
            text(
                f"TRUNCATE {schema}.fact_employee_payrolls RESTART IDENTITY;"
                f"TRUNCATE {schema}.fact_employee_performances RESTART IDENTITY;"
                f"TRUNCATE {schema}.fact_employee_trainings RESTART IDENTITY;"
                f"TRUNCATE {schema}.fact_candidate_recruitments RESTART IDENTITY;"
            )
        )

        df_employee_payrolls.to_sql(
            "fact_employee_payrolls",
            connection,
            schema=schema,
            if_exists="append",
            index=False,
        )

        df_employee_performances.to_sql(
            "fact_employee_performances",
            connection,
            schema=schema,
            if_exists="append",
            index=False,
        )

        df_employee_trainings.to_sql(
            "fact_employee_trainings",
            connection,
            schema=schema,
            if_exists="append",
            index=False,
        )

        df_candidate_recruitments.to_sql(
            "fact_candidate_recruitments",
            connection,
            schema=schema,
            if_exists="append",
            index=False,
        )
