from pyspark.sql import SparkSession
from gspread import authorize
from oauth2client.service_account import ServiceAccountCredentials
from airflow.hooks.base_hook import BaseHook
import json
from decimal import Decimal


def get_spark_session():
    """Create and return a Spark session."""
    return (
        SparkSession.builder.config(
            "spark.jars.packages", "org.postgresql:postgresql:42.7.0"
        )
        .master("local")
        .appName("PySpark_Postgres_Data_Marts")
        .getOrCreate()
    )

def setup_google_sheets_credentials():
    conn = BaseHook.get_connection("google-sheet-conn")
    key = json.loads(conn.extra)  # Baca data JSON dari field Extra
    scope = ["https://www.googleapis.com/auth/drive", "https://spreadsheets.google.com/feeds"]
    return authorize(ServiceAccountCredentials.from_json_keyfile_dict(key, scope))

def export_to_google_sheets(df_pandas, sheet_name, client):
    """Export a pandas DataFrame to Google Sheets, updating if it already exists."""
    try:
        # Try to open the existing spreadsheet
        spreadsheet = client.open(sheet_name)
        worksheet = spreadsheet.get_worksheet(0)  # Assuming you want the first worksheet
        worksheet.clear()  # Clear the existing data
    except Exception as e:
        # If the spreadsheet does not exist, create a new one
        spreadsheet = client.create(sheet_name)
        worksheet = spreadsheet.get_worksheet(0)

    # Update the worksheet with new data
    worksheet.update([df_pandas.columns.values.tolist()] + df_pandas.values.tolist())

def convert_decimal_to_float(df):
    """Convert Decimal columns in DataFrame to float."""
    for column in df.select_dtypes(include=['object']).columns:
        df[column] = df[column].apply(lambda x: float(x) if isinstance(x, Decimal) else x)
    return df

def create_and_export_data_marts(conn_data):
    try:
        spark = get_spark_session()
    except Exception as e:
        print(f"Failed to create Spark session: {e}")
        return

    # Konfigurasi koneksi PostgreSQL
    jdbc_url = f"jdbc:postgresql://{conn_data.host}/{conn_data.schema}"
    jdbc_properties = {
        "user": f"{conn_data.login}",
        "password": f"{conn_data.password}",
        "driver": "org.postgresql.Driver",
    }

    # Mapping data mart dan nama file Google Sheets
    data_marts_queries = {
        "employee_demographics": """
            SELECT employee_id, name, gender, 
            CASE
                WHEN age < 20 THEN 'Under 20'
                WHEN age BETWEEN 20 AND 29 THEN '20-29'
                WHEN age BETWEEN 30 AND 39 THEN '30-39'
                WHEN age BETWEEN 40 AND 49 THEN '40-49'
                WHEN age >= 50 THEN '50 and above'
                ELSE 'Unknown'
            END AS age_group
            FROM dwh.int_employee_managements
        """,
        "candidate_demographics": """
            SELECT candidate_id, name, gender, 
            CASE
                WHEN age < 20 THEN 'Under 20'
                WHEN age BETWEEN 20 AND 29 THEN '20-29'
                WHEN age BETWEEN 30 AND 39 THEN '30-39'
                WHEN age BETWEEN 40 AND 49 THEN '40-49'
                WHEN age >= 50 THEN '50 and above'
                ELSE 'Unknown'
            END AS age_group,
            predict
            FROM dwh.int_recruitment_selections
        """,
        "employee_cost": """
            SELECT employee_id, name, salary, overtime_pay
            FROM dwh.int_employee_managements
        """,
        "employee_performance": """
            SELECT employee_id, review_period, rating
            FROM dwh.int_performance_managements
        """
    }

    # Mapping nama data mart ke nama Google Sheets
    google_sheets_map = {
        "employee_demographics": "Employee-Demographics",
        "candidate_demographics": "Candidate-Demographics",
        "employee_cost": "Employee-Cost",
        "employee_performance": "Employee-Performance",
    }

    # Setup Google Sheets credentials
    client = setup_google_sheets_credentials()

    # Ekstrak data mart dan ekspor ke Google Sheets
    for mart_name, query in data_marts_queries.items():
        try:
            # Baca data dari PostgreSQL
            df_spark = spark.read.format("jdbc").options(
                url=jdbc_url,
                dbtable=f"({query}) AS tmp",
                **jdbc_properties,
            ).load()

            # Konversi ke pandas
            df_pandas = df_spark.toPandas()

            # Konversi Decimal ke float jika ada
            df_pandas = convert_decimal_to_float(df_pandas)

            # Ekspor ke Google Sheets
            sheet_name = google_sheets_map[mart_name]
            export_to_google_sheets(df_pandas, sheet_name, client)

        except Exception as e:
            print(f"Error processing data mart {mart_name}: {e}")

if __name__ == "__main__":
    create_and_export_data_marts(None)