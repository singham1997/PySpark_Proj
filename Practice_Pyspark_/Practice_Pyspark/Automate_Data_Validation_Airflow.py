import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when


# PySpark validation functions
def check_missing_values(df):
    print("Checking for missing values (Nulls):")
    missing_values = df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()


def check_duplicates(df):
    print("Checking for duplicate rows:")
    duplicates = df.groupBy(df.columns).count().filter("count > 1").show()


def validate_schema(df, expected_schema):
    print("Validating schema:")
    actual_schema = df.schema
    if actual_schema == expected_schema:
        print("Schema validation successful!")
    else:
        print(f"Schema validation failed. Expected: {expected_schema}, Got: {actual_schema}")


def run_data_validation():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("DataValidation") \
        .master("local[*]") \
        .getOrCreate()

    # Example data
    data = [
        (1, "John", "Male", 30),
        (2, "Jane", "Female", None),  # Missing age value
        (3, "Alice", "Female", 25),
        (4, "Bob", "Male", 40),
        (5, "Alice", "Female", 25),  # Duplicate entry (Alice)
    ]
    columns = ["ID", "Name", "Gender", "Age"]

    # Create DataFrame
    df = spark.createDataFrame(data, columns)

    # Define expected schema (for validation purposes)
    expected_schema = spark.read.json(spark.sparkContext.parallelize([{}])).schema

    # Run validation checks
    check_missing_values(df)
    check_duplicates(df)
    validate_schema(df, expected_schema)

    # Stop the Spark session after validation
    spark.stop()


# Define Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 11),  # Adjust to your start date
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pyspark_data_validation',
    default_args=default_args,
    description='Automated PySpark Data Validation',
    schedule_interval='@daily',  # Can be adjusted to your preferred schedule
    catchup=False,  # Ensure the DAG runs only for the current date and doesn't backfill
)

# Create an Airflow task to run the data validation
run_validation_task = PythonOperator(
    task_id='run_data_validation',
    python_callable=run_data_validation,
    dag=dag,
)

run_validation_task
