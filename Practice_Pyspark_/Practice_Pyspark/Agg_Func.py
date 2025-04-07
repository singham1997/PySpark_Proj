import logging
import os

from utils import get_spark_session
from pyspark.sql.functions import sum, avg

APP_NAME = 'Calculate Department wise employees total and average salary'
ENVIRON = os.environ.get('ENVIRON')


def get_data_using_dataframe(data: list[tuple], schema: list[str]) -> None:
    spark = None

    try:
        spark = get_spark_session(APP_NAME, ENVIRON)

        employee_data_df = spark \
            .createDataFrame(data=data, schema=schema)

        employee_agg_salary_df = employee_data_df \
            .groupby('department') \
            .agg(
            sum('salary').alias('total_salary'),
            avg('salary').alias('average_salary')
        )

        employee_agg_salary_df.show()
    except Exception as e:
        logging.error(f"Running program has an exception: {str(e)}")
    finally:
        if spark:
            spark.stop()


def get_data_using_spark_sql(data: list[tuple], schema: list[str]) -> None:
    spark = None

    try:
        spark = get_spark_session(APP_NAME, ENVIRON)

        employee_data_df = spark \
            .createDataFrame(data=data, schema=schema)

        employee_data_df.createDataFrame('employee')

        spark_sql = """
            SELECT department, 
                sum(salary) as total_salary, 
                avg(salary) as average_salary
            FROM employee
            GROUP BY department
        """

        agg_employee_data_df = spark.sql(spark_sql)
        agg_employee_data_df.show()
    except Exception as e:
        logging.error(f"Running program has an exception: {str(e)}")
    finally:
        if spark:
            spark.stop()


if __name__ == '__main__':
    employee_data = [
        ("Alice", "HR", 30000),
        ("Bob", "HR", 35000),
        ("Charlie", "IT", 45000),
        ("David", "IT", 50000),
        ("Eve", "Finance", 60000)
    ]

    employee_schema = ["name", "department", "salary"]

    get_data_using_dataframe(employee_data, employee_schema)
    get_data_using_spark_sql(employee_data, employee_schema)

