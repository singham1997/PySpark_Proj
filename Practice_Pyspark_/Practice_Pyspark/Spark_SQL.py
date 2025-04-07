

import logging
import os

from utils import get_spark_session

APP_NAME = 'Calculate Department wise employees total and average salary'
ENVIRON = os.environ.get('ENVIRON')

def get_data_using_spark_sql(employee_data: list[tuple], employee_schema: list[str]) -> None:
    spark = None

    try:
        spark = get_spark_session(APP_NAME, ENVIRON)

        employee_data_df = spark \
            .createDataFrame(data=employee_data, schema=employee_schema)

        employee_data_df.createOrReplaceTempView('employee')

        spark_sql = """
            SELECT 
                name, department, salary
            FROM employee
            WHERE department='IT'
            ORDER BY salary DESC
        """

        employee_info_data_df = spark.sql(spark_sql)

        employee_info_data_df.show()
    except Exception as e:
        logging.error(f"Running program has an exception: {str(e)}")
    finally:
        if spark:
            spark.stop()

if __name__ == '__main__':
    # Sample Data
    employee_data = [("Alice", "HR", 3000), ("Bob", "Finance", 4000), ("Charlie", "IT", 5000), ("David", "IT", 4500)]
    employee_schema = ["name", "department", "salary"]

    get_data_using_spark_sql(employee_data, employee_schema)
