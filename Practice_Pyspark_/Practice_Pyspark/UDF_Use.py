

import logging
import os

from pyspark.sql.types import IntegerType
from pyspark.sql.functions import udf

from utils import get_spark_session

APP_NAME = 'Calculate Department wise employees total and average salary'
ENVIRON = os.environ.get('ENVIRON')

def get_data_using_dataframe(data: list[tuple], schema: list[str]) -> None:
    spark = None

    try:
        spark = get_spark_session(APP_NAME, ENVIRON)

        user_data_df = spark \
            .createDataFrame(data=data, schema=schema)

        def get_name_length(user):
            return len(user)

        name_len_udf = udf(get_name_length, IntegerType())

        result_df = user_data_df \
            .withColumn('name_len', name_len_udf(user_data_df['name']))

        result_df.show()
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

        def get_name_length(user):
            return len(user)

        spark.udf.register('get_name_length', get_name_length, IntegerType())

        employee_data_df.createOrReplaceTempView('employee')

        spark_sql = """
            SELECT 
                name, get_name_length(name) as name_len
            FROM employee
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
    # Sample data
    user_data = [("Alice",), ("Bob",), ("Charlie",)]
    user_schema = ["name"]

    get_data_using_dataframe(user_data, user_schema)
    get_data_using_spark_sql(user_data, user_schema)
