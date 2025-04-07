import logging
import os

from utils import get_spark_session

APP_NAME = 'User Department Table Join'
ENVIRON = os.environ.get('ENVIRON')

def get_data_using_dataframe(data1: list[tuple], schema1: list[str], data2: list[tuple], schema2: list[str]) -> None:
    spark = None

    try:
        spark = get_spark_session(APP_NAME, ENVIRON)

        user_data_df = spark \
                .createDataFrame(data=data1, schema=schema1)

        department_data_df = spark \
            .createDataFrame(data=data2, schema=schema2)

        join_cond = user_data_df['name'] == department_data_df['name']
        join_type = "inner"

        join_user_department_df = user_data_df \
            .join(department_data_df, how=join_type, on=join_cond) \
            .drop(department_data_df['name']) \
            .select('id', 'name', 'department')

        join_user_department_df.show()

    except Exception as e:
        logging.error(f"Running program has an exception: {str(e)}")
    finally:
        if spark:
            spark.stop()

def get_data_using_spark_sql(data1: list[tuple], schema1: list[str], data2: list[tuple], schema2: list[str]) -> None:
    spark = None

    try:
        spark = get_spark_session(APP_NAME, ENVIRON)

        user_data_df = spark \
            .createDataFrame(data=data1, schema=schema1)

        department_data_df = spark \
            .createDataFrame(data=data2, schema=schema2)

        user_data_df.createOrReplaceTempView('user')
        department_data_df.createOrReplaceTempView('department_tab')

        spark_sql = """
            SELECT 
                u.id, u.name, d.department
            FROM user u INNER JOIN department_tab d
            ON u.name=d.name
        """

        join_user_department_data_df = spark.sql(spark_sql)

        join_user_department_data_df.show()
    except Exception as e:
        logging.error(f"Running program has an exception: {str(e)}")
    finally:
        if spark:
            spark.stop()

if __name__ == '__main__':
    # Create sample DataFrames
    data1 = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
    schema1 = ["name", "id"]

    data2 = [("Alice", "HR"), ("Bob", "Finance"), ("David", "IT")]
    schema2 = ["name", "department"]

    get_data_using_dataframe(data1, schema1, data2, schema2)
    get_data_using_spark_sql(data1, schema1, data2, schema2)