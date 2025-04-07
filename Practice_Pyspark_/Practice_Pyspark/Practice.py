import logging
import os

from utils import get_spark_session
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, desc

APP_NAME = 'PERFORM WINDOW FUNCTIONS'
ENVIRON = os.environ.get('ENVIRON')

def get_spark_using_dataframe(data: list[tuple], schema: list[str]) -> None:
    spark = None

    try:
        spark = get_spark_session(APP_NAME, ENVIRON)

        sale_data_df = spark.createDataFrame(data=data, schema=schema)

        window_spec = Window \
            .partitionBy('Department') \
            .orderBy(desc('SaleAmt'))

        result_df = sale_data_df \
            .withColumn('rank', rank.over(window_spec))

        result_df.show()
    except Exception as e:
        logging.error(f"Running program has an exception: {str(e)}")
    finally:
        if spark:
            spark.stop()

def get_spark_using_spark_sql(data: list[tuple], schema: list[str]) -> None:
    spark = None

    try:
        spark = get_spark_session(APP_NAME, ENVIRON)

        sale_data_df = spark.createDataFrame(data=data, schema=schema)

        sale_data_df.createOrReplaceTempView('SALE')

        spark_sql = """
            SELECT SalePerson, 
                Department,
                SaleAmt,
                rank() over(PARTITION BY Department ORDER BY SaleAmt DESC) as rank
            FROM SALE
        """

        result_df = spark.sql(spark_sql)

        result_df.show()
    except Exception as e:
        logging.error(f"Running program has an exception: {str(e)}")
    finally:
        if spark:
            spark.stop()

if __name__ == '__main__':
    sale_data = [
        ('John', 'A', 500),
        ('Mike', 'A', 300),
        ('Jane', 'A', 400),
        ('Kate', 'B', 700),
        ('Mark', 'B', 600),
        ('Olivia', 'B', 500)
    ]

    sale_schema = ["SalePerson", "Department", "SaleAmt"]

    get_spark_using_dataframe(sale_data, sale_schema)
    get_spark_using_spark_sql(sale_data, sale_schema)