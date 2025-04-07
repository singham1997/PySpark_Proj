import logging
import os

from utils import get_spark_session
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

APP_NAME = 'Custom Schema'
ENVIRON = os.environ.get('ENVIRON')

def get_data_using_dataframe(data: list[tuple], schema: list[str]=None) -> None:
    spark = None

    try:
        spark = get_spark_session(APP_NAME, ENVIRON)

        users_schema = StructType(
            [
                StructField("user_id", IntegerType(), nullable=False),
                StructField("user_name", StringType(), nullable=True),
                StructField("user_age", IntegerType(), nullable=True)
            ]
        )

        user_data_df = spark \
                .createDataFrame(data=data, schema=users_schema)

        user_data_df.show()
    except Exception as e:
        logging.error(f"Running program has an exception: {str(e)}")
    finally:
        if spark:
            spark.stop()


if __name__ == '__main__':
    users_data = [(1, "Alice", 25),
                  (2, "Bob", 30),
                  (3, "Charlie", 35)]



    get_data_using_dataframe(users_data)