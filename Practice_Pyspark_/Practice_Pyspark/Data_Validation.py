import logging
import os

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from utils import get_spark_session
from pyspark.sql.functions import count, when, col

APP_NAME = 'Calculate Department wise employees total and average salary'
ENVIRON = os.environ.get('ENVIRON')

def check_validation(data: list[tuple], schema: list[str]=None) -> None:
    spark = None

    try:
        spark = get_spark_session(APP_NAME, ENVIRON)

        user_data_df = spark \
                .createDataFrame(data=data, schema=schema)

        def check_missing_value(df):
            print(f"Check for missing values")
            df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns])

        def check_duplicates_value(df):
            print(f"Check for duplicates value")
            df.groupby(df.columns).count().filter('count > 1').show()

        def validate_schema(df, expected_schema):
            print(f"Validate the schema")
            actual_schema = df.schema()

            if actual_schema == expected_schema:
                print("The schema is matching")
            else:
                print("The schema is not expected")

        def run_validation(df):
            expected_schema = StructType([
                StructField('id', IntegerType(), nullable=False),
                StructField('name', IntegerType(), nullable=True),
                StructField('age', IntegerType(), nullable=True)
            ])

            check_missing_value(df)
            check_duplicates_value(df)
            validate_schema(df, expected_schema)

        run_validation(user_data_df)

    except Exception as e:
        logging.error(f"Running program has an exception: {str(e)}")
    finally:
        if spark:
            spark.stop()

if __name__ == '__main__':
    # Example data
    user_data = [
        (1, "John", "Male", 30),
        (2, "Jane", "Female", None),  # Missing age value
        (3, "Alice", "Female", 25),
        (4, "Bob", "Male", 40),
        (5, "Alice", "Female", 25)  # Duplicate entry (Alice)
    ]

    user_schema = ["ID", "Name", "Gender", "Age"]

    check_validation(user_data, user_schema)