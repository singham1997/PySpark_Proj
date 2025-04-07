from pyspark.sql import SparkSession
from pyspark.sql.connect.dataframe import DataFrame


def read_data_from_external_source(spark: SparkSession, format: str, **options: dict) -> DataFrame:
    df = spark \
        .read \
        .format(format) \
        .options(**options) \
        .load()

    return df
