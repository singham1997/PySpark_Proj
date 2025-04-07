from pyspark.sql import DataFrame, SparkSession


def from_files(spark: SparkSession, data_dir: str, file_pattern: str, file_format: str) -> DataFrame:
    df = spark \
        .read \
        .format(file_format) \
        .load(f'{data_dir}/{file_pattern}')

    return df