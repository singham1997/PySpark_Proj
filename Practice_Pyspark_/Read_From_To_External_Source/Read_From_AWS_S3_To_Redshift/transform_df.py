from pyspark.sql import DataFrame


def transform(df: DataFrame) -> DataFrame:
    transformed_df = df.select('id', 'name')

    return transformed_df