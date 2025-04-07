from pyspark.sql.connect.dataframe import DataFrame


def transform(df: DataFrame) -> DataFrame:

    transformed_data_df = df.select("phoneNum", "email")

    return transformed_data_df
