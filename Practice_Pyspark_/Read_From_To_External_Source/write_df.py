from pyspark.sql.connect.dataframe import DataFrame


def write_data_to_external_source(df: DataFrame, format_type: str, mode_type: str ,**kwargs: dict) -> None:
    df.write \
        .format(format_type) \
        .options(**kwargs) \
        .mode(mode_type) \
        .save()

def write_data_to_external_source_table(df: DataFrame, format_type: str, mode_type: str , table_name: str, **kwargs: dict) -> None:
    df.write \
        .format(format_type) \
        .options(**kwargs) \
        .mode(mode_type) \
        .saveAsTable(table_name)