from pyspark.sql import DataFrame


def to_files(df: DataFrame, tgt_dir: str, file_format: str) -> None:
    df.coalesce(16) \
        .write \
        .partitionBy('year', 'month', 'date') \
        .mode('append') \
        .format(file_format) \
        .save(tgt_dir)

