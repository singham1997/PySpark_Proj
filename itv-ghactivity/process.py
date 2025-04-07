from pyspark.sql import DataFrame
from pyspark.sql.functions import year, \
    month, dayofmonth

def transform(df: DataFrame) -> DataFrame:
    return df.withColumn('year', year('created_at')) \
        .withColumn('month', month('created_at')) \
        .withColumn('day', dayofmonth('created_at'))

